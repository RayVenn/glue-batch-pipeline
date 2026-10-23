"""
Glue Spark ETL job: raw CSV trades (catalog) -> clean Parquet data lake.

    READ   glue_demo_raw.trades       (CSV, partitioned by dt, from the raw crawler)
           glue_demo_raw.securities   (CSV reference data)
    WRITE  curated/trades_clean/        row-level clean trades      (Parquet, by trade_date)
           curated/daily_symbol_stats/  daily OHLC / VWAP per symbol (Parquet, by trade_date)
           curated/trades_rejected/     bad rows + reject_reason     (Parquet, by trade_date)

Incremental processing with JOB BOOKMARKS
-----------------------------------------
The bookmarked read only returns files Glue has not processed before. We use
it to find which dt partitions changed, then re-read those *whole* partitions
and overwrite only those output partitions (dynamic partition overwrite).
That way a late file for an old day recomputes that day correctly, and
re-running the job is idempotent.

The transform() function has no Glue dependency, so it can be unit-tested
with plain PySpark (see local_test.py).
"""
import sys

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

VALID_SIDES = ["BUY", "SELL"]


def transform(trades: DataFrame, securities: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Return (clean, rejected, daily_stats).

    `trades` columns: trade_id, ts, symbol, side, price, qty, venue, dt
    `securities` columns: symbol, company_name, sector
    """
    sec = (
        securities.select(
            F.upper(F.trim(F.col("symbol"))).alias("symbol"),
            F.col("company_name"),
            F.col("sector"),
        ).dropDuplicates(["symbol"])
    )

    t = (
        trades.withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
        .withColumn("side", F.upper(F.trim(F.col("side"))))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("qty", F.col("qty").cast("long"))
        # try_to_timestamp returns NULL instead of failing (Spark 4 runs in ANSI mode)
        .withColumn("trade_ts", F.try_to_timestamp(F.col("ts"), F.lit("yyyy-MM-dd HH:mm:ss.SSS")))
        .withColumn("trade_date", F.col("dt").cast("string"))
        .join(F.broadcast(sec), on="symbol", how="left")
    )

    # First occurrence of a trade_id wins; later copies are duplicates.
    w = Window.partitionBy("trade_id").orderBy(F.col("trade_ts").asc_nulls_last(), F.col("venue"))
    t = t.withColumn("_rn", F.row_number().over(w))

    reason = (
        F.when(F.col("trade_id").isNull(), "missing_trade_id")
        .when(F.col("trade_ts").isNull(), "bad_timestamp")
        .when(F.col("price").isNull() | (F.col("price") <= 0), "bad_price")
        .when(F.col("qty").isNull() | (F.col("qty") <= 0), "bad_qty")
        .when(~F.col("side").isin(VALID_SIDES) | F.col("side").isNull(), "bad_side")
        .when(F.col("sector").isNull(), "unknown_symbol")
        .when(F.col("_rn") > 1, "duplicate")
    )
    t = t.withColumn("reject_reason", reason).drop("_rn").cache()

    clean = t.where(F.col("reject_reason").isNull()).select(
        "trade_id",
        "trade_ts",
        "symbol",
        "company_name",
        "sector",
        "side",
        "price",
        "qty",
        (F.col("price") * F.col("qty")).alias("notional"),
        "venue",
        F.current_timestamp().alias("processed_at"),
        "trade_date",
    )

    rejected = t.where(F.col("reject_reason").isNotNull()).select(
        "trade_id",
        F.col("ts").alias("raw_ts"),
        "symbol",
        "side",
        F.col("price").cast("double").alias("price"),
        F.col("qty").cast("long").alias("qty"),
        "venue",
        "reject_reason",
        F.current_timestamp().alias("processed_at"),
        "trade_date",
    )

    stats = (
        clean.groupBy("trade_date", "symbol", "sector")
        .agg(
            F.count("*").alias("n_trades"),
            F.sum("qty").alias("volume"),
            F.round(F.sum("notional"), 2).alias("notional"),
            F.round(F.sum("notional") / F.sum("qty"), 4).alias("vwap"),
            F.min_by("price", "trade_ts").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "trade_ts").alias("close"),
            F.sum(F.when(F.col("side") == "BUY", F.col("qty")).otherwise(F.lit(0))).alias("buy_volume"),
        )
        .withColumn("buy_ratio", F.round(F.col("buy_volume") / F.col("volume"), 4))
        .select(
            "symbol", "sector", "n_trades", "volume", "notional", "vwap",
            "open", "high", "low", "close", "buy_volume", "buy_ratio", "trade_date",
        )
    )
    return clean, rejected, stats


def write_partitions(df: DataFrame, path: str) -> None:
    # Dynamic overwrite: only the trade_date partitions present in df are replaced.
    (
        df.repartition("trade_date")
        .write.mode("overwrite")
        .partitionBy("trade_date")
        .option("compression", "snappy")
        .parquet(path)
    )


def main() -> None:
    # Glue-only imports live here so transform() can be tested without Glue.
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "raw_db", "trades_table", "securities_table", "output_base"]
    )
    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Default is 200 shuffle partitions: far too many for a few MB (slow + tiny files).
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    log = glue.get_logger()
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    out = args["output_base"].rstrip("/")

    # 1) Bookmarked read: only files not seen by a previous successful run.
    new_files = glue.create_dynamic_frame.from_catalog(
        database=args["raw_db"],
        table_name=args["trades_table"],
        transformation_ctx="raw_trades_bookmark",  # <- this name is the bookmark key
    )
    changed = sorted(r["dt"] for r in new_files.toDF().select("dt").distinct().collect())
    log.info(f"Partitions with new data: {changed}")
    if not changed:
        log.info("Nothing new to process (bookmark is up to date).")
        job.commit()
        return

    # 2) Re-read the full changed partitions. No transformation_ctx -> not bookmarked.
    dt_list = ",".join(f"'{d}'" for d in changed)
    trades_dyf = glue.create_dynamic_frame.from_catalog(
        database=args["raw_db"],
        table_name=args["trades_table"],
        push_down_predicate=f"dt in ({dt_list})",  # partition pruning: S3 lists only these folders
    )
    # A CSV column with blanks can be inferred as a "choice" type; force the types we want.
    trades_dyf = trades_dyf.resolveChoice(specs=[("price", "cast:double"), ("qty", "cast:long")])

    # Reference data: always read in full (bookmarking it would hide unchanged rows).
    sec_df = glue.create_dynamic_frame.from_catalog(
        database=args["raw_db"], table_name=args["securities_table"]
    ).toDF()

    clean, rejected, stats = transform(trades_dyf.toDF(), sec_df)

    write_partitions(clean, f"{out}/trades_clean/")
    write_partitions(stats, f"{out}/daily_symbol_stats/")
    write_partitions(rejected, f"{out}/trades_rejected/")

    # Small counts for the job log (data is tiny, so these extra actions are cheap).
    for name, df in [("clean", clean), ("rejected", rejected), ("stats", stats)]:
        log.info(f"{name} rows written: {df.count()}")
    for row in rejected.groupBy("reject_reason").count().collect():
        log.info(f"rejected[{row['reject_reason']}] = {row['count']}")

    job.commit()  # advances the bookmark only if we got here


if __name__ == "__main__":
    main()
