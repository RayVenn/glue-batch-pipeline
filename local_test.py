"""
Run the ETL transform locally with plain PySpark - no AWS, no cost.
Useful for iterating on the job logic before deploying.

    pip install pyspark
    python generate_data.py --days 2 --rows 5000
    python local_test.py
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent / "glue_jobs"))
from trades_etl import transform  # noqa: E402

spark = (
    SparkSession.builder.master("local[2]").appName("glue-demo-local")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Spark's CSV reader discovers the dt=... folders as a partition column, like the crawler does.
trades = spark.read.option("header", True).csv("data/raw/trades")
securities = spark.read.option("header", True).csv("data/raw/securities")

clean, rejected, stats = transform(trades, securities)

n_raw, n_clean, n_rej = trades.count(), clean.count(), rejected.count()
print(f"raw={n_raw} clean={n_clean} rejected={n_rej}")
assert n_raw == n_clean + n_rej, "every raw row must end up clean or rejected"
assert clean.groupBy("trade_id").count().where("count > 1").count() == 0, "duplicates in clean output"

rejected.groupBy("reject_reason").count().orderBy("count", ascending=False).show()
stats.orderBy("trade_date", "notional", ascending=[True, False]).show(12)
clean.printSchema()

out = "data/local_out"
for name, df in [("trades_clean", clean), ("daily_symbol_stats", stats), ("trades_rejected", rejected)]:
    df.write.mode("overwrite").partitionBy("trade_date").parquet(f"{out}/{name}")
print(f"Parquet written under {out}/  - all checks passed")
