"""
Query the data lake with Athena (serverless SQL over S3, using the Glue Data Catalog).

    python query.py                      # run the demo queries
    python query.py "SELECT * FROM glue_demo_curated.daily_symbol_stats LIMIT 5"
"""
import sys
import time

import config as C

athena = C.client("athena")
RAW, CUR = C.RAW_DB, C.CURATED_DB

DEMO_QUERIES = {
    "Row reconciliation per day (raw in = clean + rejected out)": f"""
        WITH raw AS (SELECT dt AS trade_date, count(*) AS raw_rows FROM {RAW}.trades GROUP BY dt),
             cln AS (SELECT trade_date, count(*) AS clean_rows FROM {CUR}.trades_clean GROUP BY trade_date),
             rej AS (SELECT trade_date, count(*) AS rejected_rows FROM {CUR}.trades_rejected GROUP BY trade_date)
        SELECT raw.trade_date, raw_rows, clean_rows, rejected_rows,
               raw_rows - clean_rows - rejected_rows AS diff
        FROM raw LEFT JOIN cln USING (trade_date) LEFT JOIN rej USING (trade_date)
        ORDER BY 1""",
    "Why rows were rejected": f"""
        SELECT reject_reason, count(*) AS n
        FROM {CUR}.trades_rejected GROUP BY 1 ORDER BY 2 DESC""",
    "Top 5 symbols by notional on the latest day": f"""
        SELECT trade_date, symbol, sector, n_trades, volume, notional, vwap, open, close,
               round(100 * (close / open - 1), 2) AS pct_change
        FROM {CUR}.daily_symbol_stats
        WHERE trade_date = (SELECT max(trade_date) FROM {CUR}.daily_symbol_stats)
        ORDER BY notional DESC LIMIT 5""",
    "Sector notional by day": f"""
        SELECT trade_date, sector, round(sum(notional) / 1e6, 2) AS notional_musd, sum(n_trades) AS trades
        FROM {CUR}.daily_symbol_stats GROUP BY 1, 2 ORDER BY 1, 3 DESC""",
}


def run_query(sql: str, timeout_s: int = 120) -> tuple[list[str], list[list[str]], dict]:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=C.ATHENA_WORKGROUP)["QueryExecutionId"]
    deadline = time.time() + timeout_s
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() > deadline:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Athena query {qid} timed out")
        time.sleep(1)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query {state}: {q['Status'].get('StateChangeReason')}\n{sql}")

    rows, cols = [], []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        if not cols:
            cols = [c["Name"] for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        for r in page["ResultSet"]["Rows"]:
            rows.append([d.get("VarCharValue", "") for d in r["Data"]])
    if rows and rows[0] == cols:  # first row of the first page is the header
        rows = rows[1:]
    return cols, rows, q["Statistics"]


def print_table(cols: list[str], rows: list[list[str]]) -> None:
    widths = [max(len(c), *(len(r[i]) for r in rows)) if rows else len(c) for i, c in enumerate(cols)]
    print("  " + " | ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    for r in rows:
        print("  " + " | ".join(v.ljust(w) for v, w in zip(r, widths)))


def run_demo() -> None:
    total_bytes = billed = 0
    for title, sql in DEMO_QUERIES.items():
        print(f"\n## {title}")
        cols, rows, stats = run_query(sql)
        print_table(cols, rows)
        scanned = stats.get("DataScannedInBytes", 0)
        total_bytes += scanned
        billed += max(scanned, 10 * 1024**2)  # Athena: $5/TB scanned, 10 MB minimum per query
    print(f"\nAthena scanned {total_bytes / 1024**2:.2f} MB total "
          f"(billed as >= {billed / 1024**2:.0f} MB -> ~${billed / 1024**4 * 5:.5f})")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        c, r, _ = run_query(" ".join(sys.argv[1:]))
        print_table(c, r)
    else:
        run_demo()
