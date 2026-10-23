# AWS Glue batch pipeline — a hands-on learning project

A small but complete batch data pipeline on AWS, built and run entirely from Python (boto3).
It covers the full Glue suite end to end: **S3 data lake → Glue Crawler → Data Catalog →
Glue Spark ETL job (with job bookmarks) → Glue Workflow → Athena**.

**Cost: about $0.30–0.40 per pipeline run.** Nothing costs money while idle except a few
MB of S3 storage. `teardown.py` removes everything.

The data is synthetic US equity trades (12 symbols, ~20k trades per day), with about 1%
deliberately bad rows so the ETL has real cleaning work to do.

---

## Architecture

```
                         ┌──────────────── Glue Workflow (glue-demo-workflow) ────────────────┐
                         │                                                                     │
 run_pipeline.py         │  t0 ON_DEMAND        t1 CONDITIONAL           t2 CONDITIONAL        │
 generates CSV  ──┐      │      │              (crawler SUCCEEDED)      (job SUCCEEDED)       │
                  ▼      │      ▼                     ▼                        ▼              │
 s3://.../raw/  ─────────┼─▶ RAW CRAWLER ──▶  GLUE SPARK JOB  ──────▶  CURATED CRAWLER         │
  trades/dt=YYYY-MM-DD/  │      │              trades_etl.py               │                  │
  securities/            │      │              │  reads raw tables         │                  │
                         └──────┼──────────────┼───────────────────────────┼──────────────────┘
                                ▼              ▼  writes Parquet           ▼
                     Data Catalog:      s3://.../curated/          Data Catalog:
                     glue_demo_raw        trades_clean/             glue_demo_curated
                       .trades            daily_symbol_stats/         .trades_clean
                       .securities        trades_rejected/            .daily_symbol_stats
                                                                       .trades_rejected
                                                                           │
                                                          query.py ──▶  ATHENA (SQL on S3)
```

Lake zones (the usual bronze / silver / gold idea):

| Zone | S3 prefix | Format | Catalog table | Created by |
|---|---|---|---|---|
| Raw (bronze) | `raw/trades/dt=…/` | CSV | `glue_demo_raw.trades` | raw crawler |
| Reference | `raw/securities/` | CSV | `glue_demo_raw.securities` | raw crawler |
| Clean (silver) | `curated/trades_clean/trade_date=…/` | Parquet | `glue_demo_curated.trades_clean` | job + curated crawler |
| Aggregate (gold) | `curated/daily_symbol_stats/trade_date=…/` | Parquet | `glue_demo_curated.daily_symbol_stats` | job + curated crawler |
| Quarantine | `curated/trades_rejected/trade_date=…/` | Parquet | `glue_demo_curated.trades_rejected` | job + curated crawler |

---

## What each piece teaches

| Concept | Where | What to notice |
|---|---|---|
| **Data Catalog** | `deploy.py` → `create_databases` | A database is just a namespace for table *metadata*. The data stays in S3. |
| **Crawler** | `deploy.py` → `upsert_crawler` | Reads files, infers the schema, and creates tables and partitions (`dt=…` folders become a `dt` column). Run it again after new folders land and new partitions appear. |
| **Classifier** | `deploy.py` → `create_classifier` | Tells the crawler "these CSVs have a header". Without one, the built-in classifier guesses. |
| **Glue job (Spark)** | `glue_jobs/trades_etl.py` | `GlueContext` + `DynamicFrame` read from the catalog. Then plain PySpark. Then Parquet writes partitioned by date. |
| **Job bookmarks** | `transformation_ctx="raw_trades_bookmark"` | Glue remembers which files it has processed. The next run only sees new files. `job.commit()` saves progress. |
| **Partition pruning** | `push_down_predicate="dt in (...)"` | Glue lists only the matching S3 folders instead of the whole table. |
| **Idempotent writes** | `partitionOverwriteMode=dynamic` | Re-running a day replaces only that day's output partitions. No duplicates. |
| **Data quality** | `transform()` | Bad rows are not dropped silently. They go to `trades_rejected` with a `reject_reason`, and `query.py` checks raw = clean + rejected. |
| **Workflow + triggers** | `deploy.py` → `upsert_workflow` | Glue's built-in orchestration: on-demand start, then conditional triggers chain crawler → job → crawler. |
| **Athena** | `query.py` | Serverless SQL over S3 through the same catalog. The workgroup has a 1 GB per-query scan cap as a cost guard. |

---

## Prerequisites

- Python 3.10+
- AWS credentials for an account where you can create IAM roles, S3 buckets, Glue and
  Athena resources (admin in a personal or sandbox account is simplest):
  `aws configure`, or `AWS_PROFILE=...`
- Region: `us-east-1` by default. Change it with `export AWS_REGION=us-west-2`.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run it

```bash
# 1. Create all resources (free: nothing runs yet)
python deploy.py

# 2. Run the pipeline: upload 3 days of data -> workflow -> Athena queries -> cost estimate
python run_pipeline.py
```

The workflow takes about 6–10 minutes. Most of that is crawler and Spark startup, not
data processing. You will see output like:

```
[11:40:02] workflow glue-demo-workflow: started run wr_...
[11:40:12]    crawler glue-demo-raw-crawler: RUNNING
[11:42:05]    crawler glue-demo-raw-crawler: SUCCEEDED
[11:42:15]    job     glue-demo-trades-etl: RUNNING
...
## Row reconciliation per day (raw in = clean + rejected out)
  trade_date | raw_rows | clean_rows | rejected_rows | diff
  2026-10-01 | 20085    | 19842      | 243           | 0
  2026-10-02 | 20074    | 19824      | 250           | 0
  2026-10-05 | 20085    | 19852      | 233           | 0
```

(The data is seeded by date, so you should see these same counts. 2026-10-03/04 are a
weekend, so they are skipped.)

### See incremental processing (job bookmarks)

```bash
# Add two new days. The job processes only the 2 new partitions, not all 5.
python run_pipeline.py --start 2026-10-06 --days 2
```

Then open the job run's logs in CloudWatch (`/aws-glue/jobs/output`, or the job's Runs
tab). The line `Partitions with new data: [...]` shows the bookmark at work. Run
`python run_pipeline.py --no-upload` with no new data and the job exits early with
"Nothing new to process".

`--reset-bookmark` makes the next run reprocess everything.

### Other options

```bash
python run_pipeline.py --mode steps      # orchestrate from Python instead of the Glue workflow
python query.py                          # just the demo queries
python query.py "SELECT * FROM glue_demo_curated.daily_symbol_stats ORDER BY notional DESC LIMIT 5"
```

### Iterate on the ETL logic locally (free)

The transform is a pure PySpark function, so you can test it without Glue:

```bash
pip install pyspark            # needs Java 17+
python generate_data.py --days 2 --rows 5000
python local_test.py
```

After editing `glue_jobs/trades_etl.py`, run `python deploy.py` again to upload the new
script.

---

## Cost

us-east-1 list prices, checked October 2026. Billing is per second with minimums.

| Item | Rate | Per run |
|---|---|---|
| Raw crawler | $0.44/DPU-hour, **10-minute minimum** | ~$0.15 |
| Glue Spark job (Glue 6.0, 2 × G.1X, ~2 min) | $0.308/DPU-hour, 1-minute minimum | ~$0.02–0.04 |
| Curated crawler | $0.44/DPU-hour, 10-minute minimum | ~$0.15 |
| Athena (4 queries) | $5/TB scanned, 10 MB minimum per query | < $0.001 |
| S3 (~10 MB) | | < $0.001/month |
| **Total** | | **~$0.30–0.40** |

Crawlers are most of the cost, because of the 10-minute minimum. The cost guards built in:

- 2 workers (the minimum) and a **15-minute job timeout**, so the job cannot run away
- `MaxRetries=0` and `MaxConcurrentRuns=1`
- Athena workgroup refuses any query that scans more than 1 GB
- S3 lifecycle rules expire Athena results (7 days) and Glue temp files (3 days)
- Nothing is scheduled. Every run is started by you.

`run_pipeline.py` prints an estimate after each run. Exact charges show up in Cost
Explorer after about a day (filter Service = Glue).

Options:
- `GLUE_EXECUTION_CLASS=FLEX python deploy.py` uses spare capacity at a lower rate, but
  job start can be delayed by minutes.
- `GLUE_VERSION=5.0 python deploy.py` if Glue 6.0 is not available in your region
  ($0.44/DPU-hour instead of $0.308).

---

## Console tour (after the first run)

1. **Glue → Workflows → glue-demo-workflow → History**: the run graph, each node's status.
2. **Glue → Crawlers → glue-demo-raw-crawler**: run history, and "tables added / updated".
3. **Glue → Data Catalog → Tables → trades**: inferred schema, the `dt` partition column,
   and the Partitions tab.
4. **Glue → ETL jobs → glue-demo-trades-etl → Runs**: duration, DPU-hours, logs.
   The Script tab shows the uploaded code.
5. **S3 → glue-demo-<account>-<region>/curated/**: the Parquet files and `trade_date=` folders.
6. **Athena → Query editor**: pick workgroup `glue-demo-wg` and query the tables yourself.

---

## Teardown

```bash
python teardown.py          # asks to confirm
```

Deletes triggers, workflow, job (and its bookmark), crawlers, classifier, both catalog
databases, the Athena workgroup, the S3 bucket and all its data, and the IAM role.
CloudWatch log groups `/aws-glue/*` are shared, so they are left in place.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `AccessDenied` in `deploy.py` | Your credentials cannot create IAM roles. Use an admin profile. |
| Crawler `SUCCEEDED` but no table | Wrong S3 path, or no files uploaded yet. Check `raw/trades/dt=…/`. |
| Job fails with `Table trades not found` | Raw crawler has not run yet. Use the workflow, or run the crawler first. |
| Job `FAILED` | Glue console → job → Runs → "Error logs", or CloudWatch `/aws-glue/jobs/error`. |
| `GlueVersion 6.0 is not supported` | `GLUE_VERSION=5.0 python deploy.py` |
| Athena `Table not found` | Curated crawler has not finished. Use `--skip-query`, then run `query.py` later. |

---

## Next steps to extend it

- **Scheduling**: change trigger `t0` to `Type="SCHEDULED"` with `cron(0 6 ? * MON-FRI *)`.
- **Skip the raw crawler**: use Athena partition projection, or have the job register
  partitions itself (`enableUpdateCatalog`). This removes most of the cost per run.
- **Apache Iceberg**: write `trades_clean` as an Iceberg table for ACID updates, MERGE and
  time travel (Glue 6.0 supports Iceberg v3).
- **Glue Data Quality**: replace the hand-written checks with DQDL rules.
- **Warehouse layer**: load `daily_symbol_stats` into Redshift Serverless, or query the
  lake from Redshift with Spectrum. More cost: Redshift Serverless bills a minimum per query.
- **Orchestration alternative**: Step Functions or MWAA (Airflow) instead of Glue workflows.
- **Infrastructure as code**: port `deploy.py` to AWS CDK (Python).

## Files

```
config.py                 names, sizing, prices, S3 layout
generate_data.py          synthetic trades CSV with dirty rows (stdlib only)
deploy.py                 create/update all AWS resources (idempotent)
glue_jobs/trades_etl.py   the Glue Spark job (transform() is testable locally)
run_pipeline.py           upload -> workflow -> queries -> cost estimate
query.py                  Athena helper + demo queries
local_test.py             run the transform with local PySpark
teardown.py               delete everything
```
