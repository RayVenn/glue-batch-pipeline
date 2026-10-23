"""
Central configuration for the Glue batch pipeline demo.

Everything is named with the PROJECT prefix so it is easy to find in the
console and easy to tear down.
"""
import os
from functools import lru_cache

import boto3

PROJECT = os.environ.get("GLUE_DEMO_PROJECT", "glue-demo")
REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"

# ---- Glue Data Catalog --------------------------------------------------
RAW_DB = PROJECT.replace("-", "_") + "_raw"          # tables created by the raw crawler
CURATED_DB = PROJECT.replace("-", "_") + "_curated"  # tables created by the curated crawler

# ---- Glue resources -----------------------------------------------------
ROLE_NAME = f"{PROJECT}-glue-role"
CSV_CLASSIFIER = f"{PROJECT}-csv-with-header"
RAW_CRAWLER = f"{PROJECT}-raw-crawler"
CURATED_CRAWLER = f"{PROJECT}-curated-crawler"
JOB_NAME = f"{PROJECT}-trades-etl"
WORKFLOW_NAME = f"{PROJECT}-workflow"
TRIGGER_START = f"{PROJECT}-t0-start"
TRIGGER_AFTER_RAW_CRAWL = f"{PROJECT}-t1-after-raw-crawl"
TRIGGER_AFTER_JOB = f"{PROJECT}-t2-after-etl"
ATHENA_WORKGROUP = f"{PROJECT}-wg"

# ---- Glue job sizing (this is where cost is controlled) -----------------
# Glue 6.0 is ~30% cheaper per DPU-hour than 5.1. If 6.0 is not available in
# your region, set GLUE_VERSION=5.0 (the script is compatible with Spark 3.5+).
GLUE_VERSION = os.environ.get("GLUE_VERSION", "6.0")
WORKER_TYPE = "G.1X"          # 1 DPU per worker (4 vCPU, 16 GB)
NUMBER_OF_WORKERS = 2         # minimum allowed for Spark jobs
JOB_TIMEOUT_MINUTES = 15      # hard stop -> hard cost ceiling for the job
# "STANDARD" starts fast. "FLEX" uses spare capacity: cheaper rate but the
# start can be delayed by minutes. Fine for a nightly batch, annoying for learning.
EXECUTION_CLASS = os.environ.get("GLUE_EXECUTION_CLASS", "STANDARD")

# ---- Cost estimates (us-east-1 list prices; check the pricing page) -----
JOB_PRICE_PER_DPU_HOUR = 0.308 if GLUE_VERSION.startswith("6") else 0.44
CRAWLER_PRICE_PER_DPU_HOUR = 0.44
CRAWLER_MIN_BILLED_SECONDS = 600   # every crawler run is billed >= 10 minutes
JOB_MIN_BILLED_SECONDS = 60        # Glue 2.0+ jobs are billed >= 1 minute
ATHENA_SCAN_LIMIT_BYTES = 1 * 1024**3  # workgroup refuses queries scanning > 1 GB

# ---- S3 layout -----------------------------------------------------------
#   s3://<bucket>/raw/trades/dt=YYYY-MM-DD/*.csv     landing zone (bronze)
#   s3://<bucket>/raw/securities/securities.csv      reference data
#   s3://<bucket>/curated/trades_clean/              Parquet (silver)
#   s3://<bucket>/curated/daily_symbol_stats/        Parquet aggregates (gold)
#   s3://<bucket>/curated/trades_rejected/           quarantined bad rows
#   s3://<bucket>/scripts/                           Glue job script
#   s3://<bucket>/tmp/                               Glue temp dir
#   s3://<bucket>/athena-results/                    Athena query output
RAW_TRADES_PREFIX = "raw/trades/"
RAW_SECURITIES_PREFIX = "raw/securities/"
CURATED_CLEAN_PREFIX = "curated/trades_clean/"
CURATED_STATS_PREFIX = "curated/daily_symbol_stats/"
CURATED_REJECTED_PREFIX = "curated/trades_rejected/"
SCRIPT_KEY = "scripts/trades_etl.py"
TMP_PREFIX = "tmp/"
ATHENA_PREFIX = "athena-results/"


@lru_cache(maxsize=1)
def session() -> boto3.session.Session:
    return boto3.session.Session(region_name=REGION)


def client(name: str):
    return session().client(name)


@lru_cache(maxsize=1)
def account_id() -> str:
    return client("sts").get_caller_identity()["Account"]


def bucket_name() -> str:
    # Bucket names are global, so include the account id and region.
    return os.environ.get("GLUE_DEMO_BUCKET") or f"{PROJECT}-{account_id()}-{REGION}"


def s3_uri(prefix: str = "") -> str:
    return f"s3://{bucket_name()}/{prefix}"
