"""
Run the batch pipeline end to end.

    python run_pipeline.py                                 # 3 days from 2026-10-01, Glue workflow mode
    python run_pipeline.py --start 2026-10-06 --days 2     # add 2 more days -> bookmark picks up only those
    python run_pipeline.py --mode steps                    # orchestrate each step from Python instead
    python run_pipeline.py --no-upload                     # re-run on whatever is already in S3
    python run_pipeline.py --reset-bookmark                # forget history, reprocess every partition

Flow:
  1. generate CSVs locally and upload to s3://<bucket>/raw/
  2. raw crawler      -> glue_demo_raw.trades / .securities (schema + dt partitions)
  3. Glue Spark job   -> Parquet under s3://<bucket>/curated/
  4. curated crawler  -> glue_demo_curated.trades_clean / daily_symbol_stats / trades_rejected
  5. Athena queries over the curated tables
  6. cost estimate for this run
"""
import argparse
import time
from datetime import date, datetime, timezone
from pathlib import Path

import config as C
import generate_data
import query

glue = C.client("glue")
s3 = C.client("s3")


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ------------------------------------------------------------- upload ----
def upload_data(start: date, days: int, rows: int) -> None:
    bucket = C.bucket_name()
    root = Path(__file__).parent / "data"
    for p in generate_data.write_local(start, days, rows, root):
        key = p.relative_to(root).as_posix()  # raw/trades/dt=.../file.csv
        s3.upload_file(str(p), bucket, key)
        log(f"uploaded s3://{bucket}/{key}  ({p.stat().st_size / 1024:.0f} KiB)")


# ------------------------------------------------ step-by-step mode ----
def run_crawler(name: str) -> None:
    log(f"crawler {name}: starting")
    try:
        glue.start_crawler(Name=name)
    except glue.exceptions.CrawlerRunningException:
        log("   already running, waiting for it")
    time.sleep(5)
    while True:
        c = glue.get_crawler(Name=name)["Crawler"]
        if c["State"] == "READY":
            break
        time.sleep(10)
    last = c.get("LastCrawl", {})
    log(f"crawler {name}: {last.get('Status')}")
    if last.get("Status") != "SUCCEEDED":
        raise RuntimeError(f"Crawler {name} failed: {last.get('ErrorMessage')}")


def run_job() -> str:
    run_id = glue.start_job_run(JobName=C.JOB_NAME)["JobRunId"]
    log(f"job {C.JOB_NAME}: started {run_id}")
    last_state = None
    while True:
        jr = glue.get_job_run(JobName=C.JOB_NAME, RunId=run_id)["JobRun"]
        if jr["JobRunState"] != last_state:
            last_state = jr["JobRunState"]
            log(f"job {C.JOB_NAME}: {last_state}")
        if last_state in ("SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR", "EXPIRED"):
            break
        time.sleep(10)
    if last_state != "SUCCEEDED":
        raise RuntimeError(f"Job failed: {jr.get('ErrorMessage')} (see CloudWatch /aws-glue/jobs/error)")
    return run_id


def run_steps() -> str:
    run_crawler(C.RAW_CRAWLER)
    run_id = run_job()
    run_crawler(C.CURATED_CRAWLER)
    return run_id


# ------------------------------------------------------ workflow mode ----
def run_workflow() -> str | None:
    wf_run = glue.start_workflow_run(Name=C.WORKFLOW_NAME)["RunId"]
    log(f"workflow {C.WORKFLOW_NAME}: started run {wf_run}")
    seen: dict[str, str] = {}
    job_run_id = None
    while True:
        run = glue.get_workflow_run(Name=C.WORKFLOW_NAME, RunId=wf_run, IncludeGraph=True)["Run"]
        for node in run.get("Graph", {}).get("Nodes", []):
            state = None
            if node["Type"] == "CRAWLER":
                crawls = node.get("CrawlerDetails", {}).get("Crawls", [])
                state = crawls[-1].get("State") if crawls else None
            elif node["Type"] == "JOB":
                runs = node.get("JobDetails", {}).get("JobRuns", [])
                if runs:
                    state, job_run_id = runs[-1].get("JobRunState"), runs[-1].get("Id")
            if state and seen.get(node["Name"]) != state:
                seen[node["Name"]] = state
                log(f"   {node['Type'].lower():7s} {node['Name']}: {state}")
        if run["Status"] in ("COMPLETED", "STOPPED", "ERROR"):
            break
        time.sleep(10)

    st = run.get("Statistics", {})
    log(f"workflow {run['Status']}: {st.get('SucceededActions', 0)}/{st.get('TotalActions', 0)} actions succeeded")
    # Note: a workflow is "COMPLETED" even when an action failed, so check the counts.
    if run["Status"] != "COMPLETED" or st.get("FailedActions", 0) or st.get("TimeoutActions", 0) \
            or st.get("SucceededActions", 0) < 3:
        raise RuntimeError("Workflow did not finish all 3 steps. Check the Glue console > Workflows > History.")
    return job_run_id


# --------------------------------------------------------------- cost ----
def crawler_cost(name: str, since: datetime) -> float:
    crawls = glue.list_crawls(CrawlerName=name, MaxResults=5).get("Crawls", [])
    total = 0.0
    for c in crawls:
        if c.get("StartTime") and c["StartTime"] >= since:
            dpu_hours = c.get("DPUHour") or 0.0
            # At least 2 DPU for the 10-minute billing minimum.
            minimum = 2 * C.CRAWLER_MIN_BILLED_SECONDS / 3600
            total += max(dpu_hours, minimum) * C.CRAWLER_PRICE_PER_DPU_HOUR
    return total


def job_cost(run_id: str | None) -> tuple[float, str]:
    if not run_id:
        return 0.0, "no job run found"
    jr = glue.get_job_run(JobName=C.JOB_NAME, RunId=run_id)["JobRun"]
    secs = max(jr.get("ExecutionTime", 0), C.JOB_MIN_BILLED_SECONDS)
    dpu_seconds = jr.get("DPUSeconds") or secs * jr.get("NumberOfWorkers", C.NUMBER_OF_WORKERS)
    rate = C.JOB_PRICE_PER_DPU_HOUR
    if jr.get("ExecutionClass") == "FLEX":
        rate = 0.29 * (0.7 if C.GLUE_VERSION.startswith("6") else 1)  # approx; check pricing page
    return dpu_seconds / 3600 * rate, f"{jr.get('ExecutionTime', 0)}s runtime, {dpu_seconds / 3600:.3f} DPU-h"


def print_cost(since: datetime, run_id: str | None) -> None:
    raw = crawler_cost(C.RAW_CRAWLER, since)
    cur = crawler_cost(C.CURATED_CRAWLER, since)
    job, detail = job_cost(run_id)
    print(f"""
Estimated cost of this run (list prices, {C.REGION}):
  raw crawler       ${raw:.3f}   (10-minute billing minimum)
  Glue job          ${job:.3f}   ({detail})
  curated crawler   ${cur:.3f}
  Athena + S3       < $0.01
  -------------------------------
  total            ~${raw + cur + job + 0.01:.2f}
Exact numbers appear in Cost Explorer after ~24h (filter Service = Glue).""")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-10-01")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--rows", type=int, default=20000, help="trades per day")
    ap.add_argument("--mode", choices=["workflow", "steps"], default="workflow")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--reset-bookmark", action="store_true")
    ap.add_argument("--skip-query", action="store_true")
    a = ap.parse_args()

    started = datetime.now(timezone.utc)
    if a.reset_bookmark:
        glue.reset_job_bookmark(JobName=C.JOB_NAME)
        log("job bookmark reset: the next run reprocesses every partition")
    if not a.no_upload:
        upload_data(date.fromisoformat(a.start), a.days, a.rows)

    run_id = run_workflow() if a.mode == "workflow" else run_steps()

    if not a.skip_query:
        query.run_demo()
    print_cost(started, run_id)


if __name__ == "__main__":
    main()
