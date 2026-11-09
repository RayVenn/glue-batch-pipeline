"""
Run the batch pipeline one stage at a time (default), or as a single shot.

Stages (in order):
  1. upload           generate CSVs and put them in s3://…/raw/
  2. raw-crawler      invent schema + partitions -> glue_demo_raw.*
  3. job              Spark ETL -> Parquet under s3://…/curated/
  4. curated-crawler  register Parquet -> glue_demo_curated.*
  5. query            Athena demo queries
  6. cost             estimate Glue spend for this session

Examples:
  python run_pipeline.py                              # interactive: pause before each stage
  python run_pipeline.py --step upload                # only upload
  python run_pipeline.py --step raw-crawler
  python run_pipeline.py --step job
  python run_pipeline.py --step curated-crawler
  python run_pipeline.py --step query
  python run_pipeline.py --step cost
  python run_pipeline.py --mode workflow              # old one-shot Glue workflow
  python run_pipeline.py --mode steps --yes           # all stages, no prompts
  python run_pipeline.py --start 2026-10-06 --days 2  # incremental days
  python run_pipeline.py --no-upload --step job       # re-run job on data already in S3
  python run_pipeline.py --reset-bookmark
"""
from __future__ import annotations

import argparse
import time
from datetime import date, datetime, timezone
from pathlib import Path

import config as C
import generate_data
import query

glue = C.client("glue")
s3 = C.client("s3")

STEPS = ("upload", "raw-crawler", "job", "curated-crawler", "query", "cost")

# Brief card shown before each stage so you know why it exists.
STEP_INFO = {
    "upload": {
        "why": "Landing zone needs files. Glue jobs read from S3; nothing runs until data is there.",
        "input": "Synthetic trade + securities CSVs (generated locally)",
        "output": f"s3://…/{C.RAW_TRADES_PREFIX}dt=YYYY-MM-DD/*.csv  and  s3://…/{C.RAW_SECURITIES_PREFIX}",
        "resource": "S3 bucket (glue-demo-<account>-<region>)",
    },
    "raw-crawler": {
        "why": "Turns raw files into Catalog tables (schema + dt partitions) the Spark job can name.",
        "input": f"s3://…/{C.RAW_TRADES_PREFIX}  and  s3://…/{C.RAW_SECURITIES_PREFIX}",
        "output": f"Glue tables {C.RAW_DB}.trades  and  {C.RAW_DB}.securities",
        "resource": f"Crawler {C.RAW_CRAWLER}  (+ classifier {C.CSV_CLASSIFIER})",
    },
    "job": {
        "why": "Clean / quarantine / aggregate. Bookmarks skip already-processed raw files.",
        "input": f"Catalog {C.RAW_DB}.trades + {C.RAW_DB}.securities",
        "output": (
            f"s3://…/{C.CURATED_CLEAN_PREFIX}  "
            f"s3://…/{C.CURATED_STATS_PREFIX}  "
            f"s3://…/{C.CURATED_REJECTED_PREFIX}"
        ),
        "resource": f"Glue Spark job {C.JOB_NAME}",
    },
    "curated-crawler": {
        "why": "Job wrote Parquet to S3; Athena needs Catalog tables pointing at those folders.",
        "input": "s3://…/curated/",
        "output": (
            f"{C.CURATED_DB}.trades_clean, "
            f"{C.CURATED_DB}.daily_symbol_stats, "
            f"{C.CURATED_DB}.trades_rejected"
        ),
        "resource": f"Crawler {C.CURATED_CRAWLER}",
    },
    "query": {
        "why": "Prove the lake is queryable: reconciliation + sample analytics over curated tables.",
        "input": f"Catalog {C.CURATED_DB}.* via workgroup {C.ATHENA_WORKGROUP}",
        "output": f"Result files under s3://…/{C.ATHENA_PREFIX}  (+ printed rows)",
        "resource": f"Athena workgroup {C.ATHENA_WORKGROUP}",
    },
    "cost": {
        "why": "Crawlers dominate cost (10-minute minimum). Useful reality check after a run.",
        "input": "Glue crawl / job-run metadata since this script started",
        "output": "Printed USD estimate (Cost Explorer is authoritative after ~24h)",
        "resource": "Billing metadata only (no new AWS resource)",
    },
}


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def explain(step: str) -> None:
    info = STEP_INFO[step]
    # Resolve bucket name lazily so --help works offline.
    resource = info["resource"]
    if step == "upload":
        try:
            resource = f"S3 bucket {C.bucket_name()}"
        except Exception:
            pass
    print(
        f"\n{'=' * 60}\n"
        f"STEP: {step}\n"
        f"  resource : {resource}\n"
        f"  why      : {info['why']}\n"
        f"  input    : {info['input']}\n"
        f"  output   : {info['output']}\n"
        f"{'=' * 60}",
        flush=True,
    )


def confirm(step: str, auto_yes: bool) -> bool:
    explain(step)
    if auto_yes:
        return True
    try:
        ans = input(f"Run '{step}'? [Y/n/q] ").strip().lower()
    except EOFError:
        ans = "y"
    if ans in ("q", "quit"):
        raise SystemExit("Stopped before remaining steps.")
    return ans in ("", "y", "yes")


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


def run_one(
    step: str,
    *,
    start: date,
    days: int,
    rows: int,
    no_upload: bool,
    skip_query: bool,
    started: datetime,
    job_run_id: str | None,
) -> str | None:
    """Execute a single named stage. Returns updated job_run_id."""
    if step == "upload":
        if no_upload:
            log("skipping upload (--no-upload)")
        else:
            upload_data(start, days, rows)
    elif step == "raw-crawler":
        run_crawler(C.RAW_CRAWLER)
    elif step == "job":
        job_run_id = run_job()
    elif step == "curated-crawler":
        run_crawler(C.CURATED_CRAWLER)
    elif step == "query":
        if skip_query:
            log("skipping query (--skip-query)")
        else:
            query.run_demo()
    elif step == "cost":
        print_cost(started, job_run_id)
    else:
        raise ValueError(f"unknown step: {step}")
    return job_run_id


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-10-01")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--rows", type=int, default=20000, help="trades per day")
    ap.add_argument(
        "--mode",
        choices=["interactive", "steps", "workflow"],
        default="interactive",
        help="interactive=pause before each stage (default); steps=all stages no pause; workflow=Glue workflow",
    )
    ap.add_argument(
        "--step",
        choices=STEPS,
        help="run exactly one stage and exit (implies --mode steps for that stage)",
    )
    ap.add_argument("--yes", "-y", action="store_true", help="do not prompt in interactive mode")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--reset-bookmark", action="store_true")
    ap.add_argument("--skip-query", action="store_true")
    a = ap.parse_args()

    started = datetime.now(timezone.utc)
    start = date.fromisoformat(a.start)
    job_run_id: str | None = None

    if a.reset_bookmark:
        glue.reset_job_bookmark(JobName=C.JOB_NAME)
        log("job bookmark reset: the next run reprocesses every partition")

    # Single stage -------------------------------------------------------
    if a.step:
        if confirm(a.step, auto_yes=a.yes or a.mode != "interactive"):
            job_run_id = run_one(
                a.step,
                start=start,
                days=a.days,
                rows=a.rows,
                no_upload=a.no_upload,
                skip_query=a.skip_query,
                started=started,
                job_run_id=job_run_id,
            )
        return

    # Full Glue workflow (crawler→job→crawler chained by triggers) ------
    if a.mode == "workflow":
        if not a.no_upload:
            if confirm("upload", auto_yes=a.yes):
                upload_data(start, a.days, a.rows)
        explain("raw-crawler")
        explain("job")
        explain("curated-crawler")
        print("\n(Starting Glue workflow — those three run chained by triggers.)\n")
        if not a.yes:
            try:
                ans = input("Start workflow? [Y/n/q] ").strip().lower()
            except EOFError:
                ans = "y"
            if ans in ("q", "quit"):
                raise SystemExit("Stopped.")
            if ans not in ("", "y", "yes"):
                raise SystemExit("Workflow not started.")
        job_run_id = run_workflow()
        if not a.skip_query and confirm("query", auto_yes=a.yes):
            query.run_demo()
        if confirm("cost", auto_yes=a.yes):
            print_cost(started, job_run_id)
        return

    # Interactive / steps: run each stage yourself ----------------------
    auto_yes = a.yes or a.mode == "steps"
    for step in STEPS:
        if step == "upload" and a.no_upload:
            continue
        if step == "query" and a.skip_query:
            continue
        if not confirm(step, auto_yes=auto_yes):
            log(f"skipped {step}")
            continue
        job_run_id = run_one(
            step,
            start=start,
            days=a.days,
            rows=a.rows,
            no_upload=a.no_upload,
            skip_query=a.skip_query,
            started=started,
            job_run_id=job_run_id,
        )


if __name__ == "__main__":
    main()
