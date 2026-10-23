"""
Create (or update) every AWS resource for the pipeline, using boto3 only.
Safe to re-run: each step is create-or-update.

    python deploy.py

Creates:
  S3       bucket with raw/ curated/ scripts/ tmp/ athena-results/ prefixes
  IAM      role that Glue assumes (AWSGlueServiceRole + access to this bucket only)
  Glue     2 catalog databases, 1 CSV classifier, 2 crawlers, 1 Spark job,
           1 workflow with 3 triggers (start -> raw crawler -> ETL job -> curated crawler)
  Athena   workgroup with a 1 GB per-query scan limit
"""
import json
import time
from pathlib import Path

from botocore.exceptions import ClientError

import config as C

HERE = Path(__file__).parent
s3 = C.client("s3")
iam = C.client("iam")
glue = C.client("glue")
athena = C.client("athena")


def step(msg: str) -> None:
    print(f"\n==> {msg}")


def error_code(e: ClientError) -> str:
    return e.response["Error"]["Code"]


# ---------------------------------------------------------------- S3 ----
def create_bucket(bucket: str) -> None:
    step(f"S3 bucket s3://{bucket}")
    try:
        if C.REGION == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": C.REGION})
        print("   created")
    except ClientError as e:
        if error_code(e) != "BucketAlreadyOwnedByYou":
            raise
        print("   already exists")
    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={k: True for k in (
            "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")},
    )
    # Housekeeping so scratch data never piles up and costs money.
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={"Rules": [
            {"ID": "expire-athena-results", "Status": "Enabled",
             "Filter": {"Prefix": C.ATHENA_PREFIX}, "Expiration": {"Days": 7}},
            {"ID": "expire-glue-tmp", "Status": "Enabled",
             "Filter": {"Prefix": C.TMP_PREFIX}, "Expiration": {"Days": 3}},
        ]},
    )


def upload_script(bucket: str) -> None:
    step(f"Upload Glue job script -> s3://{bucket}/{C.SCRIPT_KEY}")
    s3.upload_file(str(HERE / "glue_jobs" / "trades_etl.py"), bucket, C.SCRIPT_KEY)


# --------------------------------------------------------------- IAM ----
def create_role(bucket: str) -> str:
    step(f"IAM role {C.ROLE_NAME}")
    trust = {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
    created = False
    try:
        arn = iam.create_role(RoleName=C.ROLE_NAME, AssumeRolePolicyDocument=json.dumps(trust),
                              Description="Glue demo pipeline role")["Role"]["Arn"]
        created = True
        print("   created")
    except ClientError as e:
        if error_code(e) != "EntityAlreadyExists":
            raise
        arn = iam.get_role(RoleName=C.ROLE_NAME)["Role"]["Arn"]
        print("   already exists")

    # Managed policy: Glue service basics (catalog, CloudWatch logs, ...).
    iam.attach_role_policy(RoleName=C.ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
    # The managed policy only covers buckets named aws-glue-*, so grant ours explicitly.
    iam.put_role_policy(RoleName=C.ROLE_NAME, PolicyName="pipeline-bucket-access", PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
             "Resource": f"arn:aws:s3:::{bucket}"},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
             "Resource": f"arn:aws:s3:::{bucket}/*"},
        ],
    }))
    if created:
        print("   waiting 15s for IAM propagation...")
        time.sleep(15)
    return arn


def retry_on_iam_lag(fn, *args, **kwargs):
    """New IAM roles take a few seconds before Glue can assume them."""
    for attempt in range(8):
        try:
            return fn(*args, **kwargs)
        except ClientError as e:
            msg = str(e)
            if error_code(e) == "InvalidInputException" and ("role" in msg.lower() or "assume" in msg.lower()):
                print(f"   role not ready yet, retrying ({attempt + 1})...")
                time.sleep(5)
                continue
            raise
    return fn(*args, **kwargs)


# ------------------------------------------------------- Glue catalog ----
def create_databases() -> None:
    for db, desc in [(C.RAW_DB, "Raw landing zone (CSV) - tables made by raw crawler"),
                     (C.CURATED_DB, "Curated zone (Parquet) - tables made by curated crawler")]:
        step(f"Glue database {db}")
        try:
            glue.create_database(DatabaseInput={"Name": db, "Description": desc})
            print("   created")
        except glue.exceptions.AlreadyExistsException:
            print("   already exists")


def create_classifier() -> None:
    # Tell the crawler our CSVs always have a header row (the built-in
    # classifier guesses, and guesses wrong when every column looks like text).
    step(f"Glue CSV classifier {C.CSV_CLASSIFIER}")
    spec = {"Name": C.CSV_CLASSIFIER, "Delimiter": ",", "QuoteSymbol": '"',
            "ContainsHeader": "PRESENT", "DisableValueTrimming": False, "AllowSingleColumn": False}
    try:
        glue.create_classifier(CsvClassifier=spec)
        print("   created")
    except glue.exceptions.AlreadyExistsException:
        glue.update_classifier(CsvClassifier=spec)
        print("   updated")


def upsert_crawler(name: str, role_arn: str, db: str, paths: list[str], classifiers: list[str], desc: str) -> None:
    step(f"Glue crawler {name}")
    spec = dict(
        Role=role_arn,
        DatabaseName=db,
        Description=desc,
        Targets={"S3Targets": [{"Path": p} for p in paths]},
        Classifiers=classifiers,
        SchemaChangePolicy={"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"},
        RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
        # One table per S3 path, even if partition folders have slightly different schemas.
        Configuration=json.dumps({
            "Version": 1.0,
            "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"},
            "CrawlerOutput": {"Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}},
        }),
    )
    try:
        retry_on_iam_lag(glue.create_crawler, Name=name, **spec)
        print("   created")
    except glue.exceptions.AlreadyExistsException:
        retry_on_iam_lag(glue.update_crawler, Name=name, **spec)
        print("   updated")


# ------------------------------------------------------------ Glue job ----
def upsert_job(role_arn: str, bucket: str) -> None:
    step(f"Glue Spark job {C.JOB_NAME}  (Glue {C.GLUE_VERSION}, {C.NUMBER_OF_WORKERS} x {C.WORKER_TYPE}, "
         f"{C.EXECUTION_CLASS}, timeout {C.JOB_TIMEOUT_MINUTES} min)")
    spec = dict(
        Description="Raw CSV trades -> clean Parquet + daily stats + rejects",
        Role=role_arn,
        Command={"Name": "glueetl", "ScriptLocation": f"s3://{bucket}/{C.SCRIPT_KEY}", "PythonVersion": "3"},
        GlueVersion=C.GLUE_VERSION,
        WorkerType=C.WORKER_TYPE,
        NumberOfWorkers=C.NUMBER_OF_WORKERS,
        Timeout=C.JOB_TIMEOUT_MINUTES,
        MaxRetries=0,
        ExecutionClass=C.EXECUTION_CLASS,
        ExecutionProperty={"MaxConcurrentRuns": 1},
        DefaultArguments={
            "--job-bookmark-option": "job-bookmark-enable",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-metrics": "true",
            "--enable-glue-datacatalog": "true",
            "--TempDir": f"s3://{bucket}/{C.TMP_PREFIX}",
            "--raw_db": C.RAW_DB,
            "--trades_table": "trades",
            "--securities_table": "securities",
            "--output_base": f"s3://{bucket}/curated/",
        },
        Tags={"project": C.PROJECT},
    )
    try:
        retry_on_iam_lag(glue.create_job, Name=C.JOB_NAME, **spec)
        print("   created")
    except glue.exceptions.AlreadyExistsException:
        spec.pop("Tags")
        retry_on_iam_lag(glue.update_job, JobName=C.JOB_NAME, JobUpdate=spec)
        print("   updated")


# ------------------------------------------------------- Glue workflow ----
def upsert_workflow() -> None:
    step(f"Glue workflow {C.WORKFLOW_NAME}")
    try:
        glue.create_workflow(Name=C.WORKFLOW_NAME, MaxConcurrentRuns=1,
                             Description="raw crawler -> ETL job -> curated crawler")
        print("   created")
    except glue.exceptions.AlreadyExistsException:
        print("   already exists")

    triggers = [
        # Entry point. Swap to Type=SCHEDULED + Schedule="cron(0 6 ? * MON-FRI *)" for a nightly batch.
        dict(Name=C.TRIGGER_START, Type="ON_DEMAND",
             Actions=[{"CrawlerName": C.RAW_CRAWLER}]),
        dict(Name=C.TRIGGER_AFTER_RAW_CRAWL, Type="CONDITIONAL", StartOnCreation=True,
             Predicate={"Logical": "ANY", "Conditions": [
                 {"LogicalOperator": "EQUALS", "CrawlerName": C.RAW_CRAWLER, "CrawlState": "SUCCEEDED"}]},
             Actions=[{"JobName": C.JOB_NAME}]),
        dict(Name=C.TRIGGER_AFTER_JOB, Type="CONDITIONAL", StartOnCreation=True,
             Predicate={"Logical": "ANY", "Conditions": [
                 {"LogicalOperator": "EQUALS", "JobName": C.JOB_NAME, "State": "SUCCEEDED"}]},
             Actions=[{"CrawlerName": C.CURATED_CRAWLER}]),
    ]
    for t in triggers:
        try:
            glue.create_trigger(WorkflowName=C.WORKFLOW_NAME, **t)
            print(f"   trigger {t['Name']}: created")
        except glue.exceptions.AlreadyExistsException:
            update = {k: v for k, v in t.items() if k in ("Name", "Actions", "Predicate")}
            glue.update_trigger(Name=t["Name"], TriggerUpdate=update)
            print(f"   trigger {t['Name']}: updated")


# -------------------------------------------------------------- Athena ----
def upsert_workgroup(bucket: str) -> None:
    step(f"Athena workgroup {C.ATHENA_WORKGROUP}")
    cfg = {
        "ResultConfiguration": {"OutputLocation": f"s3://{bucket}/{C.ATHENA_PREFIX}"},
        "EnforceWorkGroupConfiguration": True,
        "PublishCloudWatchMetricsEnabled": False,
        "BytesScannedCutoffPerQuery": C.ATHENA_SCAN_LIMIT_BYTES,  # cost guard
    }
    try:
        athena.create_work_group(Name=C.ATHENA_WORKGROUP, Configuration=cfg,
                                 Description="Glue demo queries")
        print("   created")
    except ClientError as e:
        if "already" not in str(e).lower():
            raise
        athena.update_work_group(WorkGroup=C.ATHENA_WORKGROUP, ConfigurationUpdates={
            "ResultConfigurationUpdates": cfg["ResultConfiguration"],
            "EnforceWorkGroupConfiguration": True,
            "BytesScannedCutoffPerQuery": C.ATHENA_SCAN_LIMIT_BYTES,
        })
        print("   updated")


def main() -> None:
    bucket = C.bucket_name()
    print(f"Account {C.account_id()}  region {C.REGION}  bucket {bucket}")
    create_bucket(bucket)
    upload_script(bucket)
    role_arn = create_role(bucket)
    create_databases()
    create_classifier()
    upsert_crawler(C.RAW_CRAWLER, role_arn, C.RAW_DB,
                   [C.s3_uri(C.RAW_TRADES_PREFIX), C.s3_uri(C.RAW_SECURITIES_PREFIX)],
                   [C.CSV_CLASSIFIER], "Discovers raw CSV tables + dt partitions")
    upsert_crawler(C.CURATED_CRAWLER, role_arn, C.CURATED_DB,
                   [C.s3_uri(C.CURATED_CLEAN_PREFIX), C.s3_uri(C.CURATED_STATS_PREFIX),
                    C.s3_uri(C.CURATED_REJECTED_PREFIX)],
                   [], "Discovers curated Parquet tables + trade_date partitions")
    upsert_job(role_arn, bucket)
    upsert_workflow()
    upsert_workgroup(bucket)

    r = C.REGION
    print(f"""
Done. Nothing has run yet, so nothing has been billed except a few cents of S3 at most.

Next:   python run_pipeline.py

Console:
  Workflow  https://{r}.console.aws.amazon.com/glue/home?region={r}#/v2/etl-configuration/workflows/view/{C.WORKFLOW_NAME}
  Job       https://{r}.console.aws.amazon.com/gluestudio/home?region={r}#/editor/job/{C.JOB_NAME}/details
  Crawlers  https://{r}.console.aws.amazon.com/glue/home?region={r}#/v2/data-catalog/crawlers
  Tables    https://{r}.console.aws.amazon.com/glue/home?region={r}#/v2/data-catalog/tables
  Athena    https://{r}.console.aws.amazon.com/athena/home?region={r}#/query-editor
""")


if __name__ == "__main__":
    main()
