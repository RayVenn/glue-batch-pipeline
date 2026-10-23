"""
Delete everything deploy.py created, so nothing keeps costing money.

    python teardown.py            # asks for confirmation
    python teardown.py --yes

Left in place: the shared CloudWatch log groups /aws-glue/* (tiny; delete in the
console if you want).
"""
import argparse

from botocore.exceptions import ClientError

import config as C

glue, s3, iam, athena = (C.client(n) for n in ("glue", "s3", "iam", "athena"))


def quietly(desc: str, fn, *args, **kwargs) -> None:
    try:
        fn(*args, **kwargs)
        print(f"   deleted {desc}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("EntityNotFoundException", "NoSuchEntity", "NoSuchBucket", "InvalidRequestException"):
            print(f"   (not found) {desc}")
        else:
            print(f"   !! {desc}: {e}")
    except Exception as e:  # keep going: a half-finished teardown leaves billable resources
        print(f"   !! {desc}: {type(e).__name__}: {e}")


def empty_bucket(bucket: str) -> None:
    try:
        pages = s3.get_paginator("list_object_versions").paginate(Bucket=bucket)
        for page in pages:
            objs = [{"Key": o["Key"], "VersionId": o["VersionId"]}
                    for o in page.get("Versions", []) + page.get("DeleteMarkers", [])]
            for i in range(0, len(objs), 1000):
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs[i:i + 1000], "Quiet": True})
        print(f"   emptied s3://{bucket}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchBucket":
            raise


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args()
    bucket = C.bucket_name()
    if not a.yes and input(f"Delete all '{C.PROJECT}' resources and s3://{bucket}? [y/N] ").lower() != "y":
        return

    print("==> Glue")
    for t in (C.TRIGGER_START, C.TRIGGER_AFTER_RAW_CRAWL, C.TRIGGER_AFTER_JOB):
        quietly(f"trigger {t}", glue.delete_trigger, Name=t)
    quietly(f"workflow {C.WORKFLOW_NAME}", glue.delete_workflow, Name=C.WORKFLOW_NAME)
    quietly(f"job {C.JOB_NAME}", glue.delete_job, JobName=C.JOB_NAME)  # also drops its bookmark
    for c in (C.RAW_CRAWLER, C.CURATED_CRAWLER):
        quietly(f"crawler {c}", glue.delete_crawler, Name=c)
    quietly(f"classifier {C.CSV_CLASSIFIER}", glue.delete_classifier, Name=C.CSV_CLASSIFIER)
    for db in (C.RAW_DB, C.CURATED_DB):
        quietly(f"database {db} (and its tables)", glue.delete_database, Name=db)

    print("==> Athena")
    quietly(f"workgroup {C.ATHENA_WORKGROUP}", athena.delete_work_group,
            WorkGroup=C.ATHENA_WORKGROUP, RecursiveDeleteOption=True)

    print("==> S3")
    empty_bucket(bucket)
    quietly(f"bucket {bucket}", s3.delete_bucket, Bucket=bucket)

    print("==> IAM")
    quietly("inline policy", iam.delete_role_policy, RoleName=C.ROLE_NAME, PolicyName="pipeline-bucket-access")
    quietly("managed policy attachment", iam.detach_role_policy, RoleName=C.ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
    quietly(f"role {C.ROLE_NAME}", iam.delete_role, RoleName=C.ROLE_NAME)
    print("\nDone.")


if __name__ == "__main__":
    main()
