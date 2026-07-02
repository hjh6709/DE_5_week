from __future__ import annotations

from datetime import datetime, timedelta
from io import StringIO
import os

import boto3
import pandas as pd
import psycopg2

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from airflow.hooks.base import BaseHook


BUCKET = os.environ.get("DE5_S3_BUCKET", "YOUR_BUCKET_NAME")
DATA_PATH = "/opt/airflow/data/job_postings.csv"
REQUIRED_COLUMNS = ["id", "title", "company", "location", "job_category", "due_date"]


def failure_callback(context):
    task_id = context.get("task_instance").task_id
    dag_run = context.get("dag_run")
    print(f"Pipeline failure: task={task_id}, dag_run={dag_run}")


def get_s3_client():
    conn = BaseHook.get_connection("aws_default")
    extra = conn.extra_dejson
    region_name = extra.get("region_name") or os.environ.get("AWS_DEFAULT_REGION") or "ap-northeast-2"
    kwargs = {"region_name": region_name}
    if conn.login and conn.password:
        kwargs["aws_access_key_id"] = conn.login
        kwargs["aws_secret_access_key"] = conn.password
    return boto3.client("s3", **kwargs)


def ensure_bucket(s3):
    if BUCKET == "YOUR_BUCKET_NAME":
        raise ValueError("DE5_S3_BUCKET 환경변수 또는 BUCKET 값을 실제 AWS S3 버킷명으로 설정하세요.")
    buckets = [bucket["Name"] for bucket in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in buckets:
        region = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
        if region == "us-east-1":
            s3.create_bucket(Bucket=BUCKET)
        else:
            s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": region})


def execution_date_from_context(context):
    logical_date = context["logical_date"]
    if isinstance(logical_date, str):
        return logical_date[:10]
    return logical_date.strftime("%Y-%m-%d")


@dag(
    dag_id="wanted_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * *",
    catchup=True,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_callback,
    },
    tags=["de5", "wanted", "s3", "redshift"],
)
def wanted_pipeline():
    @task
    def extract(**context):
        execution_date = execution_date_from_context(context)
        s3 = get_s3_client()
        ensure_bucket(s3)

        df = pd.read_csv(DATA_PATH, encoding="utf-8-sig")
        row_count = len(df)
        print(f"extract row count: {row_count}")
        print(df.head())

        raw_key = f"raw/{execution_date}/job_postings.json"
        # raw/{execution_date}/job_postings.json
        body = df.to_json(orient="records", force_ascii=False, date_format="iso")
        s3.put_object(Bucket=BUCKET, Key=raw_key, Body=body.encode("utf-8"), ContentType="application/json")

        metadata = s3.head_object(Bucket=BUCKET, Key=raw_key)
        print(
            "S3 raw upload 확인: "
            f"Key={raw_key}, ContentLength={metadata['ContentLength']}, LastModified={metadata['LastModified']}"
        )
        return {"bucket": BUCKET, "key": raw_key, "row_count": row_count, "execution_date": execution_date}

    @task
    def transform(raw_result):
        execution_date = raw_result["execution_date"]
        s3 = get_s3_client()

        obj = s3.get_object(Bucket=raw_result["bucket"], Key=raw_result["key"])
        df = pd.read_json(StringIO(obj["Body"].read().decode("utf-8")))

        before_count = len(df)
        df = df[REQUIRED_COLUMNS]
        df["due_date"] = pd.to_datetime(df["due_date"], errors="coerce").dt.date
        run_date = pd.to_datetime(execution_date).date()
        df = df.dropna(subset=REQUIRED_COLUMNS)
        df = df[df["due_date"] >= run_date]
        df = df.drop_duplicates(subset=["id"], keep="first")
        after_count = len(df)

        print(f"transform before row count: {before_count}")
        print(f"transform after row count: {after_count}")

        processed_key = f"processed/{execution_date}/job_postings_clean.json"
        # processed/{execution_date}/job_postings_clean.json
        body = df.to_json(orient="records", force_ascii=False, date_format="iso")
        s3.put_object(Bucket=BUCKET, Key=processed_key, Body=body.encode("utf-8"), ContentType="application/json")

        metadata = s3.head_object(Bucket=BUCKET, Key=processed_key)
        print(
            "S3 processed 저장 확인: "
            f"Key={processed_key}, ContentLength={metadata['ContentLength']}, LastModified={metadata['LastModified']}"
        )
        return {"bucket": BUCKET, "key": processed_key, "row_count": after_count, "execution_date": execution_date}

    @task
    def load(processed_result):
        redshift_conn = BaseHook.get_connection("redshift_default")
        extra = redshift_conn.extra_dejson
        conn = psycopg2.connect(
            host=redshift_conn.host,
            port=redshift_conn.port or 5439,
            dbname=redshift_conn.schema or "dev",
            user=redshift_conn.login,
            password=redshift_conn.password,
        )

        create_sql = """
        CREATE TABLE IF NOT EXISTS job_postings (
            id VARCHAR,
            title VARCHAR,
            company VARCHAR,
            location VARCHAR,
            job_category VARCHAR,
            due_date DATE
        );
        """
        query_sql = """
        SELECT job_category, COUNT(*) AS cnt
        FROM job_postings
        GROUP BY job_category
        ORDER BY cnt DESC;
        """
        s3_uri = f"s3://{processed_result['bucket']}/{processed_result['key']}"
        iam_role = extra.get("iam_role") or os.environ.get("REDSHIFT_IAM_ROLE")
        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        if iam_role and iam_role != "REDSHIFT_IAM_ROLE_ARN":
            copy_auth = f"IAM_ROLE '{iam_role}'"
        elif access_key and secret_key:
            copy_auth = f"CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'"
        else:
            raise ValueError("Redshift COPY를 위해 redshift_default extra의 iam_role 또는 AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY가 필요합니다.")

        copy_sql = f"""
        COPY job_postings
        FROM '{s3_uri}'
        {copy_auth}
        FORMAT AS JSON 'auto'
        TIMEFORMAT 'auto'
        TRUNCATECOLUMNS;
        """

        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(create_sql)
                    cur.execute("TRUNCATE TABLE job_postings;")
                    cur.execute(copy_sql)
                    print(f"Redshift COPY 적재 완료: {s3_uri}")
                    cur.execute(query_sql)
                    results = cur.fetchall()

            print("Redshift 직군별 공고 수")
            for job_category, cnt in results:
                print(f"{job_category}: {cnt}")
            return {"s3_uri": s3_uri, "category_counts": results}
        finally:
            conn.close()

    raw_result = extract()
    processed_result = transform(raw_result)
    load(processed_result)


wanted_pipeline()
