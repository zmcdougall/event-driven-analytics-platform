from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import boto3


def put_hello_object() -> None:
    # MinIO is S3-compatible. boto3 talks to it like S3.
    s3 = boto3.client(
        "s3"
        , endpoint_url="http://minio:9000"
        , aws_access_key_id="minio"
        , aws_secret_access_key="minio12345"
        , region_name="us-east-1"
    )

    body = f"hello from airflow at {datetime.utcnow().isoformat()}Z\n"
    s3.put_object(
        Bucket="lake"
        , Key="bronze/hello.txt"
        , Body=body.encode("utf-8")
        , ContentType="text/plain"
    )


with DAG(
    dag_id="write_to_minio"
    , start_date=datetime(2026, 1, 1)
    , schedule=None
    , catchup=False
    , tags=["minio", "s3"]
) as dag:

    PythonOperator(
        task_id="put_hello_object"
        , python_callable=put_hello_object
    )
