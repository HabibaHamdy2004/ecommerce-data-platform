
from datetime import datetime, timezone
from io import BytesIO
import time
import os

import boto3
import pandas as pd
import pyarrow.parquet as pq
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator


ACCOUNT_ID = "70506183160351"
DBT_JOB_ID = "70506183141656"

TABLES = {
    "customers": {
        "target": "CUSTOMERS",
        "keys": ["CUSTOMER_ID"],
    },
    "orders": {
        "target": "ORDERS",
        "keys": ["ORDER_ID"],
    },
    "order_items": {
        "target": "ORDER_ITEMS",
        "keys": ["ORDER_ID", "ORDER_ITEM_ID"],
    },
    "payments": {
        "target": "PAYMENTS",
        "keys": ["ORDER_ID", "PAYMENT_SEQUENTIAL"],
    },
    "products": {
        "target": "PRODUCTS",
        "keys": ["PRODUCT_ID"],
    },
    "sellers": {
        "target": "SELLER",
        "keys": ["SELLER_ID"],
    },
}


def load_snowflake():
    from snowflake.connector.pandas_tools import write_pandas
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
    )

    hook = SnowflakeHook(snowflake_conn_id="snowflake_dwh")
    conn = hook.get_conn()

    try:
        for prefix, config in TABLES.items():
            table = config["target"]
            key_columns = config["keys"]

            # 1. Read processed Parquet files from MinIO.
            paginator = s3.get_paginator("list_objects_v2")

            keys = [
                obj["Key"]
                for page in paginator.paginate(
                    Bucket="processed",
                    Prefix=f"{prefix}/",
                )
                for obj in page.get("Contents", [])
                if obj["Key"].endswith(".parquet")
            ]

            if not keys:
                raise ValueError(
                    f"No Parquet files found for {prefix}"
                )

            frames = []

            for key in keys:
                obj = s3.get_object(
                    Bucket="processed",
                    Key=key,
                )

                frame = pq.read_table(
                    BytesIO(obj["Body"].read())
                ).to_pandas()

                frames.append(frame)

            df = pd.concat(frames, ignore_index=True)

            # 2. Validate ORDERS timestamps before converting them.
            if prefix == "orders":
                date_col = "order_purchase_timestamp"

                if date_col not in df.columns:
                    raise ValueError(
                        f"ORDERS source is missing {date_col}"
                    )

                source_dates = pd.to_datetime(
                    df[date_col],
                    errors="coerce",
                )

                invalid_parse = (
                    df[date_col].notna()
                    & source_dates.isna()
                )

                if invalid_parse.any():
                    examples = df.loc[
                        invalid_parse, date_col
                    ].head(5).tolist()

                    raise ValueError(
                        "ORDERS contains unparseable timestamps. "
                        f"Examples: {examples}"
                    )

                valid_dates = source_dates.dropna()

                if not valid_dates.empty:
                    invalid_years = (
                        (valid_dates.dt.year < 2016)
                        | (valid_dates.dt.year > 2020)
                    )

                    if invalid_years.any():
                        raise ValueError(
                            "ORDERS source contains timestamps "
                            "outside the expected 2016-2020 range. "
                            "Upload and MERGE were stopped."
                        )

                print("=== ORDERS SOURCE TIMESTAMP CHECK ===")
                print("Original dtype:", df[date_col].dtype)
                print("Sample:", source_dates.head(5).tolist())
                print("Minimum:", source_dates.min())
                print("Maximum:", source_dates.max()
                      )
                if prefix == "orders":
                   df[date_col] = source_dates
            # 3. Convert datetime columns to strings before write_pandas.
            # This avoids sending datetime64[ns] values directly
            # through the Parquet upload path.
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(
                    df[col].dtype
                ):
                    df[col] = df[col].dt.strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    )

            # Snowflake table and column names are uppercase.
            df.columns = [str(c).upper() for c in df.columns]

            if df.columns.duplicated().any():
                raise ValueError(
                    f"{table}: duplicate column names in source."
                )

            # 4. Read the target table's actual columns.
            target_columns = [
                row[0]
                for row in hook.get_records(
                    f"""
                    SELECT COLUMN_NAME
                    FROM OLIST_DWH.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'RAW'
                      AND TABLE_NAME = '{table}'
                    ORDER BY ORDINAL_POSITION
                    """
                )
            ]

            if not target_columns:
                raise ValueError(
                    f"{table}: target table or columns not found."
                )

            missing_columns = [
                col for col in target_columns
                if col not in df.columns
            ]

            if missing_columns:
                raise ValueError(
                    f"{table}: source is missing target columns: "
                    f"{missing_columns}"
                )

            missing_keys = [
                col for col in key_columns
                if col not in target_columns
            ]

            if missing_keys:
                raise ValueError(
                    f"{table}: target is missing key columns: "
                    f"{missing_keys}"
                )

            # Keep only columns that exist in the target.
            df = df[target_columns]
            source_rows = len(df)

            # 5. Reject NULL and duplicate source keys.
            if df[key_columns].isna().any().any():
                raise ValueError(
                    f"{table}: source contains NULL merge keys."
                )

            duplicated = df.duplicated(
                subset=key_columns,
                keep=False,
            )

            if duplicated.any():
                examples = df.loc[
                    duplicated, key_columns
                ].head(5).to_dict("records")

                raise ValueError(
                    f"{table}: source contains duplicate merge keys. "
                    f"Examples: {examples}"
                )

            print(
                f"{table}: source rows={source_rows}; "
                f"merge keys={key_columns}"
            )

            # 6. Create a temporary staging table.
            staging = f"_AIRFLOW_LOAD_{table}"

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE OR REPLACE TEMPORARY TABLE
                    OLIST_DWH.RAW.{staging}
                    LIKE OLIST_DWH.RAW.{table}
                    """
                )

            # Upload into staging only. The target is not changed here.
            success, _, loaded_rows, copy_output = write_pandas(
                conn,
                df,
                table_name=staging,
                database="OLIST_DWH",
                schema="RAW",
                quote_identifiers=False,
            )

            if not success or loaded_rows != source_rows:
                raise RuntimeError(
                    f"{table}: staging load failed. "
                    f"Expected {source_rows}; loaded {loaded_rows}. "
                    f"Details: {copy_output}"
                )

            # 7. Validate timestamps in Snowflake staging before MERGE.
            if table == "ORDERS":
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            COUNT_IF(
                                ORDER_PURCHASE_TIMESTAMP IS NOT NULL
                                AND (
                                    YEAR(ORDER_PURCHASE_TIMESTAMP) < 2016
                                    OR YEAR(ORDER_PURCHASE_TIMESTAMP) > 2020
                                )
                            ),
                            MIN(ORDER_PURCHASE_TIMESTAMP),
                            MAX(ORDER_PURCHASE_TIMESTAMP)
                        FROM OLIST_DWH.RAW._AIRFLOW_LOAD_ORDERS
                        """
                    )

                    bad_rows, min_ts, max_ts = cur.fetchone()

                    print("=== STAGING TIMESTAMP VALIDATION ===")
                    print("Invalid timestamp rows:", bad_rows)
                    print("Minimum timestamp:", min_ts)
                    print("Maximum timestamp:", max_ts)

                    cur.execute(
                        """
                        SELECT
                            ORDER_ID,
                            ORDER_PURCHASE_TIMESTAMP::VARCHAR
                        FROM OLIST_DWH.RAW._AIRFLOW_LOAD_ORDERS
                        LIMIT 10
                        """
                    )

                    print("=== STAGING TIMESTAMP SAMPLE ===")
                    for row in cur.fetchall():
                        print(row)

                    if bad_rows is None or bad_rows > 0:
                        raise ValueError(
                            "Unsafe ORDERS timestamps detected in staging. "
                            "MERGE was blocked."
                        )

                    if min_ts is None or max_ts is None:
                        raise ValueError(
                            "ORDERS staging timestamp validation failed: "
                            "minimum or maximum timestamp is NULL."
                        )

            # 8. Build the MERGE statement.
            on_clause = " AND ".join(
                f"T.{column} = S.{column}"
                for column in key_columns
            )

            non_key_columns = [
                col for col in target_columns
                if col not in key_columns
            ]

            if not non_key_columns:
                raise ValueError(
                    f"{table}: no non-key columns available for MERGE."
                )

            insert_columns = ", ".join(target_columns)

            source_values = ", ".join(
                f"S.{col}" for col in target_columns
            )

            update_clause = ", ".join(
                f"T.{col} = S.{col}"
                for col in non_key_columns
            )

            changed_clause = " OR ".join(
                f"T.{col} IS DISTINCT FROM S.{col}"
                for col in non_key_columns
            )

            merge_sql = f"""
                MERGE INTO OLIST_DWH.RAW.{table} T
                USING OLIST_DWH.RAW.{staging} S
                ON {on_clause}
                WHEN MATCHED AND ({changed_clause}) THEN
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({source_values})
            """

            # 9. Merge only after staging validation passes.
            try:
                with conn.cursor() as cur:
                    cur.execute("BEGIN")
                    cur.execute(merge_sql)
                    merge_result = cur.fetchone()
                    print(f"{table}: MERGE result: {merge_result}")
                    cur.execute("COMMIT")

            except Exception:
                with conn.cursor() as cur:
                    cur.execute("ROLLBACK")
                raise

            # 10. Validate target keys.
            target_rows = hook.get_first(
                f"SELECT COUNT(*) FROM OLIST_DWH.RAW.{table}"
            )[0]

            duplicate_groups = hook.get_first(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT {", ".join(key_columns)}
                    FROM OLIST_DWH.RAW.{table}
                    GROUP BY {", ".join(key_columns)}
                    HAVING COUNT(*) > 1
                )
                """
            )[0]

            null_condition = " OR ".join(
                f"{col} IS NULL" for col in key_columns
            )

            null_keys = hook.get_first(
                f"""
                SELECT COUNT(*)
                FROM OLIST_DWH.RAW.{table}
                WHERE {null_condition}
                """
            )[0]

            if duplicate_groups or null_keys:
                raise ValueError(
                    f"{table}: target validation failed. "
                    f"Duplicate key groups={duplicate_groups}; "
                    f"null-key rows={null_keys}."
                )

            print(
                f"{table}: MERGE complete. "
                f"Source rows={source_rows}; "
                f"target rows={target_rows}; "
                f"duplicate key groups={duplicate_groups}; "
                f"null-key rows={null_keys}."
            )

        print("All six RAW tables merged and validated.")

    finally:
        conn.close()


def run_dbt_cloud():
    connection = BaseHook.get_connection("dbt_cloud")
    token = connection.password

    if not token:
        raise ValueError(
            "The dbt_cloud connection has no API token."
        )

    base_url = (
        f"https://ee590.us1.dbt.com/api/v2/accounts/"
        f"{ACCOUNT_ID}"
    )

    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }

    response = requests.post(
        f"{base_url}/jobs/{DBT_JOB_ID}/run/",
        headers=headers,
        json={
            "cause": "Triggered by Airflow Olist pipeline"
        },
        timeout=30,
    )

    response.raise_for_status()
    result = response.json()

    if not result.get("status", {}).get("is_success"):
        raise RuntimeError(
            f"Could not trigger dbt Cloud: {result}"
        )

    run_id = result["data"]["id"]
    print(f"dbt Cloud run started. Run ID: {run_id}")

    deadline = time.monotonic() + 1800

    while time.monotonic() < deadline:
        time.sleep(15)

        response = requests.get(
            f"{base_url}/runs/{run_id}/",
            headers=headers,
            timeout=30,
        )

        response.raise_for_status()
        run_data = response.json()["data"]
        run_status = run_data.get("status")

        print(
            f"dbt Cloud run {run_id}: status={run_status}"
        )

        if run_status == 10:
            print("dbt Cloud build completed successfully.")
            return

        if run_status in (20, 30):
            raise RuntimeError(
                f"dbt Cloud run {run_id} ended with status "
                f"{run_status}. Check dbt Cloud logs."
            )

    raise TimeoutError(
        f"dbt Cloud run {run_id} did not finish within 30 minutes."
    )


with DAG(
    dag_id="olist_pipeline",
    description="Olist cleaning, Snowflake merge, and dbt Cloud",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["olist", "data-engineering"],
) as dag:

    clean_olist_data = DockerOperator(
        task_id="clean_olist_data",
        image="ecommerce-data-platform-spark:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="ecommerce-data-platform_default",
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        command=[
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[*]",
            "--conf",
            "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
            "--conf",
            "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf",
            "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
            "--conf",
            f"spark.hadoop.fs.s3a.access.key={os.environ['AWS_ACCESS_KEY_ID']}",
            "--conf",
            f"spark.hadoop.fs.s3a.secret.key={os.environ['AWS_SECRET_ACCESS_KEY']}",
            "--conf",
            "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "/opt/spark/scripts/clean_data.py",
        ],
        environment={
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
        },
    )

    load_snowflake_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_snowflake,
    )

    run_dbt_cloud_task = PythonOperator(
        task_id="run_dbt_cloud",
        python_callable=run_dbt_cloud,
    )

    clean_olist_data >> load_snowflake_task >> run_dbt_cloud_task

