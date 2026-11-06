
import os

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestMinIO")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.environ["AWS_ACCESS_KEY_ID"],
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .getOrCreate()
)

try:
    print("=" * 60)
    print("TESTING SPARK -> MINIO")
    print("=" * 60)

    df = (
        spark.read
        .option("header", True)
        .csv("s3a://raw/customers.csv")
    )

    print(f"Rows: {df.count():,}")
    df.show(5, truncate=False)
    print("MINIO READ SUCCESS")

finally:
    spark.stop()
