from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestMinIO")
    .getOrCreate()
)

print("=" * 80)
print("TESTING SPARK -> MINIO")
print("=" * 80)

df = (
    spark.read
    .option("header", True)
    .csv("s3a://raw/customers.csv")
)

print(f"Rows: {df.count():,}")

df.show(5, truncate=False)

print("=" * 80)
print("MINIO READ SUCCESS")
print("=" * 80)

spark.stop()