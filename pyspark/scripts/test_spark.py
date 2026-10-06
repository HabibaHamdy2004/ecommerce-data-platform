from pyspark.sql import SparkSession


# ============================================================
# 1. Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommerceDataProfiling")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)


# ============================================================
# 2. Raw Tables in MinIO
# ============================================================

files = {
    "customers": "s3a://row/customers.csv",
    "orders": "s3a://row/orders.csv",
    "order_items": "s3a://row/order_items.csv",
    "payments": "s3a://row/payments.csv",
    "products": "s3a://row/products.csv",
    "sellers": "s3a://row/sellers.csv",
}


# ============================================================
# 3. Read and Profile Each Table
# ============================================================

for name, path in files.items():

    print("\n" + "=" * 100)
    print(f"TABLE: {name}")
    print("=" * 100)

    # Read CSV
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )

    # Columns
    print("\nCOLUMNS:")
    for column in df.columns:
        print(f"  - {column}")

    # Schema
    print("\nSCHEMA:")
    df.printSchema()

    # Row count
    print("\nROW COUNT:")
    print(df.count())

    # Sample
    print("\nSAMPLE ROWS:")
    df.show(5, truncate=False)


# ============================================================
# 4. Finish
# ============================================================

spark.stop()

print("\n" + "=" * 100)
print("DATA PROFILING COMPLETED SUCCESSFULLY")
print("=" * 100)