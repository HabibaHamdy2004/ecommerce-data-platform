from pyspark.sql import SparkSession


# ============================================================
# Spark Session + Iceberg REST Catalog + MinIO S3A
# ============================================================

spark = (
    SparkSession.builder
    .appName("CreateAllIcebergTables")

    # ========================================================
    # Iceberg Catalog
    # ========================================================

    .config(
        "spark.sql.catalog.ecommerce",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.ecommerce.type",
        "rest"
    )
    .config(
        "spark.sql.catalog.ecommerce.uri",
        "http://ecommerce-iceberg-rest:8181"
    )

    # ========================================================
    # Iceberg Warehouse
    # ========================================================

    .config(
        "spark.sql.catalog.ecommerce.warehouse",
        "s3://iceberg-warehouse/"
    )

    # ========================================================
    # Iceberg + MinIO
    # ========================================================

    .config(
        "spark.sql.catalog.ecommerce.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config(
        "spark.sql.catalog.ecommerce.s3.endpoint",
        "http://ecommerce-minio:9000"
    )
    .config(
        "spark.sql.catalog.ecommerce.s3.path-style-access",
        "true"
    )
    .config(
        "spark.sql.catalog.ecommerce.s3.access-key-id",
        "admin"
    )
    .config(
        "spark.sql.catalog.ecommerce.s3.secret-access-key",
        "admin12345"
    )
    .config(
        "spark.sql.catalog.ecommerce.client.region",
        "us-east-1"
    )

    # ========================================================
    # Hadoop S3A + MinIO
    # Used for reading processed Parquet files
    # ========================================================

    .config(
        "spark.hadoop.fs.s3a.access.key",
        "admin"
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "admin12345"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://ecommerce-minio:9000"
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    .getOrCreate()
)


print("=" * 80)
print("ICEBERG TABLE CREATION STARTED")
print("=" * 80)


# ============================================================
# Create namespace
# ============================================================

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS ecommerce.raw
""")

print("Namespace ecommerce.raw is ready")


# ============================================================
# Tables
# ============================================================

tables = [
    "customers",
    "products",
    "sellers",
    "orders",
    "order_items",
    "payments"
]


# ============================================================
# Create Iceberg tables
# ============================================================

for table_name in tables:

    print()
    print("=" * 80)
    print(f"PROCESSING: {table_name}")
    print("=" * 80)

    # ========================================================
    # Read processed Parquet from MinIO
    # ========================================================

    df = spark.read.parquet(
        f"s3a://processed/{table_name}"
    )

    row_count = df.count()

    print(f"Processed rows: {row_count:,}")

    # ========================================================
    # Create / replace Iceberg table
    # ========================================================

    df.writeTo(
        f"ecommerce.raw.{table_name}"
    ).using(
        "iceberg"
    ).tableProperty(
        "format-version",
        "2"
    ).createOrReplace()

    print(
        f"Iceberg table created: ecommerce.raw.{table_name}"
    )


# ============================================================
# Verify all tables
# ============================================================

print()
print("=" * 80)
print("ICEBERG TABLES")
print("=" * 80)

spark.sql("""
    SHOW TABLES IN ecommerce.raw
""").show(
    truncate=False
)


# ============================================================
# Verify row counts
# ============================================================

print()
print("=" * 80)
print("ICEBERG ROW COUNTS")
print("=" * 80)

for table_name in tables:

    count = spark.table(
        f"ecommerce.raw.{table_name}"
    ).count()

    print(
        f"{table_name:<15} {count:,} rows"
    )


print()
print("=" * 80)
print("ALL ICEBERG TABLES CREATED SUCCESSFULLY")
print("=" * 80)


spark.stop()