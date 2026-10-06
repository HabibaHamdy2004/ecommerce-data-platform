from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommerceDataCleaning")
    .getOrCreate()
)

print("=" * 80)
print("STARTING E-COMMERCE DATA CLEANING")
print("=" * 80)


# ============================================================
# 2. Paths
# ============================================================

RAW_PATH = "s3a://raw"
PROCESSED_PATH = "s3a://processed"


# ============================================================
# 3. Schemas
# ============================================================

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])


product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", DoubleType(), True),
    StructField("product_length_cm", DoubleType(), True),
    StructField("product_height_cm", DoubleType(), True),
    StructField("product_width_cm", DoubleType(), True),
])


seller_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])


order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])


order_item_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
    StructField("shipping_limit_date", StringType(), True),
])


payment_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])


# ============================================================
# 4. Helper Function
# ============================================================

def save_parquet(df, table_name):
    output_path = f"{PROCESSED_PATH}/{table_name}"

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Saved: {output_path}")


# ============================================================
# 5. Customers
# ============================================================

print("\n" + "=" * 80)
print("CUSTOMERS")
print("=" * 80)

customers = (
    spark.read
    .option("header", True)
    .schema(customer_schema)
    .csv(f"{RAW_PATH}/customers.csv")
)

customers = (
    customers
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("customer_unique_id", trim(col("customer_unique_id")))
    .withColumn("customer_city", lower(trim(col("customer_city"))))
    .withColumn("customer_state", upper(trim(col("customer_state"))))
)

customers = customers.dropDuplicates(["customer_id"])

customers = customers.filter(
    col("customer_id").isNotNull()
)

print(f"Customers: {customers.count():,}")

save_parquet(customers, "customers")


# ============================================================
# 6. Products
# ============================================================

print("\n" + "=" * 80)
print("PRODUCTS")
print("=" * 80)

products = (
    spark.read
    .option("header", True)
    .schema(product_schema)
    .csv(f"{RAW_PATH}/products.csv")
)

products = (
    products
    .withColumn("product_id", trim(col("product_id")))
    .withColumn(
        "product_category_name",
        lower(trim(col("product_category_name")))
    )
)

products = products.dropDuplicates(["product_id"])

products = products.filter(
    col("product_id").isNotNull()
)

print(f"Products: {products.count():,}")

save_parquet(products, "products")


# ============================================================
# 7. Sellers
# ============================================================

print("\n" + "=" * 80)
print("SELLERS")
print("=" * 80)

sellers = (
    spark.read
    .option("header", True)
    .schema(seller_schema)
    .csv(f"{RAW_PATH}/sellers.csv")
)

sellers = (
    sellers
    .withColumn("seller_id", trim(col("seller_id")))
    .withColumn("seller_city", lower(trim(col("seller_city"))))
    .withColumn("seller_state", upper(trim(col("seller_state"))))
)

sellers = sellers.dropDuplicates(["seller_id"])

sellers = sellers.filter(
    col("seller_id").isNotNull()
)

print(f"Sellers: {sellers.count():,}")

save_parquet(sellers, "sellers")


# ============================================================
# 8. Orders
# ============================================================

print("\n" + "=" * 80)
print("ORDERS")
print("=" * 80)

orders = (
    spark.read
    .option("header", True)
    .schema(order_schema)
    .csv(f"{RAW_PATH}/orders.csv")
)

orders = (
    orders
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("order_status", lower(trim(col("order_status"))))
)

timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

for column_name in timestamp_columns:
    orders = orders.withColumn(
        column_name,
        to_timestamp(col(column_name))
    )

orders = orders.dropDuplicates(["order_id"])

orders = orders.filter(
    col("order_id").isNotNull()
)

print(f"Orders: {orders.count():,}")

save_parquet(orders, "orders")


# ============================================================
# 9. Order Items
# ============================================================

print("\n" + "=" * 80)
print("ORDER ITEMS")
print("=" * 80)

order_items = (
    spark.read
    .option("header", True)
    .schema(order_item_schema)
    .csv(f"{RAW_PATH}/order_items.csv")
)

order_items = (
    order_items
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("seller_id", trim(col("seller_id")))
)

order_items = order_items.withColumn(
    "shipping_limit_date",
    to_timestamp(col("shipping_limit_date"))
)

order_items = order_items.dropDuplicates(
    ["order_id", "order_item_id"]
)

order_items = order_items.filter(
    col("order_id").isNotNull()
)

print(f"Order Items: {order_items.count():,}")

save_parquet(order_items, "order_items")


# ============================================================
# 10. Payments
# ============================================================

print("\n" + "=" * 80)
print("PAYMENTS")
print("=" * 80)

payments = (
    spark.read
    .option("header", True)
    .schema(payment_schema)
    .csv(f"{RAW_PATH}/payments.csv")
)

payments = (
    payments
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("payment_type", lower(trim(col("payment_type"))))
)

payments = payments.dropDuplicates(
    ["order_id", "payment_sequential"]
)

payments = payments.filter(
    col("order_id").isNotNull()
)

print(f"Payments: {payments.count():,}")

save_parquet(payments, "payments")


# ============================================================
# 11. Final Validation
# ============================================================

print("\n" + "=" * 80)
print("DATA CLEANING COMPLETED SUCCESSFULLY")
print("=" * 80)

print("\nProcessed tables:")
print("  customers")
print("  products")
print("  sellers")
print("  orders")
print("  order_items")
print("  payments")

spark.stop()