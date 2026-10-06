import pandas as pd
from pathlib import Path


# =========================================================
# 1. Paths
# =========================================================

SOURCE_FILE = "Brazilian E-Commerce Public Dataset by Olist.csv"

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)


# =========================================================
# 2. Read Source
# =========================================================

print("Reading source dataset...")

df = pd.read_csv(SOURCE_FILE)

print(f"Source rows: {len(df):,}")
print(f"Source columns: {len(df.columns)}")


# =========================================================
# 3. Remove technical index column
# =========================================================

df = df.drop(columns=["Unnamed: 0"], errors="ignore")


# =========================================================
# 4. CUSTOMER TABLE
# Grain: 1 row = 1 customer
# Business key: customer_id
# =========================================================

customer_columns = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
]

customers = (
    df[customer_columns]
    .drop_duplicates(subset=["customer_id"])
    .reset_index(drop=True)
)

customers.to_csv(
    OUTPUT_DIR / "customers.csv",
    index=False
)

print(f"customers.csv: {len(customers):,} rows")


# =========================================================
# 5. PRODUCT TABLE
# Grain: 1 row = 1 product
# Business key: product_id
# =========================================================

product_columns = [
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]

products = (
    df[product_columns]
    .drop_duplicates(subset=["product_id"])
    .reset_index(drop=True)
)

products.to_csv(
    OUTPUT_DIR / "products.csv",
    index=False
)

print(f"products.csv: {len(products):,} rows")


# =========================================================
# 6. SELLER TABLE
# Grain: 1 row = 1 seller
# Business key: seller_id
# =========================================================

seller_columns = [
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
]

sellers = (
    df[seller_columns]
    .drop_duplicates(subset=["seller_id"])
    .reset_index(drop=True)
)

sellers.to_csv(
    OUTPUT_DIR / "sellers.csv",
    index=False
)

print(f"sellers.csv: {len(sellers):,} rows")


# =========================================================
# 7. ORDER TABLE
# Grain: 1 row = 1 order
# Business key: order_id
# =========================================================

order_columns = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

orders = (
    df[order_columns]
    .drop_duplicates(subset=["order_id"])
    .reset_index(drop=True)
)

orders.to_csv(
    OUTPUT_DIR / "orders.csv",
    index=False
)

print(f"orders.csv: {len(orders):,} rows")


# =========================================================
# 8. ORDER ITEMS TABLE
# Grain: 1 row = 1 order item
# Business key: order_id + order_item_id
# =========================================================

order_item_columns = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "price",
    "freight_value",
    "shipping_limit_date",
]

order_items = (
    df[order_item_columns]
    .drop_duplicates(
        subset=["order_id", "order_item_id"]
    )
    .reset_index(drop=True)
)

order_items.to_csv(
    OUTPUT_DIR / "order_items.csv",
    index=False
)

print(f"order_items.csv: {len(order_items):,} rows")


# =========================================================
# 9. PAYMENT TABLE
# Grain: 1 row = 1 payment
# Business key: order_id + payment_sequential
# =========================================================

payment_columns = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]

payments = (
    df[payment_columns]
    .drop_duplicates(
        subset=["order_id", "payment_sequential"]
    )
    .reset_index(drop=True)
)

payments.to_csv(
    OUTPUT_DIR / "payments.csv",
    index=False
)

print(f"payments.csv: {len(payments):,} rows")


# =========================================================
# 10. Summary
# =========================================================

print("\n" + "=" * 50)
print("SOURCE SPLIT COMPLETED")
print("=" * 50)

print(f"Customers   : {len(customers):,}")
print(f"Products    : {len(products):,}")
print(f"Sellers     : {len(sellers):,}")
print(f"Orders      : {len(orders):,}")
print(f"Order Items : {len(order_items):,}")
print(f"Payments    : {len(payments):,}")

print("\nFiles created inside:")
print(OUTPUT_DIR.resolve())