import pandas as pd

print("Reading dataset...")

file_path = r"E:\projects\ecommerce-data-platform\Brazilian E-Commerce Public Dataset by Olist.csv"

df = pd.read_csv(file_path)

print("Dataset loaded successfully!")
print(df.shape)
print(df.columns.tolist())
print(df.dtypes)
print(df.head(20))
pd.set_option("display.max_columns", None)


# print("\nMissing Values:")
# print(df.isnull().sum())

# print("\nDuplicate Rows:")
# print(df.duplicated().sum())

# print("\nNumber of items per order:")
# print(df.groupby("order_id")["order_item_id"].nunique().value_counts().sort_index())


# print("\nOrders per customer:")
# print(
#     df.groupby("customer_unique_id")["order_id"]
#       .nunique()
#       .describe()
# )


# print("Number of unique orders:")
# print(df["order_id"].nunique())

# print("\nNumber of rows:")
# print(len(df))

# print("\nOrders with multiple rows:")
# print(
#     df.groupby("order_id")
#       .size()
#       .value_counts()
#       .sort_index()
# )

# print("\nUnique order items:")
# print(
#     df.groupby("order_id")["order_item_id"]
#       .nunique()
#       .describe()
# )

# print("\nRows vs unique order items per order:")

# check_items = (
#     df.groupby("order_id")
#       .agg(
#           rows=("order_id", "size"),
#           unique_items=("order_item_id", "nunique")
#       )
# )

# print(check_items.head(20))
# print(
#     (check_items["rows"] == check_items["unique_items"]).value_counts()
# )

# false_orders = check_items[
#     check_items["rows"] != check_items["unique_items"]
# ]

# print(false_orders.head(20))


# payment_check = (
#     df.groupby(["order_id", "payment_sequential"])
#       .agg(
#           payment_type_count=("payment_type", "nunique"),
#           payment_value_count=("payment_value", "nunique"),
#           installments_count=("payment_installments", "nunique")
#       )
#       .reset_index()
# )

# print("\nPayment records:")
# print(payment_check.head(20))

# print("\nNumber of payment records:")
# print(len(payment_check))

# print("\nPayments per order:")
# print(
#     payment_check.groupby("order_id")
#                  .size()
#                  .value_counts()
#                  .sort_index()
# )




# print(
#     df[df["order_id"] == "0016dfedd97fc2950e388d2971d718c7"][
#         [
#             "order_id",
#             "order_item_id",
#             "payment_sequential",
#             "payment_type",
#             "payment_installments",
#             "payment_value"
#         ]
#     ]
# )

# order_id = "0016dfedd97fc2950e388d2971d718c7"
# pd.set_option("display.max_columns", None)
# print(
#     df[df["order_id"] == order_id][
#         [
#             "order_id",
#             "order_item_id",
#             "product_id",
#             "price",
#             "freight_value",
#             "payment_sequential",
#             "payment_type",
#             "payment_installments",
#             "payment_value"
#         ]
#     ]
# )

# إجمالي قيمة الـ Payments لكل Order
# payment_totals = (
#     df.groupby("order_id")["payment_value"]
#       .sum()
#       .reset_index(name="total_payment")
# )

# # إجمالي قيمة الـ Items + الشحن لكل Order
# item_totals = (
#     df.groupby("order_id")
#       .agg(
#           total_price=("price", "sum"),
#           total_freight=("freight_value", "sum")
#       )
#       .reset_index()
# )

# # نحسب إجمالي قيمة الـ Order Items
# item_totals["total_items_value"] = (
#     item_totals["total_price"]
#     + item_totals["total_freight"]
# )

# # ندمج النتيجتين
# comparison = payment_totals.merge(
#     item_totals,
#     on="order_id"
# )

# # الفرق بين الاثنين
# comparison["difference"] = (
#     comparison["total_payment"]
#     - comparison["total_items_value"]
# )

# print(comparison.head(20))

# # print("\nOrders where payment = items + freight:")

# # print(
# #     (comparison["difference"].abs() < 0.01).value_counts()
# # )

# payment_counts = (
#     df.groupby("order_id")["payment_sequential"]
#       .nunique()
# )

# multi_payment_orders = payment_counts[
#     payment_counts > 1
# ].index

# print("Number of orders with multiple payments:")
# print(len(multi_payment_orders))
# print("======================================================")

# check_multi_payment = (
#     df[df["order_id"].isin(multi_payment_orders)]
#     .groupby("order_id")
#     .agg(
#         rows=("order_id", "size"),
#         unique_items=("order_item_id", "nunique"),
#         payments=("payment_sequential", "nunique")
#     )
#     .reset_index()
# )
# pd.set_option("display.max_columns", None)


# print(check_multi_payment.head(20))

# print(
#     check_multi_payment[
#         check_multi_payment["unique_items"] > 1
#     ].head(20)
# )

# check_multi_payment["expected_rows"] = (
#     check_multi_payment["unique_items"]
#     * check_multi_payment["payments"]
# )

# print(
#     (
#         check_multi_payment["rows"]
#         == check_multi_payment["expected_rows"]
#     ).value_counts()
# )