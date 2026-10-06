from minio import Minio
from pathlib import Path

client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="admin12345",
    secure=False
)

bucket_name = "processed"

tables = [
    "order_items",
    "payments",
    "products",
    "sellers"
]

for table in tables:

    prefix = f"{table}/"
    output_dir = Path(f"snowflake_upload/{table}")
    output_dir.mkdir(parents=True, exist_ok=True)

    objects = client.list_objects(
        bucket_name,
        prefix=prefix,
        recursive=True
    )

    count = 0

    for obj in objects:

        if obj.object_name.endswith("_SUCCESS"):
            continue

        file_name = Path(obj.object_name).name
        output_path = output_dir / file_name

        print(f"Downloading: {obj.object_name}")

        client.fget_object(
            bucket_name,
            obj.object_name,
            str(output_path)
        )

        count += 1

    print(f"{table}: {count} files downloaded.\n")

print("ALL REMAINING TABLES DOWNLOADED.")