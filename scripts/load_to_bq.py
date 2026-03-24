import os 
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# Config 
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = os.environ["GCP_DATASET_ID"]
LOCATION = os.environ["GCP_LOCATION"]
DATA_DIR = Path(__file__).parent.parent / "data"

TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments":"olist_order_payments_dataset.csv",
    "order_reviews":"olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products":"olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation":"olist_geolocation_dataset.csv",
    "product_category_name_translation":"product_category_name_translation.csv"
}

def load_to_bigquery(data_dir : Path) -> None:
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    client.create_dataset(dataset_ref, exists_ok=True)
    print(f"Dataset {DATASET_ID} ready.")

    for table_name, filename in TABLES.items():
        filepath = data_dir / filename
        if not filepath.exists():
            print(f"WARNING: {filename} not found, skipping.")
            continue

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition="WRITE_TRUNCATE",
            allow_quoted_newlines=True if table_name == "order_reviews" else False
    )

        with open(filepath, "rb") as f:
            job = client.load_table_from_file(
                f,
                f"{PROJECT_ID}.{DATASET_ID}.{table_name}",
                job_config=job_config
            )
            job.result()
            print(f"Loaded {table_name}")


if __name__ == "__main__":
    load_to_bigquery(DATA_DIR)