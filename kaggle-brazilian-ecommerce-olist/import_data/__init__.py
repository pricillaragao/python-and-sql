import os
import csv
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

DATASET_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.path.pardir,
    "kaggle-brazilian-ecommerce-olist-dataset",
)


def main():
    load_dotenv()

    db_name = os.environ.get("DB_NAME")
    if db_name is None:
        raise EnvironmentError("Environment variable DB_NAME must be set")

    db_user = os.environ.get("DB_USER")
    if db_user is None:
        raise EnvironmentError("Environment variable DB_USER must be set")

    db_password = os.environ.get("DB_PASSWORD")
    if db_password is None:
        raise EnvironmentError("Environment variable DB_PASSWORD must be set")

    db_host = os.environ.get("DB_HOST")
    if db_host is None:
        raise EnvironmentError("Environment variable DB_HOST must be set")

    db_port = os.environ.get("DB_PORT")
    if db_port is None:
        raise EnvironmentError("Environment variable DB_PORT must be set")

    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )

    try:
        _import_data(conn=conn)
    finally:
        conn.close()


def _import_data(conn):
    print("Importing data start...")
    _import_customers(conn=conn)
    print("Importing data finished successfully!")


def _import_customers(conn):
    print("Importing customers...")
    filepath = os.path.join(DATASET_DIR, "olist_customers_dataset.csv")
    sql = "INSERT INTO customers(customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) VALUES %s;"
    data = []
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data_row = (
                row["customer_id"],
                row["customer_unique_id"],
                row["customer_zip_code_prefix"],
                row["customer_city"],
                row["customer_state"],
            )
            data.append(data_row)
    cursor = conn.cursor()
    psycopg2.extras.execute_values(cursor, sql, data)
    conn.commit()
    cursor.close()
    print("Importing customers finished successfully!")
