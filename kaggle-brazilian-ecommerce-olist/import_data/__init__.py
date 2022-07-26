import os
import csv
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from psycopg2.extensions import adapt, register_adapter, AsIs
from shapely.geometry import Point

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
    print("[import_data] Start...")
    _import_geolocation(conn=conn)
    _import_customers(conn=conn)
    _import_sellers(conn=conn)
    print("[import_data] Finished successfully!")


def _import_geolocation(conn):
    def adapt_point(point: Point):
        def quote(v):
            return adapt(v).getquoted().decode()

        x, y = quote(point.x), quote(point.y)
        return AsIs("'(%s, %s)'" % (x, y))

    print("[import_data] Importing geolocation...")
    register_adapter(Point, adapt_point)
    filepath = os.path.join(DATASET_DIR, "olist_geolocation_dataset.csv")
    sql = (
        "INSERT INTO geolocation(zip_code_prefix, coordinates, city, state) VALUES %s;"
    )
    data = []
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            coordinate = Point(
                float(row["geolocation_lat"]), float(row["geolocation_lng"])
            )
            data_row = (
                row["geolocation_zip_code_prefix"],
                coordinate,
                row["geolocation_city"],
                row["geolocation_state"],
            )
            data.append(data_row)
    cursor = conn.cursor()
    psycopg2.extras.execute_values(cursor, sql, data)
    conn.commit()
    cursor.close()
    print("[import_data] Importing geolocation finished successfully!")


def _import_customers(conn):
    print("[import_data] Importing customers...")
    filepath = os.path.join(DATASET_DIR, "olist_customers_dataset.csv")
    sql = (
        "INSERT INTO customers(id, unique_id, zip_code_prefix, city, state) VALUES %s;"
    )
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
    print("[import_data] Importing customers finished successfully!")


def _import_sellers(conn):
    print("[import_data] Importing sellers...")
    filepath = os.path.join(DATASET_DIR, "olist_sellers_dataset.csv")
    sql = "INSERT INTO sellers(id, zip_code_prefix, city, state) VALUES %s;"
    data = []
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data_row = (
                row["seller_id"],
                row["seller_zip_code_prefix"],
                row["seller_city"],
                row["seller_state"],
            )
            data.append(data_row)
    cursor = conn.cursor()
    psycopg2.extras.execute_values(cursor, sql, data)
    conn.commit()
    cursor.close()
    print("[import_data] Importing sellers finished successfully!")
