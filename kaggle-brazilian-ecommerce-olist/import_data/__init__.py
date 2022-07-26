import os
import csv
import psycopg2
import psycopg2.extras
from typing import Dict
from decimal import Decimal
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
    _import_products(conn=conn)
    _import_orders(conn=conn)
    _import_order_items(conn=conn)
    _import_order_payments(conn=conn)
    _import_order_reviews(conn=conn)
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


def _import_products(conn):
    def as_int_or_none(value: str):
        if value:
            return int(value)
        else:
            return None

    def get_category_name_translations() -> Dict:
        filepath = os.path.join(DATASET_DIR, "product_category_name_translation.csv")
        translations = {}
        with open(filepath) as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                translations[row[0]] = row[1]
        return translations

    print("[import_data] Importing products...")
    filepath = os.path.join(DATASET_DIR, "olist_products_dataset.csv")
    sql = "INSERT INTO products(id, category_name, name_length, description_length, photos_qty, weight_g, length_cm, height_cm, width_cm) VALUES %s;"
    data = []
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        category_name_translations = get_category_name_translations()
        for row in reader:
            data_row = (
                row["product_id"],
                category_name_translations.get(row["product_category_name"]),
                as_int_or_none(row["product_name_length"]),
                as_int_or_none(row["product_description_length"]),
                as_int_or_none(row["product_photos_qty"]),
                as_int_or_none(row["product_weight_g"]),
                as_int_or_none(row["product_length_cm"]),
                as_int_or_none(row["product_height_cm"]),
                as_int_or_none(row["product_width_cm"]),
            )
            data.append(data_row)
    cursor = conn.cursor()
    psycopg2.extras.execute_values(cursor, sql, data)
    conn.commit()
    cursor.close()
    print("[import_data] Importing products finished successfully!")


def _import_orders(conn):
    print("[import_data] Importing orders...")
    filepath = os.path.join(DATASET_DIR, "olist_orders_dataset.csv")
    sql = "INSERT INTO orders(id, customer_id, status, purchase_timestamp, approved_at, delivered_carrier_date, delivered_customer_date, estimated_delivery_date) VALUES %s;"
    data = []
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data_row = (
                row["order_id"],
                _as_str_or_none(row["customer_id"]),
                row["order_status"],
                _append_time_zone(row["order_purchase_timestamp"]),
                _with_appended_time_zone_or_none(row["order_approved_at"]),
                _with_appended_time_zone_or_none(row["order_delivered_carrier_date"]),
                _with_appended_time_zone_or_none(row["order_delivered_customer_date"]),
                _with_appended_time_zone_or_none(row["order_estimated_delivery_date"]),
            )
            data.append(data_row)
    cursor = conn.cursor()
    psycopg2.extras.execute_values(cursor, sql, data)
    conn.commit()
    cursor.close()
    print("[import_data] Importing orders finished successfully!")


def _import_order_items(conn):
    print("[import_data] Importing order items...")
    filepath = os.path.join(DATASET_DIR, "olist_order_items_dataset.csv")
    sql = "INSERT INTO order_items(order_id, order_item_id, product_id, seller_id, limit_date, price, freight_value) VALUES %s;"
    data = []
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data_row = (
                row["order_id"],
                row["order_item_id"],
                row["product_id"],
                row["seller_id"],
                _append_time_zone(row["shipping_limit_date"]),
                Decimal(row["price"]),
                Decimal(row["freight_value"]),
            )
            data.append(data_row)
    cursor = conn.cursor()
    psycopg2.extras.execute_values(cursor, sql, data)
    conn.commit()
    cursor.close()
    print("[import_data] Importing order items finished successfully!")


def _import_order_payments(conn):
    def get_brazilian_real_currency_code():
        return "BRL"

    print("[import_data] Importing order payments...")
    filepath = os.path.join(DATASET_DIR, "olist_order_payments_dataset.csv")
    sql = "INSERT INTO order_payments(order_id, sequential, type, installments, payment_value, currency_code) VALUES %s;"
    data = []
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data_row = (
                row["order_id"],
                row["payment_sequential"],
                row["payment_type"],
                row["payment_installments"],
                Decimal(row["payment_value"]),
                get_brazilian_real_currency_code(),
            )
            data.append(data_row)
    cursor = conn.cursor()
    psycopg2.extras.execute_values(cursor, sql, data)
    conn.commit()
    cursor.close()
    print("[import_data] Importing order payments finished successfully!")


def _import_order_reviews(conn):
    print("[import_data] Importing order reviews...")
    filepath = os.path.join(DATASET_DIR, "olist_order_reviews_dataset.csv")
    sql = "INSERT INTO order_reviews(id, order_id, score, comment_title, comment_message, creation_date, answer_timestamp) VALUES %s;"
    data = []
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data_row = (
                row["review_id"].strip('"'),
                row["order_id"].strip('"'),
                row["review_score"],
                row["review_comment_title"].strip('"'),
                row["review_comment_message"].strip('"'),
                _append_time_zone(row["review_creation_date"].strip('"')),
                _with_appended_time_zone_or_none(
                    row["review_answer_timestamp"].strip('"')
                ),
            )
            data.append(data_row)
    cursor = conn.cursor()
    psycopg2.extras.execute_values(cursor, sql, data)
    conn.commit()
    cursor.close()
    print("[import_data] Importing order items reviews successfully!")


def _as_str_or_none(value: str):
    if value:
        return value
    else:
        return None


def _append_time_zone(timestamp: str):
    brasilia_time_zone = "-03"
    return f"{timestamp}{brasilia_time_zone}"


def _with_appended_time_zone_or_none(timestamp: str):
    if _as_str_or_none(timestamp):
        return _append_time_zone(_as_str_or_none(timestamp))
    else:
        return None
