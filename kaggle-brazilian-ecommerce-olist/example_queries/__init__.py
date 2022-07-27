import os
import random
import psycopg2
from typing import List, Tuple
from dotenv import load_dotenv


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
        _example_queries(conn=conn)
    finally:
        conn.close()


def _example_queries(conn):
    print("[example_queries] Starting...")
    customers = _select_all_customers(conn=conn)
    random_customer = random.choice(customers)
    _select_one_customer(conn=conn, customer_id=random_customer[0])
    _biggest_sellers_by_number_of_order_items(conn=conn)
    _biggest_customer_by_order_prices(conn=conn)
    print("[example_queries] Finished successfully!")


def _select_all_customers(conn) -> List[Tuple]:
    print("[example_queries] Select all customers starting...")
    sql = "SELECT id, unique_id, zip_code_prefix, city, state FROM customers;"
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    print(result)
    print("[example_queries] Select all customers finished successfully!")
    return result


def _select_one_customer(conn, customer_id: str):
    print("[example_queries] Select one customer starting...")
    sql = "SELECT id, unique_id, zip_code_prefix, city, state FROM customers WHERE id = %s;"
    cursor = conn.cursor()
    cursor.execute(sql, (customer_id,))
    result = cursor.fetchone()
    cursor.close()
    print(result)
    print("[example_queries] Select one customer finished successfully!")


def _biggest_sellers_by_number_of_order_items(conn):
    print("[example_queries] Biggest sellers by number of order items starting...")
    sql = """
        SELECT seller_id, COUNT(*) 
        FROM order_items
        GROUP BY seller_id
        ORDER BY COUNT DESC;
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    print(result)
    print(
        "[example_queries] Biggest sellers by number of order items finished successfully!"
    )


def _biggest_customer_by_order_prices(conn):
    print("[example_queries] Biggest customer by order prices starting...")
    sql = """
        SELECT o.customer_id, SUM(ot.price) as price_sum
        FROM orders o, order_items ot
        WHERE o.id = ot.order_id
        GROUP BY o.customer_id
        ORDER BY price_sum DESC
        LIMIT 1;
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()
    print(result)
    print("[example_queries] Biggest customer by order prices finished successfully!")
