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
    _select_one_customer(conn, random_customer[0])
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
