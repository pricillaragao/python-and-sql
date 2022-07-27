import os
import json
import psycopg2
from dotenv import load_dotenv

JSON_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "json",
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
        _example_writing_to_json(conn=conn)
    finally:
        conn.close()


def _example_writing_to_json(conn):
    print("[example_writing_to_json] Start...")
    if not os.path.isdir(JSON_DIR):
        os.mkdir(JSON_DIR)
    _get_reviews_by_product(conn=conn)
    print("[example_writing_to_json] Finished successfully!")


def _get_reviews_by_product(conn):
    print("[example_writing_to_json] Get reviews by product starting...")
    data = {}
    cursor = conn.cursor()

    get_products_sql = """
            SELECT id, category_name 
            FROM products
            ORDER BY category_name;
        """

    get_reviews_by_product_sql = """
            SELECT r.score, r.comment_title, r.comment_message
            FROM orders o, order_reviews r
            WHERE o.id IN (
                SELECT oi.order_id
                FROM order_items oi
                WHERE oi.product_id = %s
            )
            AND o.id = r.order_id
            ORDER BY r.score;
        """

    cursor.execute(get_products_sql)
    get_products_result = cursor.fetchall()
    for row in get_products_result:
        product_id = row[0]
        cursor.execute(get_reviews_by_product_sql, (product_id,))
        get_reviews_by_product_result = cursor.fetchall()
        data[row[0]] = {
            "category_name": row[1],
            "reviews": [
                {
                    "score": review_row[0],
                    "comment_title": review_row[1],
                    "comment_message": review_row[2],
                }
                for review_row in get_reviews_by_product_result
            ],
        }

    cursor.close()

    jsonfile_path = os.path.join(JSON_DIR, "reviews_by_product.json")
    with open(jsonfile_path, mode="w") as jsonfile:
        json.dump(data, fp=jsonfile)

    print("[example_writing_to_json] Get reviews by product finished successfully!")
