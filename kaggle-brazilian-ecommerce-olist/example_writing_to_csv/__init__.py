import os
import csv
import psycopg2
from dotenv import load_dotenv


CSV_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "csv",
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
        _example_writing_to_csv(conn=conn)
    finally:
        conn.close()


def _example_writing_to_csv(conn):
    print("[example_writing_to_csv] Start...")
    if not os.path.isdir(CSV_DIR):
        os.mkdir(CSV_DIR)
    _product_category_average_dimensions(conn=conn)
    _number_of_orders_by_days_of_week(conn=conn)
    print("[example_writing_to_csv] Finished successfully!")


def _product_category_average_dimensions(conn):
    print("[example_writing_to_csv] Product category average dimensions starting...")
    sql = """
        SELECT category_name, AVG(length_cm) as avg_length_cm, AVG(height_cm) as avg_height_cm, AVG(width_cm) as avg_width_cm  
        FROM products
        WHERE category_name IS NOT NULL 
        GROUP BY category_name 
        ORDER by category_name;
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()

    csvfile_path = os.path.join(CSV_DIR, "product_category_average_dimensions.csv")
    with open(csvfile_path, mode="w") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "category_name",
                "avg_length_cm",
                "avg_height_cm",
                "avg_width_cm",
            ],
        )
        writer.writeheader()
        for row in result:
            row_data = {
                "category_name": row[0],
                "avg_length_cm": row[1],
                "avg_height_cm": row[2],
                "avg_width_cm": row[3],
            }
            writer.writerow(row_data)
    print(
        "[example_writing_to_csv] Product category average dimensions finished successfully!"
    )


def _number_of_orders_by_days_of_week(conn):
    def to_english_day_of_week(day_of_week: int) -> str:
        english_days_of_week = {
            1: "monday",
            2: "tuesday",
            3: "wednesday",
            4: "thursday",
            5: "friday",
            6: "saturday",
            7: "sunday",
        }
        return english_days_of_week[day_of_week]

    print("[example_writing_to_csv] Number of orders by days of week starting...")
    sql = """
        SELECT extract(isodow from purchase_timestamp) as day_of_week, COUNT(*) 
        from orders o 
        group by day_of_week 
        order by day_of_week; 
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()

    csvfile_path = os.path.join(CSV_DIR, "number_of_orders_by_day_of_week.csv")
    with open(csvfile_path, mode="w") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "day_of_week",
                "number_of_orders",
            ],
        )
        writer.writeheader()
        for row in result:
            row_data = {
                "day_of_week": to_english_day_of_week(row[0]),
                "number_of_orders": row[1],
            }
            writer.writerow(row_data)
    print(
        "[example_writing_to_csv] Number of orders by days of week finished successfully!"
    )
