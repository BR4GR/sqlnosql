import os
import time
import timeit
from dataclasses import dataclass
from typing import Any, Dict, List

import psutil
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


@dataclass
class MeasurementResult:
    operation: str
    database: str
    execution_time: float
    result_count: int
    query_complexity: int


class DatabaseMeasurer:
    def __init__(self):
        self.results = []

    def measure_operation(self, operation_name, database_type, query_complexity=1):
        def decorator(func):
            def wrapper(*args, **kwargs):
                process = psutil.Process()

                # Use timeit to measure execution time
                stmt = lambda: func(*args, **kwargs)  # Wrap the function call
                n = 10  # Number of times to execute the statement
                execution_time = timeit.timeit(stmt, number=n) / n  # Average time

                result = func(*args, **kwargs)  # Execute function once to get results
                result_count = len(result) if isinstance(result, list) else 1

                measurement = MeasurementResult(
                    operation=operation_name,
                    database=database_type,
                    execution_time=execution_time,
                    result_count=result_count,
                    query_complexity=query_complexity,
                )

                self.results.append(measurement)
                return result

            return wrapper

        return decorator


class PostgreSQLQueries:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    def get_product_with_reviews(self, product_id):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT p.*, r.user_name, r.rating, r.review_text, r.date
            FROM products p
            LEFT JOIN reviews r ON p.id = r.product_id
            WHERE p.id = %s
        """,
            (product_id,),
        )
        return cur.fetchall()

    def get_products_by_category(self, category):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM products WHERE category = %s", (category,))
        return cur.fetchall()

    def get_average_ratings(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT p.id, p.name, AVG(r.rating) as avg_rating, COUNT(r.id) as review_count
            FROM products p
            LEFT JOIN reviews r ON p.id = r.product_id
            GROUP BY p.id, p.name
            ORDER BY avg_rating DESC
        """
        )
        return cur.fetchall()

    def get_products_price_range(self, min_price, max_price):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT p.*, AVG(r.rating) as avg_rating
            FROM products p
            LEFT JOIN reviews r ON p.id = r.product_id
            WHERE p.price BETWEEN %s AND %s
            GROUP BY p.id
        """,
            (min_price, max_price),
        )
        return cur.fetchall()

    def get_products_with_high_ratings(self, min_rating):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT p.*
            FROM products p
            JOIN reviews r ON p.id = r.product_id
            GROUP BY p.id
            HAVING AVG(r.rating) >= %s
        """,
            (min_rating,),
        )
        return cur.fetchall()

    def get_products_with_keyword_reviews(self, keyword):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT p.*
            FROM products p
            WHERE EXISTS (
                SELECT 1
                FROM reviews r
                WHERE r.product_id = p.id
                AND r.review_text LIKE %s
            )
        """,
            ("%" + keyword + "%",),
        )
        return cur.fetchall()

    def close(self):
        self.conn.close()


class MongoDBQueries:
    def __init__(self):
        self.client = MongoClient(
            f"mongodb://{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}"
        )
        self.db = self.client[os.getenv("MONGO_DB")]

    def get_product_with_reviews(self, product_id):
        return list(self.db.products.find({"_id": product_id}))

    def get_products_by_category(self, category):
        return list(self.db.products.find({"category": category}))

    def get_average_ratings(self):
        return list(
            self.db.products.find(
                {}, {"_id": 1, "name": 1, "avg_rating": 1, "review_count": 1}
            ).sort("avg_rating", -1)
        )

    def get_products_price_range(self, min_price, max_price):
        return list(
            self.db.products.find(
                {"price": {"$gte": min_price, "$lte": max_price}},
                {"_id": 1, "name": 1, "price": 1, "avg_rating": 1},
            )
        )

    def get_products_with_high_ratings(self, min_rating):
        return list(self.db.products.find({"avg_rating": {"$gte": min_rating}}))

    def get_products_with_keyword_reviews(self, keyword):
        return list(
            self.db.products.find(
                {"reviews.review_text": {"$regex": keyword, "$options": "i"}}
            )
        )

    def close(self):
        self.client.close()


def run_measurements():
    measurer = DatabaseMeasurer()

    pg_queries = PostgreSQLQueries()
    mongo_queries = MongoDBQueries()

    test_cases = [
        (1, "Electronics", 50, 200),
        (500, "Books", 10, 100),
        (750, "Clothing", 20, 150),
    ]

    for product_id, category, min_price, max_price in test_cases:

        @measurer.measure_operation("get_product_with_reviews", "PostgreSQL", 2)
        def pg_product_reviews():
            return pg_queries.get_product_with_reviews(product_id)

        @measurer.measure_operation("get_product_with_reviews", "MongoDB", 1)
        def mongo_product_reviews():
            return mongo_queries.get_product_with_reviews(product_id)

        @measurer.measure_operation("get_products_by_category", "PostgreSQL", 1)
        def pg_category():
            return pg_queries.get_products_by_category(category)

        @measurer.measure_operation("get_products_by_category", "MongoDB", 1)
        def mongo_category():
            return mongo_queries.get_products_by_category(category)

        @measurer.measure_operation("get_products_price_range", "PostgreSQL", 2)
        def pg_price_range():
            return pg_queries.get_products_price_range(min_price, max_price)

        @measurer.measure_operation("get_products_price_range", "MongoDB", 1)
        def mongo_price_range():
            return mongo_queries.get_products_price_range(min_price, max_price)

        @measurer.measure_operation("get_products_with_high_ratings", "PostgreSQL", 2)
        def pg_high_ratings():
            return pg_queries.get_products_with_high_ratings(4.0)

        @measurer.measure_operation("get_products_with_high_ratings", "MongoDB", 1)
        def mongo_high_ratings():
            return mongo_queries.get_products_with_high_ratings(4.0)

        @measurer.measure_operation(
            "get_products_with_keyword_reviews", "PostgreSQL", 3
        )
        def pg_keyword_reviews():
            return pg_queries.get_products_with_keyword_reviews("great")

        @measurer.measure_operation("get_products_with_keyword_reviews", "MongoDB", 1)
        def mongo_keyword_reviews():
            return mongo_queries.get_products_with_keyword_reviews("great")

        pg_product_reviews()
        mongo_product_reviews()
        pg_category()
        mongo_category()
        pg_price_range()
        mongo_price_range()
        pg_high_ratings()
        mongo_high_ratings()
        pg_keyword_reviews()
        mongo_keyword_reviews()

    @measurer.measure_operation("get_average_ratings", "PostgreSQL", 3)
    def pg_avg_ratings():
        return pg_queries.get_average_ratings()

    @measurer.measure_operation("get_average_ratings", "MongoDB", 1)
    def mongo_avg_ratings():
        return mongo_queries.get_average_ratings()

    pg_avg_ratings()
    mongo_avg_ratings()

    pg_queries.close()
    mongo_queries.close()

    return measurer.results


if __name__ == "__main__":
    print("Running measurements...")
    results = run_measurements()

    print(f"\nCompleted {len(results)} measurements")
    for result in results:
        print(
            f"{result.database:10} | {result.operation:25} | "
            f"{result.execution_time:.4f}s | {result.result_count:4} results"
        )
