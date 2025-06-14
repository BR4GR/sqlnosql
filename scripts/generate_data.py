import os
from faker import Faker
from dotenv import load_dotenv
import psycopg2
from pymongo import MongoClient
import random
from datetime import datetime, timedelta

load_dotenv()
fake = Faker()

def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def get_mongo_client():
    return MongoClient(f"mongodb://{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}")

def generate_products(count=1000):
    categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports']
    products = []
    
    for i in range(1, count + 1):
        products.append({
            'id': i,
            'name': fake.catch_phrase(),
            'category': random.choice(categories),
            'price': round(random.uniform(10, 500), 2),
            'description': fake.text(max_nb_chars=200)
        })
    
    return products

def generate_reviews(products, reviews_per_product=5):
    reviews = []
    review_id = 1
    
    for product in products:
        num_reviews = random.randint(1, reviews_per_product)
        for _ in range(num_reviews):
            reviews.append({
                'id': review_id,
                'product_id': product['id'],
                'user_name': fake.user_name(),
                'rating': random.randint(1, 5),
                'review_text': fake.text(max_nb_chars=300),
                'date': fake.date_time_between(start_date='-1y', end_date='now')
            })
            review_id += 1
    
    return reviews

def insert_postgres_data(products, reviews):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute(open('sql/schema.sql').read())
    
    for product in products:
        cur.execute("""
            INSERT INTO products (id, name, category, price, description)
            VALUES (%s, %s, %s, %s, %s)
        """, (product['id'], product['name'], product['category'], 
              product['price'], product['description']))
    
    for review in reviews:
        cur.execute("""
            INSERT INTO reviews (id, product_id, user_name, rating, review_text, date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (review['id'], review['product_id'], review['user_name'],
              review['rating'], review['review_text'], review['date']))
    
    conn.commit()
    cur.close()
    conn.close()

def insert_mongo_data(products, reviews):
    client = get_mongo_client()
    db = client[os.getenv('MONGO_DB')]
    
    db.products.drop()
    
    products_with_reviews = []
    for product in products:
        product_reviews = [r for r in reviews if r['product_id'] == product['id']]
        
        mongo_product = {
            '_id': product['id'],
            'name': product['name'],
            'category': product['category'],
            'price': product['price'],
            'description': product['description'],
            'reviews': product_reviews,
            'avg_rating': sum(r['rating'] for r in product_reviews) / len(product_reviews) if product_reviews else 0,
            'review_count': len(product_reviews)
        }
        products_with_reviews.append(mongo_product)
    
    db.products.insert_many(products_with_reviews)
    client.close()

if __name__ == "__main__":
    print("Generating data...")
    products = generate_products(10000)
    reviews = generate_reviews(products, 55)
    
    print(f"Generated {len(products)} products and {len(reviews)} reviews")
    
    print("Inserting into PostgreSQL...")
    insert_postgres_data(products, reviews)
    
    print("Inserting into MongoDB...")
    insert_mongo_data(products, reviews)
    
    print("Data generation complete!")