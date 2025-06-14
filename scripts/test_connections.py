import os
from dotenv import load_dotenv
import psycopg2
from pymongo import MongoClient

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    print("✅ PostgreSQL connected")
    conn.close()
except Exception as e:
    print(f"❌ PostgreSQL error: {e}")

try:
    client = MongoClient(f"mongodb://{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}")
    db = client[os.getenv('MONGO_DB')]
    print("✅ MongoDB connected")
    client.close()
except Exception as e:
    print(f"❌ MongoDB error: {e}")