# db_config.py
import os
import psycopg2 # If using PostgreSQL. Use 'mysql.connector' if MySQL.
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# 1. Load the variables from .env into Python's memory
load_dotenv()

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        # This allows us to access columns by name (row['performance_score'])
        # instead of index (row[5]), matching the logic we wrote earlier.
        return conn
    except Exception as e:
        print(f"ERROR: Could not connect to database. Check your .env file.\n{e}")
        return None

def get_cursor(conn):
    # Returns a cursor that acts like a dictionary
    return conn.cursor(cursor_factory=RealDictCursor)