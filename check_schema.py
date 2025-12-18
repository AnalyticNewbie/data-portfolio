import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

try:
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    
    # Get column names for 'teams'
    cur.execute("SELECT * FROM teams LIMIT 0")
    col_names = [desc[0] for desc in cur.description]
    
    print("--- Columns in 'teams' table ---")
    print(col_names)
    
    # Print a sample row to see what the data looks like
    cur.execute("SELECT * FROM teams LIMIT 1")
    row = cur.fetchone()
    print("\n--- Sample Row ---")
    print(row)
    
    conn.close()

except Exception as e:
    print(f"Error: {e}")