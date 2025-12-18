import psycopg2
import os

def check():
    db_url = os.getenv('DB_URL')
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    
    # 1. Check if the table exists
    cur.execute("SELECT to_regclass('daily_intelligence');")
    print(f"Table exists: {cur.fetchone()[0]}")
    
    # 2. Check how many rows are in the table
    cur.execute("SELECT COUNT(*) FROM daily_intelligence;")
    print(f"Total rows in table: {cur.fetchone()[0]}")
    
    # 3. See the most recent dates stored
    cur.execute("SELECT DISTINCT prediction_date FROM daily_intelligence ORDER BY prediction_date DESC LIMIT 5;")
    print(f"Latest dates in DB: {cur.fetchall()}")
    
    conn.close()

if __name__ == "__main__":
    check()