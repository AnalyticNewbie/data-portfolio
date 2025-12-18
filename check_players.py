import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

def check_players():
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        
        # 1. Get Column Names
        cur.execute("SELECT * FROM players LIMIT 0")
        col_names = [desc[0] for desc in cur.description]
        print(f"--- Schema: {col_names} ---")
        
        # 2. Get Total Count
        cur.execute("SELECT COUNT(*) FROM players")
        count = cur.fetchone()[0]
        print(f"\n✅ Total Players in Database: {count}")
        
        # 3. Show Sample Data
        if count > 0:
            print("\n--- First 5 Players ---")
            cur.execute("SELECT * FROM players LIMIT 5")
            rows = cur.fetchall()
            for row in rows:
                print(row)
        else:
            print("\n⚠️ The table is empty. Run fetch_players.py first.")
        
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_players()