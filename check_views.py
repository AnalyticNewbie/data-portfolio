import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

def check_views():
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        
        # Check v_team_advanced_stats
        print("Checking v_team_advanced_stats...")
        cur.execute("SELECT * FROM v_team_advanced_stats LIMIT 1")
        row = cur.fetchone()
        if row:
            print("✅ Found! Sample:", row)
        else:
            print("⚠️ View exists but is empty.")

        # Check v_team_volatility
        print("\nChecking v_team_volatility...")
        cur.execute("SELECT * FROM v_team_volatility LIMIT 1")
        row = cur.fetchone()
        if row:
            print("✅ Found! Sample:", row)
        else:
            print("⚠️ View exists but is empty.")
            
        conn.close()

    except psycopg2.errors.UndefinedTable:
        print("❌ Error: One of the views does NOT exist.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_views()