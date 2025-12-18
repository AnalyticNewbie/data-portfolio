import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def force_add_column():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "nba"),
        user=os.getenv("DB_USER", "nba_user"),
        password=os.getenv("DB_PASSWORD")
    )
    conn.autocommit = True  # crucial for schema changes
    cur = conn.cursor()

    print("Checking schema...")
    try:
        # Try to add the column
        cur.execute("ALTER TABLE players ADD COLUMN team_id INT;")
        print("✅ SUCCESS: Column 'team_id' added.")
    except psycopg2.errors.DuplicateColumn:
        print("ℹ️ Column 'team_id' already exists.")
    except Exception as e:
        print(f"❌ Error: {e}")

    conn.close()

if __name__ == "__main__":
    force_add_column()