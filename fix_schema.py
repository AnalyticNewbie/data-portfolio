import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def inspect_and_fix():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "nba"),
        user=os.getenv("DB_USER", "nba_user"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()
    
    # 1. Check columns in 'players' table
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'players'")
    columns = [row[0] for row in cur.fetchall()]
    print(f"Current columns in 'players' table: {columns}")

    # 2. Automatically determine the correct ID column
    if "player_id" in columns:
        print("✅ Column 'player_id' already exists.")
    elif "id" in columns:
        print("⚠️ Found column 'id'. Renaming it to 'player_id' for consistency...")
        cur.execute("ALTER TABLE players RENAME COLUMN id TO player_id;")
        conn.commit()
        print("✅ Rename successful.")
    elif "person_id" in columns:
        print("⚠️ Found column 'person_id'. Renaming it to 'player_id' for consistency...")
        cur.execute("ALTER TABLE players RENAME COLUMN person_id TO player_id;")
        conn.commit()
        print("✅ Rename successful.")
    else:
        print("❌ Could not find a recognizable ID column. Please check your table manually.")

    conn.close()

if __name__ == "__main__":
    inspect_and_fix()