import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def debug_players():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "nba"),
        user=os.getenv("DB_USER", "nba_user"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()

    print("--- Database Debug ---")
    
    # 1. Count Total Players
    cur.execute("SELECT COUNT(*) FROM players")
    total = cur.fetchone()[0]
    print(f"Total Players: {total}")

    # 2. Count Players with Missing Team ID
    cur.execute("SELECT COUNT(*) FROM players WHERE team_id IS NULL")
    missing = cur.fetchone()[0]
    print(f"Players with NULL team_id: {missing} (Should be 0)")

    # 3. Check a specific player (e.g., Donovan Mitchell for CLE)
    print("\nChecking sample player (Donovan Mitchell):")
    cur.execute("SELECT id, name, team_id, current_status FROM players WHERE name ILIKE '%Mitchell%' LIMIT 1")
    row = cur.fetchone()
    if row:
        print(f"  ID: {row[0]}")
        print(f"  Name: {row[1]}")
        print(f"  Team ID: {row[2]}  <-- If this is None, that is the problem.")
    else:
        print("  Player not found.")

    conn.close()

if __name__ == "__main__":
    debug_players()