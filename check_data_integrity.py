import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    dbname=os.getenv("DB_NAME", "nba"),
    user=os.getenv("DB_USER", "nba_user"),
    password=os.getenv("DB_PASSWORD")
)
cur = conn.cursor()

# 1. Check for missing Team IDs
cur.execute("SELECT COUNT(*) FROM players WHERE team_id IS NULL")
missing_teams = cur.fetchone()[0]

# 2. Check for missing Logs (2025-26 Season)
cur.execute("SELECT COUNT(*) FROM player_logs WHERE game_date > '2025-10-01'")
recent_logs = cur.fetchone()[0]

print(f"Players with NO Team ID: {missing_teams}")
print(f"Game Logs for current season: {recent_logs}")

conn.close()