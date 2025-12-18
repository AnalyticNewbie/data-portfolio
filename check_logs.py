import os, psycopg2
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"), dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM player_logs")
print(f"Total Player Game Logs: {cur.fetchone()[0]}")
conn.close()