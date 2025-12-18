import psycopg2
import json
import os
from datetime import datetime

def sync():
    conn = psycopg2.connect(os.getenv('DB_URL'))
    cur = conn.cursor()
    cur.execute("SELECT category, payload FROM daily_intelligence WHERE prediction_date = CURRENT_DATE ORDER BY id DESC")
    rows = cur.fetchall()

    site_data = {"last_updated": datetime.now().strftime("%Y-%m-%d"), "matchups": {}, "props": []}
    for category, payload in rows:
        if category == 'Team': site_data["matchups"] = payload
        if category == 'Player': site_data["props"] = payload

    with open("data.json", "w") as f:
        json.dump(site_data, f, indent=4)
    conn.close()

if __name__ == "__main__":
    sync()