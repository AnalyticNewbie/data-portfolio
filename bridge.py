# bridge.py
import json
import psycopg2
import os

def send_to_db(data_dict, category):
    # This uses an Environment Variable for security
    conn = psycopg2.connect(os.getenv('DB_URL'))
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO daily_intelligence (category, payload) VALUES (%s, %s)",
        (category, json.dumps(data_dict))
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Factual {category} data moved to database.")