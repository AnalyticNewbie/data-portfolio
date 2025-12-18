import json
import psycopg2
import os

def log_prediction_to_db(data_dict, category):
    try:
        conn = psycopg2.connect(os.getenv('DB_URL'))
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO daily_intelligence (category, payload) VALUES (%s, %s)",
            (category, json.dumps(data_dict))
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"Successfully bridged {category} data to the database.")
    except Exception as e:
        print(f"Bridge Failed: {e}")