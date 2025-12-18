import psycopg2
import json
import os
from datetime import datetime

def sync():
    # 1. Get the Secret
    db_url = os.getenv('DB_URL')
    if not db_url:
        raise ValueError("DB_URL environment variable is empty! Check GitHub Secrets.")
    
    print("Attempting to connect to database...")
    
    try:
        # 2. Connect once using the variable
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # 3. Fetch the latest intelligence
        cur.execute("""
            SELECT category, payload 
            FROM daily_intelligence 
            WHERE prediction_date = CURRENT_DATE 
            ORDER BY id DESC
        """)
        rows = cur.fetchall()

        # 4. Prepare the Data Contract for v2.3
        site_data = {
            "last_updated": datetime.now().strftime("%Y-%m-%d"), 
            "metrics": {
                "accuracy": "58.7%", # This matches your v2.3 baseline
                "mae": "10.13"
            },
            "matchups": {}, 
            "props": []
        }

        for category, payload in rows:
            if category == 'Team': 
                site_data["matchups"] = payload
            if category == 'Player': 
                site_data["props"] = payload

        # 5. Write the file that the Hub page reads
        with open("data.json", "w") as f:
            json.dump(site_data, f, indent=4)
        
        print(f"Successfully synced {len(rows)} categories to data.json")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error during sync: {e}")
        raise e

if __name__ == "__main__":
    sync()