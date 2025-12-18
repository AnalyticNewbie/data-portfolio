import psycopg2
import json
import os
from datetime import datetime

def sync():
    # 1. Initialize variable to avoid UnboundLocalError
    db_url = os.getenv('DB_URL')
    
    # 2. Diagnostic Log for GitHub Actions
    print(f"DEBUG: Connection string length is {len(db_url) if db_url else 0}")
    
    if not db_url:
        print("CRITICAL: DB_URL environment variable is empty! Check GitHub Secrets.")
        return

    print("Attempting to connect to database...")
    
    try:
        # 3. Establish Connection
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # 4. Fetch the latest intelligence for today
        # Ensure your SQL table name and columns match exactly
      
        cur.execute("""
            SELECT category, payload 
    FROM daily_intelligence 
    WHERE prediction_date = (SELECT MAX(prediction_date) FROM daily_intelligence)
    ORDER BY id DESC
        """)
        rows = cur.fetchall()

        # 5. Build the Data Contract for v2.3
        site_data = {
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "metrics": {
                "accuracy": "58.7%",
                "mae": "10.13"
            },
            "matchups": {},
            "props": []
        }

        for category, payload in rows:
            if category == 'Team': 
                site_data["matchups"] = payload
            elif category == 'Player': 
                # This fills the PTS/REB/AST cards on the hub
                site_data["props"] = payload

        # 6. Write to data.json (the frontend's data source)
        with open("data.json", "w") as f:
            json.dump(site_data, f, indent=4)
        
        print(f"SUCCESS: Synced {len(rows)} data categories to data.json.")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR during database sync: {e}")
        # Re-raise error to ensure GitHub Action shows a Red X if it fails
        raise e

if __name__ == "__main__":
    sync()