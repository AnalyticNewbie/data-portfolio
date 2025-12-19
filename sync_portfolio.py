import psycopg2
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables (Local .env or GitHub Secrets)
load_dotenv()

def sync():
    # 1. Fetch DB_URL from environment
    db_url = os.getenv('DB_URL')
    
    # 2. Diagnostic Log
    if db_url:
        print(f"DEBUG: Connection string detected (Length: {len(db_url)})")
    else:
        print("CRITICAL: DB_URL environment variable is missing!")
        return

    print("Attempting to connect to PostgreSQL database...")
    
    try:
        # 3. Establish Connection
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # 4. Fetch the latest daily intelligence
        # This pulls both Team predictions and Player props from the cloud
        cur.execute("""
            SELECT category, payload 
            FROM daily_intelligence 
            WHERE prediction_date = (SELECT MAX(prediction_date) FROM daily_intelligence)
            ORDER BY id DESC
        """)
        rows = cur.fetchall()

        # 5. Build the Data Contract for v2.3 Frontend
        site_data = {
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "metrics": {
                "accuracy": "58.7%",
                "mae": "10.13"
            },
            "matchups": [],
            "props": []
        }

        # 6. Parse the DB payload into the JSON structure
        for category, payload in rows:
            if category == 'Team': 
                site_data["matchups"] = payload
            elif category == 'Player': 
                site_data["props"] = payload

        # 7. Write to the project-specific subdirectory
        # This ensures the Hub page at /projects/nba-predictor/ can see it
        json_path = "projects/nba-predictor/data.json"
        
        # Ensure directory exists (Safety check)
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        
        with open(json_path, "w") as f:
            json.dump(site_data, f, indent=4)
        
        print(f"✅ SUCCESS: Synced {len(rows)} categories to {json_path}")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ ERROR during database sync: {e}")
        raise e

if __name__ == "__main__":
    sync()