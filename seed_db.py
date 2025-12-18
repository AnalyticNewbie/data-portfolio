import psycopg2
import os
import json
from datetime import datetime

def seed():
    db_url = os.getenv('DB_URL')
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    
    # Sample payload matching your FE requirements
    sample_payload = [
        {"name": "LeBron James", "away_abbr": "LAL", "home_abbr": "CLE", "proj_pts": 24.5, "proj_reb": 7.2, "proj_ast": 8.1, "edge_type": "High"}
    ]
    
    cur.execute("""
        INSERT INTO daily_intelligence (prediction_date, category, payload)
        VALUES (CURRENT_DATE, 'Player', %s)
    """, (json.dumps(sample_payload),))
    
    conn.commit()
    print("Database seeded with v2.3 test data!")
    conn.close()

seed()