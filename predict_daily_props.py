import sys
import os
import pandas as pd
import joblib
import psycopg2
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def generate_top_insights(all_projections):
    insights = []
    for p in all_projections:
        pts_diff = abs(p['proj_pts'] - p['avg_pts'])
        edge_pct = pts_diff / p['avg_pts'] if p['avg_pts'] > 0 else 0
        if p['avg_min'] > 18:  # Filter for players with solid minutes
            insights.append({
                "name": p['name'], "proj_pts": round(p['proj_pts'], 1),
                "proj_reb": round(p['proj_reb'], 1), "proj_ast": round(p['proj_ast'], 1),
                "away_abbr": p['away_abbr'], "home_abbr": p['home_abbr'],
                "edge_score": edge_pct, "edge_type": "High" if edge_pct > 0.18 else "Normal"
            })
    return sorted(insights, key=lambda x: x['edge_score'], reverse=True)[:5]

def main():
    conn = get_db_conn()
    # Simplified logic: In a real run, this would loop games. 
    # For now, it collects projections into 'all_projections'
    all_projections = []
    
    # ... (Your logic to fetch players and run models goes here) ...
    # Example of what your loop appends:
    # all_projections.append({"name": "Player X", "proj_pts": 25.0, "avg_pts": 20.0, ...})

    # GENERATE INSIGHTS
    top_5 = generate_top_insights(all_projections)

    json_path = "projects/nba-predictor/data.json"
    final_data = {"props": top_5, "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M")}

    if os.path.exists(json_path):
        with open(json_path, "r") as f:
            try:
                data = json.load(f)
                data.update(final_data)
                final_data = data
            except: pass

    with open(json_path, "w") as f:
        json.dump(final_data, f, indent=4)
    print(f"✅ Props Synced to {json_path}")

if __name__ == "__main__":
    main()