import sys
import os
import pandas as pd
import joblib
import psycopg2
import json
from datetime import datetime
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_prediction_data(player_id, player_name):
    """
    Placeholder: Replace this with your actual model prediction logic.
    It should return a dictionary with proj_pts, proj_reb, proj_ast, and avg_pts.
    """
    # This is where you'd normally load your .joblib models 
    # and fetch player_logs for features.
    return None 

def generate_top_insights(all_projections):
    insights = []
    for p in all_projections:
        pts_diff = abs(p['proj_pts'] - p['avg_pts'])
        edge_pct = pts_diff / p['avg_pts'] if p['avg_pts'] > 0 else 0
        
        # metric check (e.g. 180cm height check or other metrics if relevant)
        if p.get('avg_min', 0) > 18:  
            insights.append({
                "name": p['name'], 
                "proj_pts": round(p['proj_pts'], 1),
                "proj_reb": round(p['proj_reb'], 1), 
                "proj_ast": round(p['proj_ast'], 1),
                "away_abbr": p.get('away_abbr', 'N/A'), 
                "home_abbr": p.get('home_abbr', 'N/A'),
                "edge_score": edge_pct, 
                "edge_type": "High" if edge_pct > 0.18 else "Normal"
            })
    return sorted(insights, key=lambda x: x['edge_score'], reverse=True)[:5]

def main():
    conn = get_db_conn()
    all_projections = []
    
    # 1. Get today's games (Targeting ET date)
    target_date = datetime.now().strftime("%Y-%m-%d")
    query_games = "SELECT home_team_id, away_team_id, game_id FROM games WHERE game_date_et = %s"
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query_games, (target_date,))
        todays_games = cur.fetchall()

    if not todays_games:
        print(f"No games found for {target_date}. Check your schedule table.")
        return

    print(f"Found {len(todays_games)} games for today. Processing rosters...")

    # 2. Loop through every game
    for game in todays_games:
        team_ids = (game['home_team_id'], game['away_team_id'])
        query_players = "SELECT player_id, name, team_id FROM players WHERE team_id IN %s"
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query_players, (team_ids,))
            players = cur.fetchall()

        # 3. Run the model for every player found
        for p in players:
            try:
                # Logic Fix: Properly aligned try/except
                result = get_prediction_data(p['player_id'], p['name'])
                if result:
                    # Add team context for the UI
                    result['home_abbr'] = str(game['home_team_id']) # Replace with actual ABBR if available
                    result['away_abbr'] = str(game['away_team_id'])
                    all_projections.append(result)
            except Exception as e:
                print(f"Error predicting for {p['name']}: {e}")
                continue 

    # 4. GENERATE THE TOP 5 FROM THE FULL LIST
    top_5 = generate_top_insights(all_projections)

    # 5. Save to project subdirectory
    json_path = "projects/nba-predictor/data.json"
    final_data = {"props": top_5, "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M")}

    if os.path.exists(json_path):
        with open(json_path, "r") as f:
            try:
                data = json.load(f)
                data.update(final_data)
                final_data = data
            except: 
                pass

    with open(json_path, "w") as f:
        json.dump(final_data, f, indent=4)
        
    print(f"✅ Props Synced to {json_path}")
    conn.close()

if __name__ == "__main__":
    main()