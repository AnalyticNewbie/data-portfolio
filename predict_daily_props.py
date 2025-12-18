import os
import sys
import pandas as pd
import psycopg2
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "nba"),
        user=os.getenv("DB_USER", "nba_user"),
        password=os.getenv("DB_PASSWORD")
    )

def predict_props(target_date_au):
    print(f"\n=== PLAYER PROP CHEAT SHEET (AU: {target_date_au}) ===")
    
    # Auto-convert AU Date to US Game Date
    target_date_dt = datetime.strptime(target_date_au, '%Y-%m-%d')
    us_game_date = (target_date_dt - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"US Game Date: {us_game_date}")

    conn = get_db_conn()
    
    # 1. FETCH ALL ACTIVE GAMES FOR THE DAY
    query_games = f"SELECT game_id, home_team_id, away_team_id FROM games WHERE game_date = '{us_game_date}'"
    df_games = pd.read_sql(query_games, conn)
    
    if df_games.empty:
        print(f"No games found for {us_game_date}. Pipeline complete.")
        return

    # 2. BULK FETCH ALL PLAYER LOGS (The Speed Fix)
    # We fetch everything once to avoid looping database queries
    print("Pre-loading historical data into memory...")
    query_logs = "SELECT player_id, pts, reb, ast, min, game_date FROM player_logs ORDER BY game_date DESC"
    all_logs = pd.read_sql(query_logs, conn)
    
    # 3. GET TEAM ABBREVIATIONS FOR DISPLAY
    teams_df = pd.read_sql("SELECT team_id, team_abbr FROM teams", conn)
    team_map = dict(zip(teams_df['team_id'], teams_df['team_abbr']))

    all_predictions = []

    for _, game in df_games.iterrows():
        home_abbr = team_map.get(game['home_team_id'], "UNK")
        away_abbr = team_map.get(game['away_team_id'], "UNK")
        print(f"\n>>> {away_abbr} @ {home_abbr} <<<")
        print(f"{'PLAYER':<25} | {'PTS':<6} | {'REB':<6} | {'AST':<6}")
        print("-" * 55)

        # Get all players for these two teams
        t_ids = (game['home_team_id'], game['away_team_id'])
        players_query = f"SELECT player_id, name, team_id FROM players WHERE team_id IN %s"
        players_df = pd.read_sql(players_query, conn, params=(t_ids,))

        for _, p in players_df.iterrows():
            # Filter logs in memory (Instant compared to SQL)
            p_logs = all_logs[all_logs['player_id'] == p['player_id']].head(20)
            
            if len(p_logs) < 3: continue # Skip bench players with no history

            # Simple weighted projection logic
            l5 = p_logs.head(5)
            pred_pts = round((l5['pts'].mean() * 0.7) + (p_logs['pts'].mean() * 0.3), 1)
            pred_reb = round((l5['reb'].mean() * 0.7) + (p_logs['reb'].mean() * 0.3), 1)
            pred_ast = round((l5['ast'].mean() * 0.7) + (p_logs['ast'].mean() * 0.3), 1)

            print(f"{p['name']:<25} | {pred_pts:<6} | {pred_reb:<6} | {pred_ast:<6}")
            
            all_predictions.append({
                "player": p['name'],
                "team": team_map.get(p['team_id']),
                "pts": pred_pts,
                "reb": pred_reb,
                "ast": pred_ast
            })

    # --- STANDARDIZED FE BRIDGE: data.json ---
    final_payload = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "target_date_au": target_date_au,
        "projections": all_predictions
    }

    with open("data.json", "w") as f:
        json.dump(final_payload, f, indent=4)
        
    print(f"\n✅ SUCCESS: Standardized data exported to data.json for Frontend.")

    conn.close()

if __name__ == "__main__":
    t_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    predict_props(t_date)