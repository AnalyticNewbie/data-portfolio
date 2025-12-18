import sys
import os
import psycopg2
from psycopg2.extras import execute_values
from nba_api.stats.endpoints import leaguegamelog
import pandas as pd
from datetime import datetime

# Config
DB_HOST = os.getenv("PGHOST", "localhost")
DB_NAME = os.getenv("PGDATABASE", "nba")
DB_USER = os.getenv("PGUSER", "nba_user")
DB_PASS = os.getenv("PGPASSWORD", "nba_pass")

def get_db_conn():
    if not DB_PASS:
        print("Error: PGPASSWORD not set.")
        sys.exit(1)
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def main():
    print("\n--- INITIALIZING FULL SEASON HISTORY (2025-26) ---\n")
    conn = get_db_conn()
    
    print("Fetching game logs from NBA API...")
    try:
        log = leaguegamelog.LeagueGameLog(season='2025-26', player_or_team_abbreviation='T')
        df = log.get_data_frames()[0]
    except Exception as e:
        print(f"Error fetching data from NBA API: {e}")
        return
    
    unique_games = df['GAME_ID'].unique()
    print(f"Found {len(unique_games)} completed games.")
    
    games_to_insert = []
    
    for game_id in unique_games:
        rows = df[df['GAME_ID'] == game_id]
        if len(rows) != 2:
            continue
            
        r0 = rows.iloc[0]
        if 'vs.' in r0['MATCHUP']:
            home = r0
            away = rows.iloc[1]
        else:
            away = r0
            home = rows.iloc[1]
            
        game_date = home['GAME_DATE']
        
        # FIX: Added 'Final' for the status column
        games_to_insert.append((
            str(game_id),
            game_date,            # game_date_et
            game_date,            # game_date
            int(home['TEAM_ID']),
            int(away['TEAM_ID']),
            int(home['PTS']),
            int(away['PTS']),
            'Final'               # status
        ))
        
    print(f"Prepared {len(games_to_insert)} games for database insertion...")

    # FIX: Updated SQL to include 'status'
    sql = """
        INSERT INTO games 
        (game_id, game_date_et, game_date, home_team_id, away_team_id, home_pts, away_pts, status)
        VALUES %s
        ON CONFLICT (game_id) DO UPDATE SET
            home_pts = EXCLUDED.home_pts,
            away_pts = EXCLUDED.away_pts,
            status = EXCLUDED.status;
    """
    
    with conn.cursor() as cur:
        execute_values(cur, sql, games_to_insert)
        
    conn.commit()
    conn.close()
    print("Success! Full season history loaded.")

if __name__ == "__main__":
    main()