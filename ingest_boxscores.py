import sys
import os
import time
import psycopg2
from psycopg2.extras import execute_values
from nba_api.stats.endpoints import boxscoreadvancedv3
import pandas as pd
from requests.exceptions import ReadTimeout, ConnectionError

# Config
DB_HOST = "localhost"
DB_NAME = "nba"
DB_USER = "nba_user"
DB_PASS = "nba_pass"

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_game_home_map(conn):
    sql = "SELECT game_id, home_team_id FROM games"
    with conn.cursor() as cur:
        cur.execute(sql)
        return {str(row[0]): int(row[1]) for row in cur.fetchall()}

def fetch_with_retry(game_id, retries=3):
    for attempt in range(retries):
        try:
            box = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id, timeout=15)
            return box.get_data_frames()
        except (ReadTimeout, ConnectionError) as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                return None
        except Exception as e:
            return None
    return None

def standardize_columns(df):
    df.columns = [c.upper() for c in df.columns]
    mappings = {
        'TEAMID': 'TEAM_ID',
        'OFFENSIVERATING': 'OFF_RATING',
        'DEFENSIVERATING': 'DEF_RATING',
        'OFF_RATING': 'OFF_RATING', 
        'DEF_RATING': 'DEF_RATING',
        'PACE': 'PACE',
        'OREB_PCT': 'OREB_PCT',
        'DREB_PCT': 'DREB_PCT'
    }
    new_cols = {}
    for col in df.columns:
        if col in mappings:
            new_cols[col] = mappings[col]
    return df.rename(columns=new_cols)

def main():
    print("\n--- INGESTING ADVANCED STATS (v4.1 Rebounding) ---\n")
    conn = get_db_conn()
    home_map = get_game_home_map(conn)
    
    print("Checking for games with missing Rebound Data...")
    sql_missing = """
        SELECT DISTINCT g.game_id 
        FROM games g
        LEFT JOIN team_game_stats tgs ON g.game_id = tgs.game_id
        WHERE (tgs.game_id IS NULL OR tgs.oreb_pct = 0)
          AND g.status = 'Final'
    """
    with conn.cursor() as cur:
        cur.execute(sql_missing)
        # FIX: Ensure uniqueness just in case
        missing_ids = list(set([row[0] for row in cur.fetchall()]))
        
    print(f"Found {len(missing_ids)} games needing update.")
    
    if not missing_ids:
        print("All caught up!")
        return

    total = len(missing_ids)
    batch_data = []
    
    for i, game_id in enumerate(missing_ids):
        print(f"[{i+1}/{total}] Fetching {game_id}...", end="\r")
        frames = fetch_with_retry(game_id)
        if not frames: continue
            
        target_df = None
        for df in frames:
            df = standardize_columns(df)
            if 'TEAM_ID' in df.columns:
                if len(df) <= 2: 
                    target_df = df
                    break
        
        if target_df is None:
            for df in frames:
                df = standardize_columns(df)
                if 'TEAM_ID' in df.columns:
                    target_df = df
                    break

        if target_df is None: continue
            
        try:
            # We use a set to track team_ids within this specific game loop
            # to prevent adding the same team twice for one game (rare API bug)
            seen_teams = set()
            
            for _, row in target_df.iterrows():
                current_team_id = int(row['TEAM_ID'])
                
                if current_team_id in seen_teams:
                    continue
                seen_teams.add(current_team_id)

                home_id_for_game = home_map.get(str(game_id))
                is_home = (current_team_id == home_id_for_game)
                
                pace = row.get('PACE', 0.0)
                if pd.isna(pace): pace = 0.0
                
                ortg = row.get('OFF_RATING', 0.0)
                if pd.isna(ortg): ortg = 0.0
                
                drtg = row.get('DEF_RATING', 0.0)
                if pd.isna(drtg): drtg = 0.0
                
                oreb = row.get('OREB_PCT', 0.0)
                if pd.isna(oreb): oreb = 0.0
                
                dreb = row.get('DREB_PCT', 0.0)
                if pd.isna(dreb): dreb = 0.0

                batch_data.append((
                    str(game_id),
                    current_team_id,
                    is_home,
                    float(pace),
                    float(ortg),
                    float(drtg),
                    float(oreb),
                    float(dreb)
                ))
        except Exception as e:
            continue

        time.sleep(0.6) 

        if len(batch_data) >= 20:
            insert_batch(conn, batch_data)
            batch_data = []

    if batch_data:
        insert_batch(conn, batch_data)

    print(f"\nDone! Stats updated.")
    conn.close()

def insert_batch(conn, data):
    sql = """
        INSERT INTO team_game_stats 
        (game_id, team_id, is_home, pace_est, ortg, drtg, oreb_pct, dreb_pct)
        VALUES %s
        ON CONFLICT (game_id, team_id) DO UPDATE SET
            is_home = EXCLUDED.is_home,
            pace_est = EXCLUDED.pace_est,
            ortg = EXCLUDED.ortg,
            drtg = EXCLUDED.drtg,
            oreb_pct = EXCLUDED.oreb_pct,
            dreb_pct = EXCLUDED.dreb_pct;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, data)
    conn.commit()

if __name__ == "__main__":
    main()