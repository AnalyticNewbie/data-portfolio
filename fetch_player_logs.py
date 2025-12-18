import os
import sys
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from nba_api.stats.endpoints import leaguegamelog

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

# Config
SEASONS_TO_TRACK = ['2023-24', '2024-25', '2025-26'] 

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_latest_log_date():
    """Finds the latest game date currently in the DB."""
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(game_date) FROM player_logs")
        latest = cur.fetchone()[0]
    except psycopg2.errors.UndefinedTable:
        # Table doesn't exist yet
        latest = None
    except Exception:
        latest = None
    finally:
        conn.close()
    return latest

def create_logs_table_if_not_exists():
    conn = get_db_conn()
    cur = conn.cursor()
    # Same schema as before
    sql = """
        CREATE TABLE IF NOT EXISTS player_logs (
            game_id TEXT,
            player_id INT,
            game_date DATE,
            matchup TEXT,
            wl TEXT,
            min FLOAT,
            pts INT,
            reb INT,
            ast INT,
            stl INT,
            blk INT,
            tov INT,
            fgm INT,
            fga INT,
            fg3m INT,
            fg3a INT,
            ftm INT,
            fta INT,
            plus_minus INT,
            PRIMARY KEY (game_id, player_id)
        );
    """
    cur.execute(sql)
    conn.commit()
    conn.close()

def parse_minutes(min_val):
    """Handles minutes formats like '24:15' or 24.25"""
    if not min_val:
        return 0.0
    min_str = str(min_val)
    if ':' in min_str:
        try:
            m, s = min_str.split(':')
            return float(m) + float(s)/60
        except:
            return 0.0
    try:
        return float(min_str)
    except:
        return 0.0

def upsert_logs(df):
    if df.empty:
        return 0
        
    conn = get_db_conn()
    cur = conn.cursor()
    
    sql = """
        INSERT INTO player_logs 
        (game_id, player_id, game_date, matchup, wl, min, pts, reb, ast, stl, blk, tov, fgm, fga, fg3m, fg3a, ftm, fta, plus_minus)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id, player_id) DO UPDATE SET
            pts = EXCLUDED.pts,
            min = EXCLUDED.min,
            reb = EXCLUDED.reb,
            ast = EXCLUDED.ast; 
    """
    
    count = 0
    # Process in batch transaction
    for _, row in df.iterrows():
        try:
            cur.execute(sql, (
                row['GAME_ID'],
                row['PLAYER_ID'],
                row['GAME_DATE'],
                row['MATCHUP'],
                row['WL'],
                parse_minutes(row['MIN']),
                row['PTS'],
                row['REB'],
                row['AST'],
                row['STL'],
                row['BLK'],
                row['TOV'],
                row['FGM'],
                row['FGA'],
                row['FG3M'],
                row['FG3A'],
                row['FTM'],
                row['FTA'],
                row['PLUS_MINUS']
            ))
            count += 1
        except Exception as e:
            print(f"Skipping row error: {e}")
            
    conn.commit()
    conn.close()
    return count

def fetch_delta():
    print("--- Smart Player Log Scraper ---")
    create_logs_table_if_not_exists()
    
    # 1. Determine Date Strategy
    latest_date = get_latest_log_date()
    
    if latest_date:
        start_date_obj = latest_date + timedelta(days=1)
        date_from = start_date_obj.strftime("%m/%d/%Y")
        print(f"Latest DB Date: {latest_date}. Fetching delta from {date_from}...")
        
        # FIXED: Don't hardcode '2024-25'. Use the global config.
        # This ensures we check 2025-26 even if we only have 2024-25 data so far.
        target_seasons = SEASONS_TO_TRACK 
    else:
        print("Table empty (or force refresh). Fetching FULL history...")
        date_from = None 
        target_seasons = SEASONS_TO_TRACK

    total_added = 0

    # 2. Fetch League-Wide Logs
    for season in target_seasons:
        print(f"Fetching {season}...", end=" ")
        try:
            logs = leaguegamelog.LeagueGameLog(
                player_or_team_abbreviation='P', 
                season=season, 
                date_from_nullable=date_from
            )
            df = logs.get_data_frames()[0]
            
            if not df.empty:
                print(f"Got {len(df)} rows. Inserting...")
                added = upsert_logs(df)
                total_added += added
                print(f"Inserted {added} rows.")
            else:
                print("No new games found.")
                
        except Exception as e:
            print(f"Error fetching {season}: {e}")

    print("-" * 30)
    print(f"SUCCESS: Process finished. Added {total_added} new log entries.")

if __name__ == "__main__":
    fetch_delta()