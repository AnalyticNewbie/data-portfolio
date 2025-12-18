import os
import sys
import time
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from nba_api.stats.endpoints import commonteamroster

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_team_ids():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT team_id, team_abbr FROM teams")
    teams = cur.fetchall()
    conn.close()
    return teams

def upsert_players(players_data, team_id):
    if not players_data:
        return 0
    conn = get_db_conn()
    cur = conn.cursor()
    
    # Force update the team_id
    sql = """
        INSERT INTO players (id, name, position, current_status, team_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET 
            name = EXCLUDED.name,
            position = EXCLUDED.position,
            current_status = EXCLUDED.current_status,
            team_id = EXCLUDED.team_id; 
    """
    
    count = 0
    for p in players_data:
        try:
            cur.execute(sql, (
                p['PLAYER_ID'], 
                p['PLAYER'], 
                p['POSITION'], 
                'Active',
                team_id 
            ))
            count += 1
        except Exception as e:
            pass
            
    conn.commit()
    conn.close()
    return count

def main():
    print("--- Fetching Active Players & Teams ---")
    teams = get_team_ids()
    print(f"Found {len(teams)} teams. Updating rosters...")
    
    total = 0
    for team_id, team_abbr in teams:
        try:
            # Fetch official roster for 2025-26
            roster = commonteamroster.CommonTeamRoster(team_id=team_id, season='2025-26')
            df = roster.get_data_frames()[0]
            
            # Insert into DB with the TEAM ID
            inserted = upsert_players(df.to_dict('records'), team_id)
            print(f"  {team_abbr}: Updated {inserted} players.")
            total += inserted
            
            time.sleep(0.5) # Avoid API rate limits
        except Exception as e:
            print(f"  Error fetching {team_abbr}: {e}")

    print(f"\nSUCCESS: Updated {total} players with Team IDs.")

if __name__ == "__main__":
    main()