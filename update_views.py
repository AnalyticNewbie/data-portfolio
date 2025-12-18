import psycopg2

DB_HOST = "localhost"
DB_NAME = "nba"
DB_USER = "nba_user"
DB_PASS = "nba_pass"

def main():
    print("--- UPDATING DATABASE VIEWS (Adding Rebound Avgs) ---")
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    
    # We must DROP the old view first because we are changing the column structure
    sql_drop = "DROP VIEW IF EXISTS v_team_advanced_stats;"
    
    sql_create = """
    CREATE VIEW v_team_advanced_stats AS
    SELECT 
        tgs.game_id,
        tgs.team_id,
        tgs.pace_est,
        tgs.ortg,
        tgs.drtg,
        tgs.oreb_pct,
        tgs.dreb_pct,
        
        -- Rolling 10 Game Averages
        AVG(tgs.pace_est) OVER (PARTITION BY tgs.team_id ORDER BY g.game_date_et ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as rolling_pace_10,
        AVG(tgs.ortg) OVER (PARTITION BY tgs.team_id ORDER BY g.game_date_et ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as rolling_ortg_10,
        AVG(tgs.drtg) OVER (PARTITION BY tgs.team_id ORDER BY g.game_date_et ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as rolling_drtg_10,
        
        -- NEW: Rolling Rebounding Stats
        AVG(tgs.oreb_pct) OVER (PARTITION BY tgs.team_id ORDER BY g.game_date_et ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as rolling_oreb_10,
        AVG(tgs.dreb_pct) OVER (PARTITION BY tgs.team_id ORDER BY g.game_date_et ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as rolling_dreb_10

    FROM team_game_stats tgs
    JOIN games g ON tgs.game_id = g.game_id;
    """
    
    with conn.cursor() as cur:
        try:
            cur.execute(sql_drop)
            print("Old view dropped.")
            cur.execute(sql_create)
            conn.commit()
            print("Success! v_team_advanced_stats updated with rolling rebound stats.")
        except Exception as e:
            print(f"Error updating view: {e}")
            conn.rollback()
            
    conn.close()

if __name__ == "__main__":
    main()