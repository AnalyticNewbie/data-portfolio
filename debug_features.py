import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Config
DB_HOST = os.getenv("PGHOST", "localhost")
DB_NAME = os.getenv("PGDATABASE", "nba")
DB_USER = os.getenv("PGUSER", "nba_user")
DB_PASS = os.getenv("PGPASSWORD")

def get_db_conn():
    if not DB_PASS:
        print("Error: PGPASSWORD environment variable not set.")
        sys.exit(1)
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def main():
    print("\n--- DEBUGGING FEATURE INPUTS ---\n")
    conn = get_db_conn()
    
    # Let's look at a specific game from your backtest (BOS vs DET on 2025-12-15)
    # If this returns 0.0 for "rolling_pd_10", our SQL view is broken.
    sql = """
        SELECT 
            g.game_date_et,
            ht.team_abbr as home,
            at.team_abbr as away,
            hf.rolling_pd_10 as home_form_10,
            af.rolling_pd_10 as away_form_10,
            hf.rest_days as home_rest,
            hsos.rolling_sos_10 as home_sos
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        LEFT JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
        LEFT JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
        LEFT JOIN v_strength_of_schedule hsos ON g.home_team_id = hsos.team_id AND g.game_id = hsos.game_id
        WHERE g.game_date_et = '2025-12-15'
        LIMIT 5;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        
    if not rows:
        print("No games found for 2025-12-15 to debug.")
        return

    print(f"{'MATCHUP':<12} | {'HOME FORM':<10} | {'AWAY FORM':<10} | {'REST':<5} | {'SOS':<5}")
    print("-" * 60)
    for r in rows:
        h_form = r['home_form_10'] if r['home_form_10'] is not None else "NULL"
        a_form = r['away_form_10'] if r['away_form_10'] is not None else "NULL"
        print(f"{r['home']} vs {r['away']:<5} | {str(h_form):<10} | {str(a_form):<10} | {str(r['home_rest']):<5} | {str(r['home_sos']):<5}")

if __name__ == "__main__":
    main()