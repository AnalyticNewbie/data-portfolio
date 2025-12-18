import sys
import os
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt

# Config
DB_HOST = "localhost"
DB_NAME = "nba"
DB_USER = "nba_user"
DB_PASS = "nba_pass"

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def main():
    print("\n--- INVESTIGATING DAY-OF-WEEK TRENDS ---\n")
    conn = get_db_conn()
    
    # We want to check if Monday games (Day 1) are structurally different
    sql = """
        SELECT 
            EXTRACT(DOW FROM g.game_date_et) as day_of_week,
            COUNT(*) as game_count,
            
            -- 1. Crowd Factor: Does the Home team win less?
            AVG(CASE WHEN g.home_pts > g.away_pts THEN 1.0 ELSE 0.0 END) as home_win_pct,
            
            -- 2. Fatigue Factor: Are teams more tired?
            AVG(hf.rest_days) as avg_home_rest,
            AVG(af.rest_days) as avg_away_rest,
            AVG(CASE WHEN hf.rest_days = 0 THEN 1.0 ELSE 0.0 END) as home_b2b_pct,
            AVG(CASE WHEN af.rest_days = 0 THEN 1.0 ELSE 0.0 END) as away_b2b_pct,
            
            -- 3. Volatility: Are games crazier?
            AVG(ABS(g.home_pts - g.away_pts)) as avg_margin,
            AVG(g.home_pts + g.away_pts) as avg_total_points
            
        FROM games g
        JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
        JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
        WHERE g.status = 'Final'
        GROUP BY day_of_week
        ORDER BY day_of_week;
    """
    
    df = pd.read_sql(sql, conn)
    conn.close()

    # Map Day Numbers to Names for readability
    days = {0: 'Sun', 1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat'}
    df['day_name'] = df['day_of_week'].map(days)
    
    # Set index for cleaner printing
    df = df.set_index('day_name')
    
    print(df[['game_count', 'home_win_pct', 'home_b2b_pct', 'avg_total_points']].round(3))
    
    print("\n--- KEY INSIGHTS ---")
    
    # Compare Monday (1) vs Average
    mon = df.loc['Mon']
    avg = df.mean()
    
    print(f"Monday Home Win %:   {mon['home_win_pct']:.3f}  (Avg: {avg['home_win_pct']:.3f})")
    print(f"Monday Home B2B %:   {mon['home_b2b_pct']:.3f}  (Avg: {avg['home_b2b_pct']:.3f})")
    print(f"Monday Total Pts:    {mon['avg_total_points']:.1f}  (Avg: {avg['avg_total_points']:.1f})")

if __name__ == "__main__":
    main()