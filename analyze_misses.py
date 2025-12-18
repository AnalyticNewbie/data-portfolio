import sys
import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import seaborn as sns
import matplotlib.pyplot as plt

# Config
DB_HOST = os.getenv("PGHOST", "localhost")
DB_NAME = os.getenv("PGDATABASE", "nba")
DB_USER = os.getenv("PGUSER", "nba_user")
DB_PASS = os.getenv("PGPASSWORD", "nba_pass")

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def main():
    print("\n--- ANALYZING MODEL FAILURES ---\n")
    conn = get_db_conn()
    
    # 1. Pull the "Misses" (Error > 15 points) vs "Hits" (Error < 5 points)
    sql = """
        SELECT 
            ABS((ph.pred_home_score + ph.pred_away_score) - (ph.actual_home_score + ph.actual_away_score)) as total_error,
            ph.actual_home_score - ph.actual_away_score as actual_margin,
            
            -- Potential "Hidden" Factors
            hf.rest_days as home_rest,
            af.rest_days as away_rest,
            (hf.rest_days - af.rest_days) as rest_diff,
            g.game_date_et,
            EXTRACT(DOW FROM g.game_date_et) as day_of_week, -- 0=Sun, 6=Sat
            
            -- Did the favorite win?
            CASE WHEN (ph.pred_home_score > ph.pred_away_score) = (ph.actual_home_score > ph.actual_away_score) THEN 1 ELSE 0 END as correct_pick,
            
            -- Pace/Rating context
            adv.rolling_pace_10,
            adv.rolling_ortg_10,
            adv.rolling_drtg_10
            
        FROM prediction_history ph
        JOIN games g ON ph.game_id = g.game_id
        JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
        JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
        LEFT JOIN v_team_advanced_stats adv ON g.home_team_id = adv.team_id AND g.game_id = adv.game_id
        WHERE ph.actual_home_score IS NOT NULL;
    """
    
    df = pd.read_sql(sql, conn)
    conn.close()

    if df.empty:
        print("Not enough history to analyze yet. Run this after ~50 predictions.")
        return

    # 2. Define "Big Miss"
    # We want to know: What is different about the games we missed badly?
    df['is_big_miss'] = df['total_error'] > 15
    
    print(f"Total Games Analyzed: {len(df)}")
    print(f"Big Misses (>15pts error): {df['is_big_miss'].sum()}")
    print("-" * 60)

    # 3. Compare Groups
    print("AVERAGES:   Hits (Normal)   vs   Misses (Chaos)")
    print("-" * 60)
    
    features = ['rest_diff', 'home_rest', 'rolling_pace_10', 'rolling_ortg_10']
    
    for feat in features:
        avg_miss = df[df['is_big_miss']][feat].mean()
        avg_hit = df[~df['is_big_miss']][feat].mean()
        print(f"{feat:<15} | {avg_hit:>10.2f}     vs   {avg_miss:>10.2f}")
        
    print("-" * 60)
    
    # 4. Day of Week Analysis (The "Sunday" Theory)
    # 0 = Sunday. Usually weird games happen on Sundays.
    print("\nError Rate by Day of Week (0=Sun, 6=Sat):")
    print(df.groupby('day_of_week')['total_error'].mean())

if __name__ == "__main__":
    main()