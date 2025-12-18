import sys
import os
import joblib
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

# Config
DB_HOST = os.getenv("PGHOST", "localhost")
DB_NAME = os.getenv("PGDATABASE", "nba")
DB_USER = os.getenv("PGUSER", "nba_user")
DB_PASS = os.getenv("PGPASSWORD", "nba_pass")

MODEL_WIN_PATH = "models/win_model.joblib"     
MODEL_MARGIN_PATH = "models/margin_model.joblib"
MODEL_TOTAL_PATH = "models/total_model.joblib"

def get_db_conn():
    if not DB_PASS:
        print("Error: PGPASSWORD not set.")
        sys.exit(1)
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def fetch_games_for_date(conn, game_date_et):
    # This query fetches inputs exactly as the main script does
    sql = """
        WITH game_features AS (
            SELECT 
                g.game_id, g.game_date_et,
                g.home_team_id, g.away_team_id,
                g.home_pts as actual_home, g.away_pts as actual_away,
                
                -- FEATURES
                (hf.rolling_pd_5 - af.rolling_pd_5) as rolling_pd_5_diff,
                (hf.rolling_pd_10 - af.rolling_pd_10) as rolling_pd_10_diff,
                (hf.rest_days - af.rest_days) as rest_days_diff,
                CASE WHEN hf.rest_days = 0 THEN 1 ELSE 0 END as home_back_to_back,
                1 as home_advantage,
                (COALESCE(hsos.rolling_sos_10, 0) - COALESCE(asos.rolling_sos_10, 0)) as sos_diff,
                
                -- VOLATILITY inputs
                COALESCE(hvol.sigma_points_for, 11.0) as home_sigma_base,
                COALESCE(avol.sigma_points_for, 11.0) as away_sigma_base,
                COALESCE(hadv.rolling_pace_10, 100.0) as home_pace,
                COALESCE(aadv.rolling_pace_10, 100.0) as away_pace,
                COALESCE(aadv.rolling_drtg_10, 112.0) as away_drtg,
                COALESCE(hadv.rolling_drtg_10, 112.0) as home_drtg

            FROM games g
            LEFT JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
            LEFT JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
            LEFT JOIN v_strength_of_schedule hsos ON g.home_team_id = hsos.team_id AND g.game_id = hsos.game_id
            LEFT JOIN v_strength_of_schedule asos ON g.away_team_id = asos.team_id AND g.game_id = asos.game_id
            LEFT JOIN v_team_volatility hvol ON g.home_team_id = hvol.team_id AND g.game_id = hvol.game_id
            LEFT JOIN v_team_volatility avol ON g.away_team_id = avol.team_id AND g.game_id = avol.game_id
            LEFT JOIN v_team_advanced_stats hadv ON g.home_team_id = hadv.team_id AND g.game_id = hadv.game_id
            LEFT JOIN v_team_advanced_stats aadv ON g.away_team_id = aadv.team_id AND g.game_id = aadv.game_id
            
            WHERE g.game_date_et = %s AND g.home_pts IS NOT NULL
        )
        SELECT * FROM game_features;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (game_date_et,))
        return cur.fetchall()

def main():
    print("--- BACKFILLING HISTORY (Last 60 Days) ---")
    conn = get_db_conn()
    
    # Load Models
    win_pipe = joblib.load(MODEL_WIN_PATH)
    margin_model = joblib.load(MODEL_MARGIN_PATH)
    total_model = joblib.load(MODEL_TOTAL_PATH)

    # Date Loop
    start_date = datetime.now() - timedelta(days=60)
    dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(60)]
    
    feature_cols = ["rolling_pd_5_diff", "rolling_pd_10_diff", "rest_days_diff", 
                    "home_back_to_back", "home_advantage", "sos_diff"]
    
    count = 0
    
    for d in dates:
        games = fetch_games_for_date(conn, d)
        if not games: continue
        
        for g in games:
            # Predict
            X_dict = {k: (float(g[k]) if g[k] is not None else 0.0) for k in feature_cols}
            X_df = pd.DataFrame([X_dict])
            
            prob = float(win_pipe.predict_proba(X_df)[0][1])
            pred_total = float(total_model.predict(X_df)[0])
            pred_margin = float(margin_model.predict(X_df)[0])
            
            pred_h = (pred_total + pred_margin) / 2
            pred_a = (pred_total - pred_margin) / 2
            
            # Calculate Composite Sigma (for completeness)
            game_pace = (float(g['home_pace']) + float(g['away_pace'])) / 2.0
            pace_mod = game_pace / 100.0
            h_def_mod = float(g['away_drtg']) / 115.0
            a_def_mod = float(g['home_drtg']) / 115.0
            sigma_h = float(g['home_sigma_base']) * pace_mod * h_def_mod
            sigma_a = float(g['away_sigma_base']) * pace_mod * a_def_mod

            # Insert into History (Mark as 'backfill')
            sql = """
                INSERT INTO prediction_history 
                (game_id, prediction_date_et, model_version, 
                 pred_home_prob, pred_home_score, pred_away_score, 
                 pred_sigma_home, pred_sigma_away, 
                 actual_home_score, actual_away_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, model_version) DO NOTHING;
            """
            with conn.cursor() as cur:
                cur.execute(sql, (
                    g['game_id'], d, 'v2.1_backfill',
                    prob, pred_h, pred_a, sigma_h, sigma_a,
                    g['actual_home'], g['actual_away']
                ))
            count += 1
            
    conn.commit()
    conn.close()
    print(f"Successfully backfilled {count} predictions.")

if __name__ == "__main__":
    main()