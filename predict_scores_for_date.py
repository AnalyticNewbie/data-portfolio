import sys
import os
import joblib
import pandas as pd
import psycopg2
import json
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Config & Paths
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")
MODEL_WIN_PATH = "models/win_model.joblib"
MODEL_MARGIN_PATH = "models/margin_model.joblib" 
MODEL_TOTAL_PATH = "models/total_model.joblib"

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_schedule_for_date(target_date_et):
    conn = get_db_conn()
    sql = """
        SELECT g.game_id, g.game_date_et, ht.team_name as home_team, at.team_name as away_team,
               (hf.rolling_pd_5 - af.rolling_pd_5) as rolling_pd_5_diff,
               (hf.rolling_pd_10 - af.rolling_pd_10) as rolling_pd_10_diff,
               (hf.rest_days - af.rest_days) as rest_days_diff,
               CASE WHEN hf.rest_days = 0 THEN 1 ELSE 0 END as home_back_to_back,
               1 as home_advantage,
               (COALESCE(hsos.rolling_sos_10, 0) - COALESCE(asos.rolling_sos_10, 0)) as sos_diff,
               EXTRACT(DOW FROM g.game_date_et) as day_of_week,
               (COALESCE(hadv.rolling_pace_10, 98.0) + COALESCE(aadv.rolling_pace_10, 98.0)) / 2.0 as pace_metric,
               (COALESCE(hadv.rolling_oreb_10, 0) - COALESCE(aadv.rolling_dreb_10, 0)) as home_glass_advantage,
               hf.rolling_pd_5 as home_form, af.rolling_pd_5 as away_form,
               COALESCE(hvol.sigma_points_for, 11.0) as home_sigma_base,
               COALESCE(avol.sigma_points_for, 11.0) as away_sigma_base
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        LEFT JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
        LEFT JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
        LEFT JOIN v_strength_of_schedule hsos ON g.home_team_id = hsos.team_id AND g.game_id = hsos.game_id
        LEFT JOIN v_strength_of_schedule asos ON g.away_team_id = asos.team_id AND g.game_id = asos.game_id
        LEFT JOIN v_team_volatility hvol ON g.home_team_id = hvol.team_id AND g.game_id = hvol.game_id
        LEFT JOIN v_team_volatility avol ON g.away_team_id = avol.team_id AND g.game_id = avol.game_id
        LEFT JOIN v_team_advanced_stats hadv ON g.home_team_id = hadv.team_id AND g.game_id = hadv.game_id
        LEFT JOIN v_team_advanced_stats aadv ON g.away_team_id = aadv.team_id AND g.game_id = aadv.game_id
        WHERE g.game_date_et = %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (target_date_et,))
        rows = cur.fetchall()
    conn.close()
    return rows

def save_prediction(conn, game_id, date, model_ver, prob, h_score, a_score, s_h, s_a):
    sql = """
        INSERT INTO prediction_history 
        (game_id, prediction_date_et, model_version, pred_home_prob, pred_home_score, pred_away_score, pred_sigma_home, pred_sigma_away)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id, model_version) DO UPDATE SET 
        pred_home_prob=EXCLUDED.pred_home_prob, pred_home_score=EXCLUDED.pred_home_score, 
        pred_away_score=EXCLUDED.pred_away_score, pred_sigma_home=EXCLUDED.pred_sigma_home, pred_sigma_away=EXCLUDED.pred_sigma_away;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (game_id, date, model_ver, prob, h_score, a_score, s_h, s_a))
    conn.commit()

def main():
    if len(sys.argv) < 2:
        print("Usage: python predict_scores_for_date.py YYYY-MM-DD")
        sys.exit(1)
        
    target_date_aedt = sys.argv[1]
    dt_et = datetime.strptime(target_date_aedt, "%Y-%m-%d") - timedelta(days=1)
    target_date_et = dt_et.strftime("%Y-%m-%d")
    
    win_pipe = joblib.load(MODEL_WIN_PATH)
    margin_model = joblib.load(MODEL_MARGIN_PATH)
    total_model = joblib.load(MODEL_TOTAL_PATH)
    
    games = get_schedule_for_date(target_date_et)
    if not games:
        print(f"No games found for {target_date_et} ET.")
        return

    feature_cols = ["rolling_pd_5_diff", "rolling_pd_10_diff", "rest_days_diff", "home_back_to_back", "home_advantage", "sos_diff", "day_of_week", "pace_metric", "home_glass_advantage"]
    results_for_json = []
    conn = get_db_conn()

    for g in games:
        X_dict = {k: (float(g[k]) if g[k] is not None else 0.0) for k in feature_cols}
        X_df = pd.DataFrame([X_dict])
        
        prob_home = float(win_pipe.predict_proba(X_df)[0][1])
        pred_total = float(total_model.predict(X_df)[0])
        pred_margin = float(margin_model.predict(X_df)[0])
        
        pred_home_score = (pred_total + pred_margin) / 2
        pred_away_score = (pred_total - pred_margin) / 2
        
        h_abbr, a_abbr = g['home_team'][:3].upper(), g['away_team'][:3].upper()
        risks = ["COIN FLIP"] if 0.45 < prob_home < 0.55 else []

        # POPULATE JSON LIST INSIDE LOOP
        results_for_json.append({
            "home_abbr": h_abbr, "away_abbr": a_abbr,
            "pred_home_score": int(pred_home_score), "pred_away_score": int(pred_away_score),
            "home_prob": f"{prob_home:.1%}", "away_prob": f"{(1-prob_home):.1%}", "risks": risks
        })
        
        save_prediction(conn, g['game_id'], target_date_et, 'v5_Adv', prob_home, pred_home_score, pred_away_score, 0, 0)

    # Final Save to Subdirectory
    json_path = "projects/nba-predictor/data.json"
    final_output = {"matchups": results_for_json, "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M")}
    
    if os.path.exists(json_path):
        with open(json_path, "r") as f:
            try:
                data = json.load(f)
                data.update(final_output)
                final_output = data
            except: pass

    with open(json_path, "w") as f:
        json.dump(final_output, f, indent=4)
    conn.close()
    print(f"✅ Scores Synced to {json_path}")

if __name__ == "__main__":
    main()