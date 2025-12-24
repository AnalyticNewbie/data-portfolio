import sys, os, joblib, pandas as pd, psycopg2, json
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv

os.environ['PYTHONIOENCODING'] = 'utf-8'
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

MODEL_WIN_PATH = "models/win_model.joblib"
MODEL_MARGIN_PATH = "models/margin_model.joblib" 
MODEL_TOTAL_PATH = "models/total_model.joblib"

TEAM_MAP = {
    1610612737: "ATL", 1610612738: "BOS", 1610612739: "CLE",
    1610612740: "NOP", 1610612741: "CHI", 1610612742: "DAL",
    1610612743: "DEN", 1610612744: "GSW", 1610612745: "HOU",
    1610612746: "LAC", 1610612747: "LAL", 1610612748: "MIA",
    1610612749: "MIL", 1610612750: "MIN", 1610612751: "BKN",
    1610612752: "NYK", 1610612753: "ORL", 1610612754: "IND",
    1610612755: "PHI", 1610612756: "PHX", 1610612757: "POR",
    1610612758: "SAC", 1610612759: "SAS", 1610612760: "OKC",
    1610612761: "TOR", 1610612762: "UTA", 1610612763: "MEM",
    1610612764: "WAS", 1610612765: "DET", 1610612766: "CHA"
}

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_crew_bias(game_id, conn):
    """Fetches the aggregate bias metrics for the assigned officiating crew."""
    sql = """
        SELECT 
            AVG(pf_bias) as pf_bias,
            AVG(home_bias_delta) as home_bias,
            AVG(timing_factor) as timing
        FROM mv_ref_master_profiles
        WHERE official_id IN (SELECT official_id FROM game_officials WHERE game_id = %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (game_id,))
        res = cur.fetchone()
        if not res or res[0] is None:
            return {"pf_bias": 0.0, "home_bias": 0.0, "timing": 0.5}
        return {"pf_bias": float(res[0]), "home_bias": float(res[1]), "timing": float(res[2])}

def get_schedule_for_date(target_date_et):
    conn = get_db_conn()
    sql = """
        SELECT g.game_id, g.game_date_et, g.home_team_id, g.away_team_id,
               ht.team_name as home_team, at.team_name as away_team,
               (hf.rolling_pd_5 - af.rolling_pd_5) as rolling_pd_5_diff,
               (hf.rolling_pd_10 - af.rolling_pd_10) as rolling_pd_10_diff,
               (hf.rest_days - af.rest_days) as rest_days_diff,
               CASE WHEN hf.rest_days = 0 THEN 1 ELSE 0 END as home_back_to_back,
               1 as home_advantage,
               (COALESCE(hsos.rolling_sos_10, 0) - COALESCE(asos.rolling_sos_10, 0)) as sos_diff,
               EXTRACT(DOW FROM g.game_date_et) as day_of_week,
               (COALESCE(hadv.rolling_pace_10, 98.0) + COALESCE(aadv.rolling_pace_10, 98.0)) / 2.0 as pace_metric,
               (COALESCE(hadv.rolling_oreb_10, 0) - COALESCE(aadv.rolling_dreb_10, 0)) as home_glass_advantage
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        LEFT JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
        LEFT JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
        LEFT JOIN v_strength_of_schedule hsos ON g.home_team_id = hsos.team_id AND g.game_id = hsos.game_id
        LEFT JOIN v_strength_of_schedule asos ON g.away_team_id = asos.team_id AND g.game_id = asos.game_id
        LEFT JOIN v_team_advanced_stats hadv ON g.home_team_id = hadv.team_id AND g.game_id = hadv.game_id
        LEFT JOIN v_team_advanced_stats aadv ON g.away_team_id = aadv.team_id AND g.game_id = aadv.game_id
        WHERE g.game_date_et = %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (target_date_et,))
        rows = cur.fetchall()
    conn.close()
    return rows

def main():
    if len(sys.argv) < 2:
        print("Usage: python predict_scores_for_date.py YYYY-MM-DD")
        sys.exit(1)
        
    target_date_et = sys.argv[1]
    
    win_pipe = joblib.load(MODEL_WIN_PATH)
    margin_model = joblib.load(MODEL_MARGIN_PATH)
    total_model = joblib.load(MODEL_TOTAL_PATH)
    
    games = get_schedule_for_date(target_date_et)
    if not games:
        print(f"No games found for {target_date_et} ET.")
        return

    conn = get_db_conn()
    feature_cols = ["rolling_pd_5_diff", "rolling_pd_10_diff", "rest_days_diff", "home_back_to_back", "home_advantage", "sos_diff", "day_of_week", "pace_metric", "home_glass_advantage"]
    results_for_json = []

    for g in games:
        X_df = pd.DataFrame([{k: (float(g[k]) if g[k] is not None else 0.0) for k in feature_cols}])
        
        # Base predictions from team data
        prob_home = float(win_pipe.predict_proba(X_df)[0][1])
        pred_total = float(total_model.predict(X_df)[0])
        pred_margin = float(margin_model.predict(X_df)[0])
        
        # APPLY OFFICIATING FACTORS
        ref = get_crew_bias(g['game_id'], conn)
        pred_total += (ref['pf_bias'] * 0.85)
        pred_margin += (ref['home_bias'] * 10.0)
        
        # Final adjusted scores
        pred_home_score = (pred_total + pred_margin) / 2
        pred_away_score = (pred_total - pred_margin) / 2
        
        h_abbr = TEAM_MAP.get(int(g['home_team_id']), g['home_team'][:3].upper())
        a_abbr = TEAM_MAP.get(int(g['away_team_id']), g['away_team'][:3].upper())
        
        results_for_json.append({
            "game_date": target_date_et,
            "home_abbr": h_abbr, 
            "away_abbr": a_abbr,
            "pred_home_score": int(pred_home_score), 
            "pred_away_score": int(pred_away_score),
            "home_prob": f"{prob_home:.1%}", 
            "away_prob": f"{(1-prob_home):.1%}", 
            "risks": ["COIN FLIP"] if 0.45 < prob_home < 0.55 else []
        })

    conn.close()
    json_path = "data.json"
    final_output = {
        "target_date_et": target_date_et,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "games": results_for_json,
        "top_props": [] 
    }
    
    with open(json_path, "w", encoding='utf-8') as f:
        json.dump(final_output, f, indent=4, ensure_ascii=False)
        
    print(f"✅ Success: Ref-Adjusted scores synced to {json_path}")

if __name__ == "__main__":
    main()