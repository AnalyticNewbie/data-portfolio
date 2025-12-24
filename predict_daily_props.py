import sys, os, json, joblib, pytz
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)

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

FEATURE_COLUMNS = ['pts_l5', 'reb_l5', 'ast_l5', 'min_l5', 'pts_l10', 'reb_l10', 'ast_l10', 'pts_l20']

try:
    pts_m = joblib.load("models/points_model.joblib")
    reb_m = joblib.load("models/rebounds_model.joblib")
    ast_m = joblib.load("models/assists_model.joblib")
    print("✅ Models loaded successfully.")
except Exception as e:
    print(f"❌ FATAL ERROR: Models missing in 'models/'. {e}")
    sys.exit(1)

def get_current_nba_date():
    tz_et = pytz.timezone('US/Eastern')
    now_et = datetime.now(tz_et)
    if now_et.hour < 4:
        return (now_et - timedelta(days=1)).strftime("%Y-%m-%d")
    return now_et.strftime("%Y-%m-%d")

def get_prediction_data(player_id, player_name):
    sql = text("SELECT pts, reb, ast, min FROM player_logs WHERE player_id = :p_id ORDER BY game_date DESC LIMIT 20")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"p_id": int(player_id)})
    
    if df.empty: return None

    def get_avg(df, col, n):
        return float(df.head(n)[col].mean()) if len(df) >= n else float(df[col].mean())

    feats = {
        'pts_l5': get_avg(df, 'pts', 5), 'reb_l5': get_avg(df, 'reb', 5),
        'ast_l5': get_avg(df, 'ast', 5), 'min_l5': get_avg(df, 'min', 5),
        'pts_l10': get_avg(df, 'pts', 10), 'reb_l10': get_avg(df, 'reb', 10),
        'ast_l10': get_avg(df, 'ast', 10), 'pts_l20': float(df['pts'].mean())
    }
    
    X = pd.DataFrame([feats])[FEATURE_COLUMNS]
    
    try:
        return {
            "name": player_name,
            "proj_pts": round(float(pts_m.predict(X)[0]), 1),
            "proj_reb": round(float(reb_m.predict(X)[0]), 1),
            "proj_ast": round(float(ast_m.predict(X)[0]), 1),
            "avg_pts": round(float(df['pts'].mean()), 1)
        }
    except Exception as e:
        print(f"      ❌ Prediction Failed for {player_name}: {e}")
        return None

def main():
    nba_today_str = get_current_nba_date()
    target_date = sys.argv[1] if len(sys.argv) > 1 else nba_today_str

    print(f"=== NBA PROP ENGINE v2.6.9 (Strict Date: {target_date}) ===")
    
    query = text("""
        SELECT DISTINCT p.player_id, p.name, p.team_id, g.home_team_id, g.away_team_id, g.game_date_et
        FROM players p
        INNER JOIN games g ON (p.team_id = g.home_team_id OR p.team_id = g.away_team_id)
        WHERE g.game_date_et = :t_date
    """)
    
    with engine.connect() as conn:
        active_data = pd.read_sql(query, conn, params={"t_date": target_date})
    
    if active_data.empty:
        print(f"No games found for {target_date}. Exiting.")
        return

    all_projections = []
    processed_players = set()

    for _, row in active_data.iterrows():
        p_id = int(row['player_id'])
        if p_id in processed_players: continue
            
        res = get_prediction_data(p_id, row['name'])
        if res:
            res["game_date"] = str(row['game_date_et'])
            res["home_abbr"] = TEAM_MAP.get(int(row['home_team_id']), "NBA")
            res["away_abbr"] = TEAM_MAP.get(int(row['away_team_id']), "PRO")
            all_projections.append(res)
            processed_players.add(p_id)

    top_5 = sorted(all_projections, key=lambda x: abs(x['proj_pts'] - x['avg_pts']), reverse=True)[:5]
    
    output = {
        "target_date_et": target_date,
        "projections": top_5,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M")
    }

    with open("data_fresh.json", "w", encoding='utf-8') as f:
        json.dump(output, f, indent=4, ensure_ascii=False)
    
    print(f"✅ SUCCESS: {len(top_5)} verified insights for {target_date} saved.")

if __name__ == "__main__":
    main()