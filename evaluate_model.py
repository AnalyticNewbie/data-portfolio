import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Config
DB_HOST = os.getenv("PGHOST", "localhost")
DB_NAME = os.getenv("PGDATABASE", "nba")
DB_USER = os.getenv("PGUSER", "nba_user")
DB_PASS = os.getenv("PGPASSWORD")

def get_db_conn():
    if not DB_PASS:
        print("Error: PGPASSWORD not set.")
        sys.exit(1)
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def main():
    days = 7
    if len(sys.argv) > 1:
        days = int(sys.argv[1])

    print(f"\n--- Model Evaluation Report (Last {days} Days) ---\n")
    
    conn = get_db_conn()
    sql = """
        SELECT 
            ph.prediction_date_et,
            ht.team_abbr as home,
            at.team_abbr as away,
            ph.pred_home_score, ph.pred_away_score,
            ph.actual_home_score, ph.actual_away_score
        FROM prediction_history ph
        JOIN games g ON ph.game_id = g.game_id
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        WHERE ph.prediction_date_et >= CURRENT_DATE - INTERVAL '%s days'
          AND ph.actual_home_score IS NOT NULL
        ORDER BY ph.prediction_date_et DESC;
    """
    
    df = pd.read_sql(sql, conn, params=(days,))
    conn.close()

    if df.empty:
        print("No completed predictions found in the last 7 days.")
        print("Did you run 'python ingest_results_et.py'?")
        return

    # --- Calculations ---
    # 1. Determine Winners (True if Home Won)
    df['pred_home_win'] = df['pred_home_score'] > df['pred_away_score']
    df['actual_home_win'] = df['actual_home_score'] > df['actual_away_score']
    
    # 2. Check Accuracy
    df['correct_winner'] = df['pred_home_win'] == df['actual_home_win']
    accuracy = df['correct_winner'].mean() * 100

    # 3. Calculate Errors
    # Score Error: How far off was the Home score prediction?
    df['home_error'] = abs(df['pred_home_score'] - df['actual_home_score'])
    # Total Error: How far off was the predicted total points?
    df['pred_total'] = df['pred_home_score'] + df['pred_away_score']
    df['actual_total'] = df['actual_home_score'] + df['actual_away_score']
    df['total_error'] = abs(df['pred_total'] - df['actual_total'])

    mae_score = df['home_error'].mean()
    mae_total = df['total_error'].mean()

    # --- Display Summary ---
    print(f"Games Evaluated: {len(df)}")
    print(f"Win Prediction Accuracy: {accuracy:.1f}%")
    print(f"Avg Score Error (MAE):   {mae_score:.1f} pts per team")
    print(f"Avg Total Error (MAE):   {mae_total:.1f} pts per game")
    print("-" * 60)
    
    # --- Display Worst Misses (To learn from) ---
    print("Worst 3 Predictions (by Score Error):")
    df_sorted = df.sort_values(by='total_error', ascending=False).head(3)
    for _, row in df_sorted.iterrows():
        matchup = f"{row['home']} vs {row['away']}"
        pred = f"{int(row['pred_home_score'])}-{int(row['pred_away_score'])}"
        actual = f"{int(row['actual_home_score'])}-{int(row['actual_away_score'])}"
        print(f"  • {matchup:<12} | Pred: {pred:<9} | Actual: {actual:<9} | Diff: {int(row['total_error'])} pts")
        
    print("-" * 60)

if __name__ == "__main__":
    main()