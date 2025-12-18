import sys
import pandas as pd
from nba_api.stats.endpoints import boxscoresummaryv2
from nba_api.stats.library.parameters import Season

# Config
# Let's check a game we know happened recently (e.g., from your schedule)
GAME_ID = '0022500001' # Opening night game, or pick a recent one from your database

def get_officials(game_id):
    print(f"--- Probing Game {game_id} for Officials ---")
    try:
        # boxscoresummaryv2 returns multiple datasets.
        # Index 0: GameSummary
        # Index 1: Other Info
        # Index 2: Officials (Usually)
        summary = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
        dfs = summary.get_data_frames()
        
        # Look for the dataframe with 'OfficialID' or 'FirstName'
        ref_df = None
        for i, df in enumerate(dfs):
            if 'OFFICIAL_ID' in df.columns or 'FIRST_NAME' in df.columns:
                print(f"Found Officials in Table Index {i}!")
                ref_df = df
                break
        
        if ref_df is not None:
            print("\nOfficials Found:")
            print(ref_df[['FIRST_NAME', 'LAST_NAME', 'JERSEY_NUM']])
            return True
        else:
            print("No Officials table found in response.")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    # You can change this ID to one of the games from your recent 'predict_scores' output
    # e.g., DAL vs DET was probably 0022500XXX. 
    # Let's try to grab a valid recent ID from your database if possible, or just hardcode one.
    # 0022500387 seems to be a recent game ID range based on your logs.
    get_officials('0022500387')