import sys
import json
from nba_api.stats.endpoints import boxscoresummaryv3

GAME_ID = '0022500387' # Using the same recent game ID

def check_v3():
    print(f"--- Probing Game {GAME_ID} using V3 Endpoint ---")
    try:
        # V3 returns a dictionary, not dataframes
        box = boxscoresummaryv3.BoxScoreSummaryV3(game_id=GAME_ID)
        data = box.get_dict()
        
        # The structure is usually nested. Let's look for "officials" key.
        # It is often under 'boxScoreSummary' -> 'officials'
        
        if 'boxScoreSummary' in data:
            summary = data['boxScoreSummary']
            if 'officials' in summary:
                refs = summary['officials']
                if refs:
                    print(f"\nSUCCESS! Found {len(refs)} officials:")
                    for ref in refs:
                        print(f"- {ref.get('firstName', '')} {ref.get('familyName', '')} (#{ref.get('jerseyNum', '')})")
                    return True
                else:
                    print("Found 'officials' key, but list is empty.")
            else:
                print("No 'officials' key in boxScoreSummary.")
                # Debug: Print keys to see what IS there
                print(f"Available keys: {summary.keys()}")
        else:
            print("Structure mismatch: 'boxScoreSummary' not found.")
            
    except Exception as e:
        print(f"Error accessing V3: {e}")
        return False

if __name__ == "__main__":
    check_v3()