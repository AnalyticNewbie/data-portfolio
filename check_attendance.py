import sys
import time
import pandas as pd
from nba_api.stats.endpoints import boxscoresummaryv2
from nba_api.stats.endpoints import leaguegamelog

# 1. Get a list of Game IDs for Mondays vs Saturdays
print("Fetching Game Log...")
log = leaguegamelog.LeagueGameLog(season='2025-26', player_or_team_abbreviation='T')
games = log.get_data_frames()[0]

# Convert date string to datetime
games['GAME_DATE'] = pd.to_datetime(games['GAME_DATE'])
games['DAY_OF_WEEK'] = games['GAME_DATE'].dt.dayofweek # 0=Mon, 5=Sat

# Filter unique Game IDs (GameLog has 2 rows per game)
unique_games = games.drop_duplicates(subset=['GAME_ID'])

mon_games = unique_games[unique_games['DAY_OF_WEEK'] == 0]['GAME_ID'].tolist()[:15]
sat_games = unique_games[unique_games['DAY_OF_WEEK'] == 5]['GAME_ID'].tolist()[:15]

print(f"Sampling {len(mon_games)} Monday games and {len(sat_games)} Saturday games...")

def get_attendance(game_id):
    try:
        # boxscoresummaryv2 returns a dataset called 'GameInfo' (index 4 or similar)
        summary = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
        # GameInfo is usually the 2nd dataframe returned (index 1)
        # Columns: GAME_DATE, ATTENDANCE, GAME_TIME
        game_info = summary.game_info.get_data_frame()
        if not game_info.empty:
            return game_info['ATTENDANCE'].iloc[0]
    except Exception as e:
        return None
    return None

data = []

# Loop Monday
print("\n--- Checking MONDAY Attendance ---")
for gid in mon_games:
    att = get_attendance(gid)
    if att:
        print(f"Game {gid}: {att:,}")
        data.append({'Day': 'Monday', 'Attendance': att})
    time.sleep(0.6)

# Loop Saturday
print("\n--- Checking SATURDAY Attendance ---")
for gid in sat_games:
    att = get_attendance(gid)
    if att:
        print(f"Game {gid}: {att:,}")
        data.append({'Day': 'Saturday', 'Attendance': att})
    time.sleep(0.6)

# Final Verdict
df = pd.DataFrame(data)
if not df.empty:
    print("\n--- FINAL VERDICT ---")
    print(df.groupby('Day')['Attendance'].mean().round(0))
else:
    print("No data found.")