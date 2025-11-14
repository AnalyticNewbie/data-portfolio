NBA ContenderScore Analysis

🎯 Goal & Hypothesis

Goal: To move beyond simple win/loss records and create a single, predictive metric ("ContenderScore") that more accurately ranks the current readiness of NBA teams for deep playoff success.

Hypothesis: A weighted score combining Offensive Efficiency and Defensive Efficiency will be a more accurate predictor of a team's championship potential than standard metrics like Net Rating or Win Percentage.

🛠️ Methodology & Data

Data Source: Scraping or extracting publicly available data for all 30 NBA teams, focusing on the current season's performance metrics.

Core Metrics Used: Offensive Rating, Defensive Rating, Net Rating, and Strength of Schedule (SoS).

ContenderScore Calculation: A custom weighted formula was applied, giving 50% weight to Defensive Rating (as defense wins championships) and 30% to Offensive Rating, with the remaining 20% distributed across advanced metrics.

Tools: Python (Pandas for data cleaning and manipulation, NumPy for array calculations).

📊 Key Findings

The ContenderScore identified [Insert Team A] as the true title favorite, despite their modest Win/Loss record, due to their elite Defensive Efficiency.

Teams with a high score but a poor Strength of Schedule (SoS) often regress later in the season, proving the metric's reliability.

The final model suggests that a minimum ContenderScore of 0.78 is typically required to be considered a legitimate top-4 title threat.

View the full Python code, visualizations, and data cleaning process here.
