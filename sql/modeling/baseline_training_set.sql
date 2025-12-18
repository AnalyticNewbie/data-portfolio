WITH final_games AS (
    SELECT
        g.game_id,
        g.game_date_et,
        g.home_team_id,
        g.away_team_id,
        CASE
            WHEN g.home_pts > g.away_pts THEN 1
            ELSE 0
        END AS home_win
    FROM games g
    WHERE g.status = 'final'
      AND g.home_pts IS NOT NULL
      AND g.away_pts IS NOT NULL
),
home_features AS (
    SELECT
        f.game_id,
        f.team_id,
        f.rest_days,
        (f.feature_json->>'rolling_pd_5')::numeric AS rolling_pd_5,
        (f.feature_json->>'rolling_pd_10')::numeric AS rolling_pd_10
    FROM features_team_game f
),
away_features AS (
    SELECT
        f.game_id,
        f.team_id,
        f.rest_days,
        (f.feature_json->>'rolling_pd_5')::numeric AS rolling_pd_5,
        (f.feature_json->>'rolling_pd_10')::numeric AS rolling_pd_10
    FROM features_team_game f
)
SELECT
    g.game_id,
    g.game_date_et,
    g.home_win,

    -- Form differentials
    (hf.rolling_pd_5 - af.rolling_pd_5) AS rolling_pd_5_diff,
    (hf.rolling_pd_10 - af.rolling_pd_10) AS rolling_pd_10_diff,

    -- Fatigue
    (hf.rest_days - af.rest_days) AS rest_days_diff,
    CASE WHEN hf.rest_days = 0 THEN 1 ELSE 0 END AS home_back_to_back,

    -- Context
    1 AS home_advantage
FROM final_games g
JOIN home_features hf
  ON hf.game_id = g.game_id
 AND hf.team_id = g.home_team_id
JOIN away_features af
  ON af.game_id = g.game_id
 AND af.team_id = g.away_team_id;