DROP VIEW IF EXISTS v_rolling_form_scheduled;

CREATE VIEW v_rolling_form_scheduled AS
WITH final_team_games AS (
    SELECT
        g.game_id,
        g.game_date_et,
        g.home_team_id AS team_id,
        TRUE AS is_home,
        g.home_pts AS pts_for,
        g.away_pts AS pts_against
    FROM games g
    WHERE g.status = 'final'
      AND g.home_pts IS NOT NULL
      AND g.away_pts IS NOT NULL

    UNION ALL

    SELECT
        g.game_id,
        g.game_date_et,
        g.away_team_id AS team_id,
        FALSE AS is_home,
        g.away_pts AS pts_for,
        g.home_pts AS pts_against
    FROM games g
    WHERE g.status = 'final'
      AND g.home_pts IS NOT NULL
      AND g.away_pts IS NOT NULL
),
scheduled_team_games AS (
    SELECT
        g.game_id,
        g.game_date_et AS scheduled_date_et,
        g.home_team_id AS team_id,
        TRUE AS is_home
    FROM games g
    WHERE g.status = 'scheduled'

    UNION ALL

    SELECT
        g.game_id,
        g.game_date_et AS scheduled_date_et,
        g.away_team_id AS team_id,
        FALSE AS is_home
    FROM games g
    WHERE g.status = 'scheduled'
)
SELECT
    s.game_id,
    s.scheduled_date_et,
    s.team_id,
    s.is_home,

    -- Last 5: average point differential (pts_for - pts_against)
    (SELECT AVG(margin)::numeric(10,3)
     FROM (
        SELECT (f.pts_for - f.pts_against) AS margin
        FROM final_team_games f
        WHERE f.team_id = s.team_id
          AND f.game_date_et < s.scheduled_date_et
        ORDER BY f.game_date_et DESC
        LIMIT 5
     ) x
    ) AS rolling_pd_5,

    (SELECT COUNT(*)
     FROM (
        SELECT 1
        FROM final_team_games f
        WHERE f.team_id = s.team_id
          AND f.game_date_et < s.scheduled_date_et
        ORDER BY f.game_date_et DESC
        LIMIT 5
     ) x
    ) AS rolling_pd_5_n,

    -- Last 10: average point differential
    (SELECT AVG(margin)::numeric(10,3)
     FROM (
        SELECT (f.pts_for - f.pts_against) AS margin
        FROM final_team_games f
        WHERE f.team_id = s.team_id
          AND f.game_date_et < s.scheduled_date_et
        ORDER BY f.game_date_et DESC
        LIMIT 10
     ) x
    ) AS rolling_pd_10,

    (SELECT COUNT(*)
     FROM (
        SELECT 1
        FROM final_team_games f
        WHERE f.team_id = s.team_id
          AND f.game_date_et < s.scheduled_date_et
        ORDER BY f.game_date_et DESC
        LIMIT 10
     ) x
    ) AS rolling_pd_10_n

FROM scheduled_team_games s;
