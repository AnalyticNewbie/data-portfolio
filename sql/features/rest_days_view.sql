CREATE OR REPLACE VIEW public.v_rest_days_scheduled AS
WITH final_team_games AS (
    -- One row per team per FINAL game
    SELECT g.game_date_et, g.home_team_id AS team_id
    FROM games g
    WHERE g.status = 'final' AND g.season_type = 'regular'

    UNION ALL

    SELECT g.game_date_et, g.away_team_id AS team_id
    FROM games g
    WHERE g.status = 'final' AND g.season_type = 'regular'
),
scheduled_team_games AS (
    -- One row per team per SCHEDULED game (plus is_home)
    SELECT
        g.game_id,
        g.game_date_et AS scheduled_date_et,
        g.home_team_id AS team_id,
        TRUE AS is_home
    FROM games g
    WHERE g.status = 'scheduled' AND g.season_type = 'regular'

    UNION ALL

    SELECT
        g.game_id,
        g.game_date_et AS scheduled_date_et,
        g.away_team_id AS team_id,
        FALSE AS is_home
    FROM games g
    WHERE g.status = 'scheduled' AND g.season_type = 'regular'
),
last_final_before AS (
    -- For each scheduled (game_id, team_id), find the last FINAL game before it
    SELECT
        s.game_id,
        s.scheduled_date_et,
        s.team_id,
        s.is_home,
        MAX(f.game_date_et) AS last_final_date_et
    FROM scheduled_team_games s
    LEFT JOIN final_team_games f
        ON f.team_id = s.team_id
       AND f.game_date_et < s.scheduled_date_et
    GROUP BY s.game_id, s.scheduled_date_et, s.team_id, s.is_home
)
SELECT
    l.game_id,
    l.scheduled_date_et,
    l.team_id,
    l.is_home,
    l.last_final_date_et,
    -- NBA "days off" convention: Mon->Tue = 0 rest days, Mon->Wed = 1 rest day
    CASE
        WHEN l.last_final_date_et IS NULL THEN NULL
        ELSE (l.scheduled_date_et::date - l.last_final_date_et::date - 1)
    END AS rest_days,
    (l.last_final_date_et IS NULL) AS rest_days_missing
FROM last_final_before l;
