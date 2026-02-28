{{ config(materialized='table') }}

WITH base_deliveries AS (
    SELECT 
        d.*,
        m.type AS format,
        m.league_id,
        m.gender
    FROM {{ ref('stg_deliveries') }} d
    JOIN {{ ref('stg_matches') }} m ON d.matchid = m.matchid
)

SELECT 
    batter_id AS player_id,
    format,
    gender,
    league_id,
    
    -- The Platinum Electrode metrics
    COUNT(DISTINCT matchid) AS matches_played,
    SUM(runs_batter_thisball) AS total_runs,
    SUM(is_legal_ball) AS balls_faced,
    SUM(CAST(is_wicket AS INT)) AS times_dismissed,
    
    -- Strike Rate & Average
    CASE WHEN SUM(CAST(is_wicket AS INT)) = 0 THEN SUM(runs_batter_thisball) 
         ELSE SUM(runs_batter_thisball) * 1.0 / SUM(CAST(is_wicket AS INT)) END AS batting_average,
    (SUM(runs_batter_thisball) * 100.0 / NULLIF(SUM(is_legal_ball), 0)) AS strike_rate,

    -- The Grains
    SUM(CAST(is_dot AS INT)) AS dot_balls,
    SUM(CASE WHEN runs_batter_thisball = 1 THEN 1 ELSE 0 END) AS ones,
    SUM(CASE WHEN runs_batter_thisball = 2 THEN 1 ELSE 0 END) AS twos,
    SUM(CASE WHEN runs_batter_thisball = 4 AND is_boundary = 1 THEN 1 ELSE 0 END) AS fours,
    SUM(CASE WHEN runs_batter_thisball = 6 AND is_boundary = 1 THEN 1 ELSE 0 END) AS sixes,
    
    -- Matchups (Pace/Spin) - Assuming you joined bowler type from a stg_players table
    SUM(CASE WHEN p.bowlstyle ILIKE '%fast%' OR p.bowlstyle ILIKE '%medium%' THEN runs_batter_thisball ELSE 0 END) AS vs_pace_runs,

    -- Phase Performance
    SUM(CASE WHEN phase = 'Powerplay' THEN runs_batter_thisball ELSE 0 END) AS pp_runs
    
FROM base_deliveries bd
LEFT JOIN {{ ref('stg_players') }} p ON bd.bowler_id = p.id
GROUP BY 1, 2, 3, 4