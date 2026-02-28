{{ config(materialized='table') }}

SELECT 
    *,
    -- Did they hit a boundary off this bowler in the last 5 balls?
    SUM(CAST(is_boundary AS INT)) OVER (
        PARTITION BY matchid, batter_id, bowler_id 
        ORDER BY inning, over_num, ball_num 
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) > 0 AS recent_boundary_same_bowler,
    
    -- Were they the non-striker when a run out occurred? (The sacrifice)
    CASE WHEN is_wicket = 1 AND wicket_type = 'run out' AND player_out_id = non_striker_id THEN 1 ELSE 0 END AS sacrificed_partner
FROM {{ ref('stg_deliveries') }}