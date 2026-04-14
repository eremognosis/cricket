{{ config(materialized='view') }}

{% set leagues_path = '../data/stageddata/leagues.parquet' %}
{% set has_leagues = false %}

{% if execute %}
	{% set q = "select count(*) as c from glob('" ~ leagues_path ~ "')" %}
	{% set r = run_query(q) %}
	{% if r and (r.rows[0][0] | int) > 0 %}
		{% set has_leagues = true %}
	{% endif %}
{% endif %}

{% if has_leagues %}
SELECT * FROM read_parquet('{{ leagues_path }}')
{% else %}
SELECT
	CAST(NULL AS VARCHAR) AS league_id,
	CAST(NULL AS VARCHAR) AS name,
	CAST(NULL AS BOOLEAN) AS is_tournament
WHERE 1 = 0
{% endif %}
