{{ config(materialized='view') }}

{% set seasons_path = '../data/stageddata/seasons.parquet' %}
{% set has_seasons = false %}

{% if execute %}
	{% set q = "select count(*) as c from glob('" ~ seasons_path ~ "')" %}
	{% set r = run_query(q) %}
	{% if r and (r.rows[0][0] | int) > 0 %}
		{% set has_seasons = true %}
	{% endif %}
{% endif %}

{% if has_seasons %}
SELECT * FROM read_parquet('{{ seasons_path }}')
{% else %}
SELECT
	CAST(NULL AS VARCHAR) AS season_id,
	CAST(NULL AS VARCHAR) AS league_id,
	CAST(NULL AS INTEGER) AS year,
	CAST(NULL AS VARCHAR) AS winner_id
WHERE 1 = 0
{% endif %}
