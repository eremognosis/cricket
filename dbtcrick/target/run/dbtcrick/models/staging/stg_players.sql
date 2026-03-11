
  
  create view "dev"."main"."stg_players__dbt_tmp" as (
    

SELECT * FROM '../data/stageddata/players.parquet'
  );
