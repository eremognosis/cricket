
  
  create view "dev"."main"."stg_leagues__dbt_tmp" as (
    

SELECT * FROM '../data/stageddata/leagues.parquet'
  );
