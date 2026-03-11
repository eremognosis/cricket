
  
  create view "dev"."main"."stg_teams__dbt_tmp" as (
    

SELECT * FROM '../data/stageddata/teams.parquet'
  );
