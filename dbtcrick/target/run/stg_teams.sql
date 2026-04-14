
  
  create view "dev"."main"."stg_teams__dbt_tmp" as (
    

SELECT * FROM read_parquet('../data/stageddata/teams.parquet')
  );
