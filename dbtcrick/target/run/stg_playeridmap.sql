
  
  create view "dev"."main"."stg_playeridmap__dbt_tmp" as (
    

SELECT * FROM read_parquet('../data/stageddata/playeridmap.parquet')
  );
