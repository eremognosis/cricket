
  
  create view "dev"."main"."stg_peoplematchdata__dbt_tmp" as (
    

SELECT * FROM read_parquet('../data/stageddata/peoplematchdata/*/*.parquet')
  );
