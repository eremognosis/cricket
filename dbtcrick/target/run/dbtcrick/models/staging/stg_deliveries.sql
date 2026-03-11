
  
  create view "dev"."main"."stg_deliveries__dbt_tmp" as (
    

SELECT * FROM '../data/stageddata/deliveries/*/*.parquet'


-- ############ Columns ############
-- matchid
-- season
-- date
-- city
-- venue
-- event
-- tourmatch
-- gender
-- type
-- overs
-- team1id
-- team2id
-- tosswin
-- decision
-- referee
-- umpire1
-- umpire2
-- tvumpire
-- isTie
-- winner
-- byRuns
-- byWickets
-- team1score
-- team2score
-- team1wickets
-- team2wickets
-- team1balls
-- team2balls
-- isDLS
-- POM
  );
