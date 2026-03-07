# Project Directory Structure

Generated on: 2026-03-07 01:01:46

```
/
├── STRUCTURE.md
├── readme.md
├── requirements.txt
├── .gitignore
├── Makefile
├── data/
│   ├── selfpeople.db
│   ├── rawdata/
│   │   ├── people.csv:Zone.Identifier
│   │   ├── leaguejsons/
│   │   │   ├── 1001201261.json
│   │   │   ├── 1001761657.json
│   │   │   └── (12780 more .json files)
│   │   ├── matches/
│   │   │   └── WT20I/
│   │   │       ├── 1043989.json:Zone.Identifier
│   │   │       ├── 1043991.json:Zone.Identifier
│   │   │       ├── (1859 more .Identifier files)
│   │   │       ├── 1043989.json
│   │   │       ├── 1043991.json
│   │   │       ├── (1858 more .json files)
│   │   │       └── README.txt
│   │   ├── playerjsons/
│   │   │   ├── 1000145860.json
│   │   │   ├── 1000485098.json
│   │   │   └── (16901 more .json files)
│   │   ├── registry/
│   │   │   └── people.csv
│   │   └── seasons/
│   │       ├── 101048/
│   │       │   └── 2001.json
│   │       ├── 101385/
│   │       │   └── 2001.json
│   │       ├── 1020677/
│   │       │   └── 2016.json
│   │       ├── 1030263/
│   │       │   └── 2016.json
│   │       ├── 1046579/
│   │       │   └── 2016.json
│   │       ├── 1048451/
│   │       │   └── 2016.json
│   │       ├── 105647/
│   │       │   └── 2001.json
│   │       ├── 1058107/
│   │       │   └── 2016.json
│   │       ├── 1058124/
│   │       │   └── 2016.json
│   │       ├── 1058342/
│   │       │   └── 2016.json
│   │       └── (6437 more subdirectories)
│   └── stageddata/
│       ├── registry.db
│       ├── registry.db-shm
│       └── registry.db-wal
├── dbtcrick/
│   ├── dbt_project.yml
│   ├── schema.yml
│   └── models/
│       ├── inter/
│       │   └── intdeliverycont.sql
│       ├── mart/
│       │   └── batterstats1.sql
│       └── staging/
│           ├── stg_deliveries.sql
│           ├── stg_matches.sql
│           └── sources.yml
├── logs/
│   ├── download.log
│   ├── error_players.log
│   └── missing_players.log
└── src/
    ├── bidmap.py:Zone.Identifier
    ├── bidmap.py
    ├── downloadespn.py
    ├── downloadpleyrs.py
    ├── extractleagues.py
    ├── extractmatches.py
    ├── extractplayers.py
    ├── extractteams.py
    ├── generate_structure.py
    ├── getleaguefiles.py
    └── metaregis.py
```
