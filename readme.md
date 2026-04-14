# Crick ETL

This project downloads cricket data, stages parquet files, and builds dbt models.

## New download behavior

- Download scripts now use jittered retries with a 3-pass max.
- Retry policy: retries transient failures (`5xx`, `408`, `409`, `425`, `429`) and skips hard `4xx`.
- Optional request pacing via `CRICK_MIN_REQUEST_INTERVAL` (seconds).

Example:

```bash
CRICK_MIN_REQUEST_INTERVAL=0.08 make extract
```

## Selective league download

Use `LEAGUE_ID` to fetch only one ESPN league id (for example BBL id):

```bash
make extract LEAGUE_ID=200
```

## SEL pipeline (matches folder specific)

Use `SEL` to run a matches-folder-specific flow:

```bash
make pipeline SEL=WBBL
```

This does:

- player download only for players found in `data/rawdata/matches/WBBL` (or `data/rawdata/WBBL`)
- match extraction for the same folder