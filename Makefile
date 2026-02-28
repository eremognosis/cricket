.PHONY: run_pipeline extract transform clean


run_pipeline: extract transform

extract:
	python src/extractleagues.py
	python src/extractteams.py
	python src/extractplayers.py
	python src/extractmatches.py


transform:
	cd dbtcrick && dbt build

# Nuke the staged data and DuckDB file to start fresh
clean:
	rm -rf data/stageddata/*.parquet
	rm -f data/stageddata/crick.duckdb