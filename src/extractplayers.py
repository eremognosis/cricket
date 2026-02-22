# ==============================================================================
# FILE : `extractplayers.py`
# 
# This script extracts player data from downloaded JSON files and processes them
# into a structured format. It reads player JSONs from PLAYERS_IN_DIR and creates
# staged data
# WRITTEN BY : RAJ 
# Last UPDATED : 2026-02-22
# ============================================================================== 


# ===== IMPORTS =====
import csv
import json, traceback
import os , time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from metaregis import MetadataRegistry
# ====================

REGISTRY = MetadataRegistry()
PLAYERS_IN_DIR = "./data/rawdata/playerjsons"
STAGED_DIR = "./data/stageddata"

def process_player(row):  # takes in the csv row
    player_id = row[0]
    name = row[2]
    cricinfo_id = row[9] # Mapping the hex to Cricinfo
    
    fpth = f"{PLAYERS_IN_DIR}/{player_id}.json"
    if not os.path.exists(fpth):
        with open(f"./logs/missing_players.log", "a") as logf:
            logf.write(f"{player_id} ({name}) missing JSON\n")
        
    file_hash = REGISTRY.get_file_hash(fpth)
    if REGISTRY.is_processed(fpth, file_hash):
        return None # Skip if already processed

    try:
        with open(fpth, "r", encoding="utf-8") as f:
            pjson = json.load(f)
            
        battype, bowltype = None, None
        for style in pjson.get('style', []):
            if style.get('type') == 'batting': battype = style.get('description')
            if style.get('type') == 'bowling': bowltype = style.get('description')

        data = {
            'player_id': player_id,
            'cricinfo_id': cricinfo_id,
            'name': name,
            'full_name': pjson.get('fullName'),
            'dob': pjson.get('dateOfBirth'),
            'gender': pjson.get('gender'),
            'batting_style': battype,
            'bowling_style': bowltype,
            'country_id': pjson.get('country'),
            'role': pjson.get('position', {}).get('name') if pjson.get('position') else None
        }
        
        REGISTRY.mark_processed(fpth, 'player', file_hash)
        return data
    except Exception as e:
        print(f"Error parsing {player_id}: {e}")
        with open(f"./logs/error_players.log", "a") as logf:
            logf.write(f" {time.time()} :  {player_id} ({name}) error: {e}\n Traceback: {traceback.format_exc()}\n\n")
        return None

def main():
    os.makedirs(STAGED_DIR, exist_ok=True)
    
    with open("./data/rawdata/registry/people.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader) # Skip header
        rows = list(reader)

    players_data = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = [executor.submit(process_player, row) for row in rows]
        for future in as_completed(futures):
            res = future.result()
            if res: players_data.append(res)

    if players_data:
        df = pd.DataFrame(players_data)
        # We append so we don't overwrite previously staged data
        out_path = f"{STAGED_DIR}/players.parquet"
        if os.path.exists(out_path):
            existing_df = pd.read_parquet(out_path)
            df = pd.concat([existing_df, df]).drop_duplicates(subset=['player_id'], keep='last')
        df.to_parquet(out_path, index=False)
        print(f"Staged {len(players_data)} new players.")

if __name__ == '__main__':
    main()