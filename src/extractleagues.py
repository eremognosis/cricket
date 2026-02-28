#=========================================================
# FILE DESCRIPTION : `extractleagues.py`
# 
# This script extracts league data from downloaded JSON files and processes them
# into a structured format. It reads league JSONs from LEAGUES_DIR and creates
# staged data suitable for further things
#
# WRITTEN BY : RAJ 
# Last UPDATED : 2026-02-22
#=========================================================

import json
import os
import glob
import pandas as pd
from metaregis import MetadataRegistry

REGISTRY = MetadataRegistry()
LEAGUES_DIR = "./data/rawdata/leaguejsons"
SEASONS_DIR = "./data/rawdata/seasons"
stageDANCE = "./data/stageddata"

def extract_id(ref_url):
    return ref_url.rstrip('/').split('/')[-1].split('?')[0] if ref_url else None  # weird but we have to do 

def main():
    os.makedirs(stageDANCE, exist_ok=True)
    leagues_data, seasons_data = [], []
    
    # 1. Extract Leagues
    for filepath in glob.glob(f"{LEAGUES_DIR}/*.json"):
        file_hash = REGISTRY.get_file_hash(filepath)
        if not REGISTRY.is_processed(filepath, file_hash):
            with open(filepath, 'r') as f:
                data = json.load(f)
                leagues_data.append({
                    'league_id': data.get('id'),
                    'name': data.get('name'),
                    'is_tournament': data.get('isTournament')
                })
            REGISTRY.mark_processed(filepath, 'league', file_hash)
            
    # 2. Extract Seasons
    for filepath in glob.glob(f"{SEASONS_DIR}/*/*.json"):
        file_hash = REGISTRY.get_file_hash(filepath)
        if not REGISTRY.is_processed(filepath, file_hash):
            league_id = os.path.basename(os.path.dirname(filepath))
            with open(filepath, 'r') as f:
                data = json.load(f)
                
            winner_ref = data.get('winner', {}).get('$ref')
            seasons_data.append({
                'season_id': str(data.get('year')),
                'league_id': league_id,
                'year': data.get('year'),
                'winner_id': extract_id(winner_ref)
            
            })
            REGISTRY.mark_processed(filepath, 'season', file_hash)

    # Save logic
    if leagues_data:
        df_l = pd.DataFrame(leagues_data)
        out_l = f"{stageDANCE}/leagues.parquet"
        if os.path.exists(out_l): df_l = pd.concat([pd.read_parquet(out_l), df_l]).drop_duplicates(subset=['league_id'], keep='last')
        df_l.to_parquet(out_l, index=False)
        
    if seasons_data:
        df_s = pd.DataFrame(seasons_data)
        out_s = f"{stageDANCE}/seasons.parquet"
        if os.path.exists(out_s): df_s = pd.concat([pd.read_parquet(out_s), df_s]).drop_duplicates(subset=['league_id', 'season_id'], keep='last')
        df_s.to_parquet(out_s, index=False)

if __name__ == '__main__':
    main()