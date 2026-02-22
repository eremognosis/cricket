# =============================================================
# FILE : `src/extractteams.py`
# this is like basically does what thattakes the team files and stages to parquit (park it?) files
# 
# 
# WRITTEN BY : RAJ
# LAST UPDATED : 2026-02-22
# 
# ============================================================= 



# IMPORTS =============================================================

import json
import os
import glob, traceback
import pandas as pd
from metaregis import MetadataRegistry

# IMPORTS OVER =============================================================


REGISTRY = MetadataRegistry() # we aint doin same shit twice, we use the registry to track what files have been processed
tdirrrrrr = "./data/rawdata/teamjsons" # standard albeit boring
STDIRRRRRR = "./data/stageddata"

def main():
    os.makedirs(STDIRRRRRR, exist_ok=True) # check of existruih
    teams_data = []
    
    for filepath in glob.glob(f"{tdirrrrrr}/*.json"):
        file_hash = REGISTRY.get_file_hash(filepath)
        if REGISTRY.is_processed(filepath, file_hash):
            continue
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            team_id = os.path.basename(filepath).split('.')[0]
            teams_data.append({
                'team_id': team_id,
                'name': data.get('name'),
                'short_name': data.get('shortDisplayName'),
                'is_national': data.get('isNational', False),
                'abbreviation': data.get('abbreviation'),
                'location': data.get('location'),
                'colour': data.get('color'),  # yes, the API uses American spelling for this field, so we naturally dont care, and use non webster anyway
                'logo_url': data.get('logos',[{}])[0].get('href') if data.get('logos') else None
            })
            REGISTRY.mark_processed(filepath, 'team', file_hash)
        except Exception as e:
            f = open("./logs/extractteams.log", "a")
            f.write(f"Failed {filepath}: {traceback.format_exc()}\n") # learn from mistakes and push mistakes to github for others to fuck around and find out (thats what scientific matehodd is )
            f.close()

    if teams_data:
        df = pd.DataFrame(teams_data)
        out_path = f"{STDIRRRRRR}/teams.parquet"
        if os.path.exists(out_path):
            df = pd.concat([pd.read_parquet(out_path), df]).drop_duplicates(subset=['team_id'], keep='last')
        df.to_parquet(out_path, index=False)
        print(f"Staged {len(teams_data)} teams.")

if __name__ == '__main__':
    main()