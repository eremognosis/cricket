
# =========================================================
# FILE DESCRIPTION : `downloadpleyrs.py`
#
# Downloads player JSON payloads from ESPN using people registry CSV,
# stores them under OUTPUT_DIR, and tracks processed files with metaregis.
#
# WRITTEN BY : RAJ
# Last UPDATED : 2026-03-07
# =========================================================


# ===== IMPORTS =====
import os, csv, pandas as pd, sqlite3
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging, traceback
from tqdm import tqdm
from metaregis import MetadataRegistry
from bidmap import idmap
# ===================


# ===== CONFIG =====
registry = MetadataRegistry()
REGDAT = "./data/rawdata/registry/people.csv"
OUTPUT_DIR = "./data/rawdata/playerjsons"
os.makedirs(OUTPUT_DIR, exist_ok=True)  

# ###########
PDATAS = []
# ==================

TARGET_ID_KEY = "id"
pc_KEY = "pageCount"


def download_and_save_target(url,id):
    """Downloads the final JSON and saves it with proper directory handling."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        doc_id = data.get(TARGET_ID_KEY)
  # PTSD from last time, we fucked up and saved all seasons of a league in one folder, so now we have to save each season in its own folder named by the leagueid, and then the file is id.json inside it. I know its cringe but we are doing it for safety and sanity and redundancy and unemployement and caffeine and drunkedness whatevrjtg.
        os.makedirs(OUTPUT_DIR, exist_ok=True) # THIS IS THE FIX (imagine the level of pride after doing an obvious shit)
        if os.path.exists(os.path.join(OUTPUT_DIR, f"{id}.json")):
            return True  # Sher ek kaam do bar nahi karta
        filepath = os.path.join(OUTPUT_DIR, f"{id}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f: # "w" because in same file duplicate should not happen for whatever reasosn
            json.dump(data, f, indent=4)
            
        return True
    except Exception as e:
        # logging.error(f"Error downloading {url}: {e}")
        f = open("./logs/download.log", "a")
        f.write(f"Error downloading {url}: {e}\n{traceback.format_exc()}") # we append, the terminal should look cute
        f.close()
        
        return False # typical politican promises


# download_and_save_target("http://core.espnuk.org/v2/sports/cricket/athletes/591136",575)


def downloadplayer(row):
    idd = row[0]
    name = row[1]
    cid = row[9]
    if not cid:
        return False
    url = f"http://core.espnuk.org/v2/sports/cricket/athletes/{cid}"
    id = idmap(int(cid))
    target_file = os.path.join(OUTPUT_DIR, f"{id}.json")

    # Skip when this exact output payload has already been tracked.
    current_hash = registry.get_file_hash(target_file)
    if current_hash and registry.is_processed(target_file, current_hash):
        return True

    # print(cid)
    PDATAS.append({
        "id": idd,
        "idnew": id,
        
    })
    ok = download_and_save_target(url, id)

    if ok:
        new_hash = registry.get_file_hash(target_file)
        if new_hash:
            registry.mark_processed(target_file, 'player_download', new_hash)

    return ok


def main():
    with open(REGDAT, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        rows = list(reader)
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(downloadplayer, row) for row in rows]
        with tqdm(total=len(futures), desc="Downloading player data") as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)
    df = pd.DataFrame(PDATAS)
    df.to_parquet("./data/stageddata/maps.parquet", index=False)

# conn = sqlite3.connect("./data/selfpeople.db")
# curr = conn.cursor()
# curr.execute('''
#              CREATE TABLE IF NOT EXISTS people (
#                  id TEXT,
#                  idnew TEXT,
#                  cricinfoid TEXT,
#                  name TEXT
#              )
             
#              ''')
# # df = pd.DataFrame(PDATAS)
# # df.to_sql("people", conn, if_exists="replace", index=False)

# curr.executemany('INSERT INTO people (id, idnew, cricinfoid, name) VALUES (?, ?, ?, ?)', PDATAS)
# conn.commit()
# conn.close()


main()