
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
import argparse
import glob
import os, csv, pandas as pd
import json,time,random
import threading
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
MAX_PASSES = 3
MIN_REQUEST_INTERVAL = float(os.getenv("CRICK_MIN_REQUEST_INTERVAL", "0.05"))

# ###########
PDATAS = []
# ==================

TARGET_ID_KEY = "id"
pc_KEY = "pageCount"

_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST_TS = 0.0


def _paced_get(url, timeout=10):
    global _LAST_REQUEST_TS
    with _REQUEST_LOCK:
        wait_for = MIN_REQUEST_INTERVAL - (time.monotonic() - _LAST_REQUEST_TS)
        if wait_for > 0:
            time.sleep(wait_for)
        response = requests.get(url, timeout=timeout)
        _LAST_REQUEST_TS = time.monotonic()
    return response


def _should_retry_status(code):
    if code >= 500:
        return True
    if code in (408, 409, 425, 429):
        return True
    if 400 <= code < 500:
        return False
    return True


def _request_json_with_retry(url, timeout=10):
    last_error = None
    for attempt in range(1, MAX_PASSES + 1):
        try:
            response = _paced_get(url, timeout=timeout)
            if response.status_code >= 400:
                if not _should_retry_status(response.status_code):
                    return None
                raise requests.HTTPError(
                    f"HTTP {response.status_code} for {url}", response=response
                )
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt == MAX_PASSES:
                break
            time.sleep((0.25 * (2 ** (attempt - 1))) + random.uniform(0.05, 0.35))
    with open("./logs/download.log", "a") as logf:
        logf.write(
            f"Failed after {MAX_PASSES} attempts for {url}: {last_error}\n{traceback.format_exc()}\n"
        )
    return None


def parse_args():
    parser = argparse.ArgumentParser(description="Download player JSONs.")
    parser.add_argument(
        "--select-data",
        default=os.getenv("SELECT_DATA", ""),
        help="Match folder key under data/rawdata/matches/<SEL> or data/rawdata/<SEL> (example: WBBL).",
    )
    return parser.parse_args()


def _resolve_matches_dir(select_data):
    if not select_data:
        return None
    preferred = f"./data/rawdata/matches/{select_data}"
    fallback = f"./data/rawdata/{select_data}"
    if os.path.exists(preferred):
        return preferred
    if os.path.exists(fallback):
        return fallback
    return preferred


def _collect_registry_ids_from_matches(select_data):
    if not select_data:
        return None
    matches_dir = _resolve_matches_dir(select_data)
    if not os.path.exists(matches_dir):
        return set()

    registry_ids = set()
    for filepath in glob.glob(f"{matches_dir}/*.json"):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            people = data.get("info", {}).get("registry", {}).get("people", {})
            for value in people.values():
                if value:
                    registry_ids.add(str(value))
        except Exception:
            with open("./logs/error_players.log", "a") as logf:
                logf.write(f"Error parsing match file {filepath}\n{traceback.format_exc()}\n")
    return registry_ids


def download_and_save_target(url,id):
    """Downloads the final JSON and saves it with proper directory handling."""
    try:
        data = _request_json_with_retry(url, timeout=10)
        if not data:
            return False
        
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
    args = parse_args()
    selected_registry_ids = _collect_registry_ids_from_matches(args.select_data)

    with open(REGDAT, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        rows = list(reader)

    if selected_registry_ids is not None:
        rows = [row for row in rows if row[0] and str(row[0]) in selected_registry_ids]

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(downloadplayer, row) for row in rows]
        with tqdm(total=len(futures), desc="Downloading player data") as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)
    df = pd.DataFrame(PDATAS)
    if not df.empty:
        out_path = "./data/stageddata/playeridmap.parquet"
        if os.path.exists(out_path):
            df = pd.concat([pd.read_parquet(out_path), df]).drop_duplicates(subset=["id"], keep="last")
        df.to_parquet(out_path, index=False)

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


if __name__ == "__main__":
    main()