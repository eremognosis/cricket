#=========================================================
# FILE DESCRIPTION : `getteams.py`
# 
# Downloads team metadata from ESPN API, fetches all team JSONs
# from paginated endpoints, and saves them locally with registry tracking.
#
# WRITTEN BY : RAJ 
# Last UPDATED : 2026-03-07
#=========================================================


# ===== IMPORTS =====
import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging, traceback
from tqdm import tqdm
from metaregis import MetadataRegistry
from bidmap import tidmap
from time import sleep
import random
# ===================


# ===== CONFIG =====
REGISTRY = MetadataRegistry()
BASE_URL  = "http://core.espnuk.org/v2/sports/cricket/teams"
OUTPUT_DIR = "./data/rawdata/teamjsons"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("./logs", exist_ok=True)
# ==================

def gettotalpages():
    try:
        response = requests.get(BASE_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return int(data.get("pageCount", 1))
    except Exception as e:
        logging.error(f"Error fetching total pages: {e}")
        return 1

def geturls(URLS,i):
    # if os.path.exists(f"{OUTPUT_DIR}/{i}.json"):
    #     return
    urssss = []
    url = f"{BASE_URL}?page={i}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
    except Exception as e:
        logging.error(f"Error fetching URLs for page {i}: {e}")
        return
    for item in items:
        ref = item.get("$ref")
        if ref:
            urssss.append(ref)
    URLS.extend(urssss)
    sleep(random.uniform(0.1,0.4))
def download_and_save_target(url):
    """Downloads the final JSON and saves it with proper directory handling."""
    id = url.rstrip('/').split('/')[-1].split('?')[0]
    id = tidmap(int(id))
    filepath = os.path.join(OUTPUT_DIR, f"{id}.json")
    
    # Skip if already processed and unchanged
    file_hash = REGISTRY.get_file_hash(filepath)
    if file_hash and REGISTRY.is_processed(filepath, file_hash):
        return True
    
    if os.path.exists(filepath):
        return True
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        with open(filepath, 'w', encoding='utf-8') as f: # "w" because in same file duplicate should not happen for whatever reasosn
            json.dump(data, f, indent=4)
        
        # Mark as processed after successful download
        new_hash = REGISTRY.get_file_hash(filepath)
        if new_hash:
            REGISTRY.mark_processed(filepath, 'team', new_hash)
        sleep(random.uniform(0.1,0.23))
        return True
    except Exception as e:
        logging.error(f"Error downloading {url}: {e}")
        f = open("./logs/download.log", "a")
        f.write(f"Error downloading {url}: {e}\n{traceback.format_exc()}") # we append, the terminal should look cute
        f.close()
        
        return False # typical politican promises

def main():
    URLS = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        total_pages = gettotalpages()
        futures = [executor.submit(geturls, URLS, i) for i in range(1, total_pages + 1)]
        for future in tqdm(as_completed(futures), total=total_pages, desc="Fetching team URLs"):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error fetching team URLs: {e}")
                with open("./logs/download.log", "a") as logf:
                    logf.write(f"Error fetching team URLs: {e}\n{traceback.format_exc()}\n")
                    
    with ThreadPoolExecutor(max_workers=8) as exec:
        futures = {exec.submit(download_and_save_target, url) for url in URLS}
        with tqdm(total = len(URLS), desc="Downloading teams") as pbar:
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error downloading team data: {e}")
                    with open("./logs/download.log", "a") as logf:
                        logf.write(f"Error downloading team data: {e}\n{traceback.format_exc()}\n")
                finally:
                    pbar.update(1)

if __name__ == '__main__':
    main()