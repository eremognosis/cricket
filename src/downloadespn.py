#=========================================================
# FILE DESCRIPTION : `downloadespn.py`
# 
# This script is supposed to downalod ESPN season data (open source mostly...) for a league, which is doanloaded
# It reads local JSON files in the INPUT_DIR, extracts paginated base URLs, in the standard epsn format
# fetches each page of data, and downloads the final JSONs for each season.
# The downloaded JSONs are saved in OUTPUT_DIR under subdirectories named by their leagueids and year.json
# The code is slightly unhinged, held together by spite, because anyway we are doing it once 
#
# WRITTEN BY : RAJ 
# Last UPDATED : 2026-02-22
#==========================================================







import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging, traceback
from tqdm import tqdm







# ==========================================
# CONFIG & PLACEHOLDERS 
# ==========================================
INPUT_DIR = "./data/rawdata/leaguejsons"
OUTPUT_DIR = "./data/rawdata/seasons"  # This will be the base directory for all season JSON, coz we fucked up in last project had to ovewrite and eventually fucked aorund and find outted (scientific method)

# JSON Keys
innkey = "seasons"
REF_KEY = "$ref"
ITEMS_KEY = "items"
ITEM_REF_KEY = "$ref"
TARGET_ID_KEY = "id"
pc_KEY = "pageCount"

MAX_WORKERS = 80   # We are good

os.makedirs(OUTPUT_DIR, exist_ok=True)  # agfain its PTSD from last run

def fetch_page_data(url):
    """Fetches a specific pagination URL and returns its items and the page count."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        items = data.get(ITEMS_KEY, [])
        turl = [item[ITEM_REF_KEY] for item in items if ITEM_REF_KEY in item]
        pc = data.get(pc_KEY, 1)
        
        return turl, pc
    except Exception as e:
        return [], 0

def download_and_save_target(url):
    """Downloads the final JSON and saves it with proper directory handling."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        doc_id = data.get(TARGET_ID_KEY)
        year = data.get('year', 'u') # u is unknown year, just in case or if modi decides to change year from gregorian to something else like modiabd
        
        if not doc_id:  # if no name, no game
            return False

        target_dir = os.path.join(OUTPUT_DIR, str(doc_id)) # PTSD from last time, we fucked up and saved all seasons of a league in one folder, so now we have to save each season in its own folder named by the leagueid, and then the file is year.json inside it. I know its cringe but we are doing it for safety and sanity and redundancy and unemployement and caffeine and drunkedness whatevrjtg.
        os.makedirs(target_dir, exist_ok=True) # THIS IS THE FIX (imagine the level of pride after doing an obvious shit)
        if os.path.exists(os.path.join(target_dir, f"{year}.json")):
            return True  # Sher ek kaam do bar nahi karta
        filepath = os.path.join(target_dir, f"{year}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f: # "w" because in same file duplicate should not happen for whatever reasosn
            json.dump(data, f, indent=4)
            
        return True
    except Exception as e:
        # logging.error(f"Error downloading {url}: {e}")
        f = open("./logs/download.log", "a")
        f.write(f"Error downloading {url}: {e}\n{traceback.format_exc()}") # we append, the terminal should look cute
        f.close()
        
        return False # typical politican promises


def main():
    if not os.path.exists(INPUT_DIR):
        print(f" '{INPUT_DIR}' is a void and void stared back to throw error")
        return

    pbburl = []
    
    # 1. parse
    print("[*] Parsing local JSONs...")
    for filename in os.listdir(INPUT_DIR):
        if filename.endswith('.json'): # only json files and not contact cards that you wont ever dial but ....wanted to
            filepath = os.path.join(INPUT_DIR, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    llddd = json.load(f)
                    if innkey in llddd and REF_KEY in llddd[innkey]:
                        pbburl.append(llddd[innkey][REF_KEY])
            except Exception:
                f = open("./logs/parsing.log", "a")
                f.write(f"Error parsing {filepath}: {traceback.format_exc()}\n")
                f.close()

    if not pbburl:
        print("No base URLs found Lol")
        return

    print(f" {len(pbburl)} urls")

    all_turl = []
    pending_page_urls = []

    # 2.cocunts and urls
    print("\n[*] Fetching Page 1s to determine pagination limits...")
    page_1_urls = [f"{base_url}?page=1" for base_url in pbburl]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_page_data, url): base_url for url, base_url in zip(page_1_urls, pbburl)}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Page 1s", unit="req"):
            base_url = futures[future]
            urls, pc = future.result()
            all_turl.extend(urls)
            
            # queuquqe
            if pc > 1:
                for p in range(2, pc + 1):
                    pending_page_urls.append(f"{base_url}?page={p}") # wait, coz you are the "third man"

    # 3. remaining
    if pending_page_urls:
        print(f"\nFetching {len(pending_page_urls)} pages cases")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_page_data, url) for url in pending_page_urls]
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Pagination", unit="req"):
                urls, _ = future.result()  # We don't care about pc anymore
                all_turl.extend(urls)

    # the api is fucked up, espn means erapalli shankara prasanna nandakishore, coz thats how much conservative nonsense it is
    all_turl = list(set(all_turl))
    print(f"\n[*] Extracted a total of {len(all_turl)} unique target URLs to download.")

    # 4. Download the actual targets with a cute progress bar  (I AI generated that cute bar...i am too lazy to write i mean, i dont know so)
    if not all_turl:
        print("..")
        return

    print("\n[*] Downloading final JSONs...")
    success_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_and_save_target, url) for url in all_turl]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading", unit="files"):
            if future.result():
                success_count += 1

    print(f"\n[+] Job's done. Downloaded {success_count}/{len(all_turl)} files. Go Fuck Yourself")

if __name__ == "__main__":
    main()
