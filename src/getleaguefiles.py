#=========================================================
# FILE DESCRIPTION : `getleaguefiles.py`
# 
# Downloads league metadata from ESPN API, fetches all league JSONs
# from paginated endpoints, and saves them locally with registry tracking.
#
# WRITTEN BY : RAJ 
# Last UPDATED : 2026-03-07
#=========================================================


# ===== IMPORTS =====
import argparse
import os
import json , time, random
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging, traceback
from tqdm import tqdm
from metaregis import MetadataRegistry
from bidmap import lidmap
# ===================


# ===== CONFIG =====
REGISTRY = MetadataRegistry()
BASE_URL  = "http://core.espnuk.org/v2/sports/cricket/leagues"
OUTPUT_DIR = "./data/rawdata/leaguejsons"  #\
MAX_PASSES = 3
MIN_REQUEST_INTERVAL = float(os.getenv("CRICK_MIN_REQUEST_INTERVAL", "0.05"))
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("./logs", exist_ok=True)
# ==================

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
            # Jittered backoff to avoid synchronized retry spikes.
            time.sleep((0.25 * (2 ** (attempt - 1))) + random.uniform(0.05, 0.35))
    with open("./logs/download.log", "a") as logf:
        logf.write(
            f"Failed after {MAX_PASSES} attempts for {url}: {last_error}\n{traceback.format_exc()}\n"
        )
    return None

def gettotalpages():
    data = _request_json_with_retry(BASE_URL, timeout=10)
    if not data:
        return 1
    return int(data.get("pageCount", 1))

def geturls(URLS,i):
    # if os.path.exists(f"{OUTPUT_DIR}/{i}.json"):
    #     return
    urssss = []
    url = f"{BASE_URL}?page={i}"
    data = _request_json_with_retry(url, timeout=10)
    if not data:
        return
    items = data.get("items", [])
    for item in items:
        ref = item.get("$ref")
        if ref:
            urssss.append(ref)
    URLS.extend(urssss)
    time.sleep(random.uniform(0.12,0.3))

def download_and_save_target(url):
    """Downloads the final JSON and saves it with proper directory handling."""
    id = url.rstrip('/').split('/')[-1].split('?')[0]
    id = lidmap(int(id))
    filepath = os.path.join(OUTPUT_DIR, f"{id}.json")
    
    # Skip if already processed and unchanged
    file_hash = REGISTRY.get_file_hash(filepath)
    if file_hash and REGISTRY.is_processed(filepath, file_hash):
        return True
    
    if os.path.exists(filepath):
        return True
    
    try:
        data = _request_json_with_retry(url, timeout=10)
        if not data:
            return False
        
        with open(filepath, 'w', encoding='utf-8') as f: # "w" because in same file duplicate should not happen for whatever reasosn
            json.dump(data, f, indent=4)
        time.sleep(random.uniform(0.1,0.24))
        # Mark as processed after successful download
        new_hash = REGISTRY.get_file_hash(filepath)
        if new_hash:
            REGISTRY.mark_processed(filepath, 'league', new_hash)
            
        return True
    except Exception as e:
        logging.error(f"Error downloading {url}: {e}")
        f = open("./logs/download.log", "a")
        f.write(f"Error downloading {url}: {e}\n{traceback.format_exc()}") # we append, the terminal should look cute
        f.close()
        
        return False # typical politican promises


def parse_args():
    parser = argparse.ArgumentParser(description="Download ESPN league metadata.")
    parser.add_argument(
        "--league-id",
        action="append",
        default=[],
        help="Only download specific ESPN league id(s). Can be repeated.",
    )
    return parser.parse_args()

def main():
    args = parse_args()
    if args.league_id:
        selected_urls = [f"{BASE_URL}/{lid}" for lid in args.league_id]
        with ThreadPoolExecutor(max_workers=8) as exec:
            futures = {exec.submit(download_and_save_target, url) for url in selected_urls}
            with tqdm(total=len(selected_urls), desc="Downloading selected leagues") as pbar:
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error downloading selected league data: {e}")
                    finally:
                        pbar.update(1)
        return

    URLS= []
    with ThreadPoolExecutor(max_workers=8) as executor:
        total_pages = gettotalpages()
        futures = [executor.submit(geturls, URLS, i) for i in range(1, total_pages + 1)]
        for future in tqdm(as_completed(futures), total=total_pages, desc="Fetching league URLs"):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error fetching league URLs: {e}")
                with open("./logs/download.log", "a") as logf:
                    logf.write(f"Error fetching league URLs: {e}\n{traceback.format_exc()}\n")
                    
    with ThreadPoolExecutor(max_workers=8) as exec:
        futures = {exec.submit(download_and_save_target, url) for url in URLS}
        with tqdm(total = len(URLS), desc="Downloading leagues") as pbar:
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error downloading league data: {e}")
                    with open("./logs/download.log", "a") as logf:
                        logf.write(f"Error downloading league data: {e}\n{traceback.format_exc()}\n")
                finally:
                    pbar.update(1)

if __name__ == '__main__':
    main()