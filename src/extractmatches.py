###############################################################
# FILE    : `extractmatches.py`
# This extracts match and delivery data with full schema support
# WRITTEN BY : RAJ
# LAST UPDATED : 2026-02-22
#  =========================================================
import json
import os
import glob, traceback
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from metaregis import MetadataRegistry

REGISTRY = MetadataRegistry()
MATCHES_DIR = "./data/rawdata/matches/WODI"
STAGED_DELIVERIES = "./data/stageddata/deliveries/WODI"
STAGED_MATCHES = "./data/stageddata/matches/WODI"

def process_match_file(filepath):
    file_hash = REGISTRY.get_file_hash(filepath)
    if not file_hash or REGISTRY.is_processed(filepath, file_hash):
        with open(f"./logs/skippedmatches.log", "a") as logf:
            logf.write(f"{filepath} skipped (already processed or hash error)\n")
            # print(f"Skipped {filepath} (already processed or hash error)")
        return None

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        info = data.get('info', {})
        innings = data.get('innings', [])
        match_id = os.path.basename(filepath).split('.')[0]
        
        if not innings or info.get('match_type') not in ('T20', 'ODI'):
            REGISTRY.mark_processed(filepath, 'skipped', file_hash)
            return None

        people = info.get('registry', {}).get('people', {})
        def get_p(name): return people.get(name, name) if name else None

        # --- 1. Match Metadata ---
        outcome = info.get('outcome', {})
        by = outcome.get('by', {})
        toss = info.get('toss', {})
        
        # Calculate team scores/wickets for the match record
        team_stats = {}
        for inn in innings:
            t_name = inn.get('team')
            t_runs = sum(d.get('runs', {}).get('total', 0) for o in inn.get('overs', []) for d in o.get('deliveries', []))
            t_wicks = sum(len(d.get('wickets', [])) for o in inn.get('overs', []) for d in o.get('deliveries', []))
            t_balls = sum(1 for o in inn.get('overs', []) for d in o.get('deliveries', []) if not d.get('extras', {}).get('wides') and not d.get('extras', {}).get('noballs'))
            team_stats[t_name] = {'runs': t_runs, 'wickets': t_wicks, 'balls': t_balls}

        teams = info.get('teams', [None, None])
        
        match_record = {
            'matchid': match_id,
            'season': info.get('season'),
            'date': info.get('dates', [None])[0],
            'city': info.get('city'),
            'venue': info.get('venue'),
            'event': info.get('event', {}).get('name'),
            'gender': 'M' if info.get('gender') == 'male' else 'F',
            'type': info.get('match_type'),
            'overs': info.get('overs'),
            'team1id': teams[0],
            'team2id': teams[1],
            'tosswin': toss.get('winner'),
            'decision': toss.get('decision'),
            'referee': get_p(info.get('officials', {}).get('match_referees', [None])[0]),
            'umpire1': get_p(info.get('officials', {}).get('umpires', [None, None])[0]),
            'umpire2': get_p(info.get('officials', {}).get('umpires', [None, None])[1]),
            'tvumpire': get_p(info.get('officials', {}).get('tv_umpires', [None])[0]),
            'isTie': 1 if outcome.get('result') == 'tie' else 0,
            'winner': outcome.get('winner'),
            'byRuns': by.get('runs', 0),
            'byWickets': by.get('wickets', 0),
            'team1score': team_stats.get(teams[0], {}).get('runs', 0),
            'team2score': team_stats.get(teams[1], {}).get('runs', 0),
            'team1wickets': team_stats.get(teams[0], {}).get('wickets', 0),
            'team2wickets': team_stats.get(teams[1], {}).get('wickets', 0),
            'team1balls': team_stats.get(teams[0], {}).get('balls', 0),
            'team2balls': team_stats.get(teams[1], {}).get('balls', 0),
            'isDLS': 1 if outcome.get('method') == 'D/L' else 0,
            'POM': get_p(info.get('player_of_match', [None])[0])
        }

        # --- 2. Deliveries Flattening 
        deliveries_list = []
        target_runs = None

        for inn_idx, inn_data in enumerate(innings):
            if inn_data.get('super_over'): continue
            
            batting_team = inn_data.get('team')
            # Reset counters for new innings
            curr_runs, curr_wicks, total_legal_balls = 0, 0, 0
            total_extras = 0
            last_boundary_balls, lwballs = 0, 0
            
            # Player-specific trackers
            pstats = {} # {id: {'runs': 0, 'balls': 0, 'wicks': 0, 'bowled_runs': 0, 'bowled_balls': 0}}
            
            def get_stat(pid):
                if pid not in pstats:
                    pstats[pid] = {'runs': 0, 'balls': 0, 'wicks': 0, 'bowled_runs': 0, 'bowled_balls': 0}
                return pstats[pid]

            for over_data in inn_data.get('overs', []):
                over_num = over_data.get('over')
                
                for ball_idx, d in enumerate(over_data.get('deliveries', []), 1):
                    runs = d.get('runs', {})
                    extras = d.get('extras', {})
                    wickets = d.get('wickets', [])
                    
                    b_id, bowl_id, ns_id = get_p(d.get('batter')), get_p(d.get('bowler')), get_p(d.get('non_striker'))
                    
                    # Logic for ball legality
                    is_wide = extras.get('wides', 0)
                    is_nb = 1 if 'noballs' in extras else 0
                    is_legal = 1 if not (is_wide or is_nb) else 0
                    
                    # Updates
                    curr_runs += runs.get('total', 0)
                    total_extras += runs.get('extras', 0)
                    if is_legal: 
                        total_legal_balls += 1
                        last_boundary_balls += 1
                        lwballs += 1
                    
                    # Batter stats
                    b_stat = get_stat(b_id)
                    b_stat['runs'] += runs.get('batter', 0)
                    if is_legal or is_nb: b_stat['balls'] += 1
                    
                    # Bowler stats
                    bowl_stat = get_stat(bowl_id)
                    bowl_stat['bowled_runs'] += (runs.get('total', 0) - extras.get('byes', 0) - extras.get('legbyes', 0))
                    if is_legal: bowl_stat['bowled_balls'] += 1
                    
                    # Boundary / Wicket tracking
                    is_boundary = 1 if runs.get('batter', 0) >= 4 else 0
                    if is_boundary: last_boundary_balls = 0
                    
                    if wickets:
                        curr_wicks += len(wickets)
                        lwballs = 0
                        for w in wickets:
                            if w.get('kind') not in ['run out', 'retired out', 'obstructing the field']:
                                bowl_stat['wicks'] += 1

                    # Phase Calculation (Simplified)
                    if info.get('match_type') == 'ODI':
                        phase = "Powerplay" if over_num < 10 else ("Middle" if over_num < 40 else "Death")
                    else:
                        phase = "Powerplay" if over_num < 6 else ("Middle" if over_num < 15 else "Death")

                    deliveries_list.append({
                        'matchid': match_id,
                        'inning': inn_idx,
                        'gender': match_record['gender'],
                        'battingteam': batting_team,
                        'over_num': over_num,
                        'ball_num': ball_idx,
                        'phase': phase,
                        'batter_id': b_id,
                        'bowler_id': bowl_id,
                        'non_striker_id': ns_id,
                        'runs_batter_thisball': runs.get('batter', 0),
                        'runs_extras_thisball': runs.get('extras', 0),
                        'runs_total_thisball': runs.get('total', 0),
                        'iswide': is_wide,
                        'isNoball': is_nb,
                        'is_boundary': is_boundary,
                        'is_dot': 1 if runs.get('total', 0) == 0 else 0,
                        'is_legal_ball': is_legal,
                        'is_wicket': 1 if wickets else 0,
                        'wicket_type': wickets[0].get('kind') if wickets else None,
                        'player_out_id': get_p(wickets[0].get('player_out')) if wickets else None,
                        'fielderct': get_p(wickets[0].get('fielders', [{}])[0].get('name')) if wickets and wickets[0].get('fielders') else None,
                        'currentruns': curr_runs,
                        'currentwickets': curr_wicks,
                        'targetruns': target_runs,
                        'current_runrate': round((curr_runs / (total_legal_balls / 6)) if total_legal_balls > 0 else 0, 2),
                        'required_runrate': round(((target_runs - curr_runs) / ((match_record['overs']*6 - total_legal_balls) / 6)) if target_runs and (match_record['overs']*6 - total_legal_balls) > 0 else 0, 2),
                        'lastboundary': last_boundary_balls,
                        'lastwicket': lwballs,
                        'totalextras': total_extras,
                        'currentstrikerruns': b_stat['runs'],
                        'currentstrikerballs': b_stat['balls'],
                        'currentnstrikerruns': get_stat(ns_id)['runs'],
                        'currentnstrikerballs': get_stat(ns_id)['balls'],
                        'currentbowlerwickets': bowl_stat['wicks'],
                        'currentbowlerruns': bowl_stat['bowled_runs'],
                        'currentbowlerballs': bowl_stat['bowled_balls']
                    })

            if inn_idx == 0:
                target_runs = curr_runs + 1

        REGISTRY.mark_processed(filepath, 'match', file_hash)
        return match_record, deliveries_list

    except Exception as e:
        print(f"Error flattening {filepath}: {e}")
        with open(f"./logs/error_matches.log", "a") as logf:
            logf.write(f"{filepath} error: {e}\n Traceback: {traceback.format_exc()}\n\n")
        return None

def main():
    os.makedirs(STAGED_DELIVERIES, exist_ok=True)
    os.makedirs(STAGED_MATCHES, exist_ok=True)
    
    match_files = glob.glob(f"{MATCHES_DIR}/*.json")
    
    batch_matches, batch_deliveries = [], []
    batch_counter = 0
    df = pd.DataFrame(batch_matches)
    if 'season' in df.columns:
         df['season'] = df['season'].astype(str)
    
    with ProcessPoolExecutor(max_workers=20) as executor:
        for result in executor.map(process_match_file, match_files):
            if result:
                m_rec, d_list = result
                batch_matches.append(m_rec)
                batch_deliveries.extend(d_list)
                
                if len(batch_matches) >= 500:
                    df.to_parquet(f"{STAGED_MATCHES}/chunk_{batch_counter}.parquet", index=False)
                    df.to_parquet(f"{STAGED_DELIVERIES}/chunk_{batch_counter}.parquet", index=False)
                    batch_matches, batch_deliveries = [], []
                    batch_counter += 1
                    print(f"Dumped chunk {batch_counter}...", end='\r')

    if batch_matches:
        df.to_parquet(f"{STAGED_MATCHES}/chunk_{batch_counter}.parquet", index=False)
        df.to_parquet(f"{STAGED_DELIVERIES}/chunk_{batch_counter}.parquet", index=False)

if __name__ == '__main__':
    main()