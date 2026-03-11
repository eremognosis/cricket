###############################################################
# FILE    : `extractmatches.py`
# This extracts match and delivery data with full schema support
# WRITTEN BY : RAJ
# LAST UPDATED : 2026-02-23
#  =========================================================
import json
import os
import glob, traceback
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from metaregis import MetadataRegistry
from bidmap import midmap, idmap
REGISTRY = MetadataRegistry()
SELECT_DATA = 'WT20I'
# 
# as of today, i will change a bit

# 
# 

MATCHES_DIR = f"./data/rawdata/matches/{SELECT_DATA}"
STAGED_DELIVERIES = f"./data/stageddata/deliveries/{SELECT_DATA}"
STAGED_MATCHES = f"./data/stageddata/matches/{SELECT_DATA}"
STAGED_PEOPLE = f"./data/stageddata/peoplematchdata/{SELECT_DATA}"
TEAM_DATA = "./data/stageddata/teams.parquet"
PLAYERMAP = "./data/stageddata/playeridmap.parquet"

os.makedirs("./logs", exist_ok=True)
os.makedirs(STAGED_DELIVERIES, exist_ok=True)
os.makedirs(STAGED_MATCHES, exist_ok=True)
os.makedirs(STAGED_PEOPLE, exist_ok=True)
teamregistry = pd.read_parquet(TEAM_DATA).set_index('name')['team_id'].to_dict() # we will need this to map team names to ids in the match records, since the deliveries only have team names and we want to link them to ids for easier analysis later
# for 23 minutes i was uccking up what the hell then realized that i was mapping name to id and not other way round, fuck dictionary
pregistry = pd.read_parquet(PLAYERMAP).set_index("id")["idnew"].to_dict()




def getteamid(name):
    return teamregistry.get(name, name) if name else None

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
        match_id = midmap(int(match_id))
        
        if not innings or info.get('match_type') not in ('T20', 'ODI'):
            REGISTRY.mark_processed(filepath, 'skipped', file_hash)
            return None

        people = info.get('registry', {}).get('people', {})
        def get_p(name): return pregistry.get(people.get(name, name),name) if name else None
        peoplmatches =[]
        for k,v in people.items():

            peoplmatches.append({
                'matchid': match_id,
                'personid': v})
        
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
        ev = ""
        x = (info.get('event', {}).get('stage', '') + 'Match,') if info.get('event', {}).get('stage') else ('Match ' + str(info.get('event', {}).get('match_number', '')) + ',')
        if info.get('event'):
            ev = x
            ev += info.get('event', {}).get('name', '')
            # ev = info.get('event', {}).get('name', '')
        match_record = {
            'matchid': match_id,
            'season': info.get('season'),
            'date': info.get('dates', [None])[0],
            'city': info.get('city'),
            'venue': info.get('venue'),
            'event': ev,
            'tourmatch' : x,
            'gender': 'M' if info.get('gender') == 'male' else 'F',
            'type': info.get('match_type'),
            'overs': info.get('overs'),
            'team1id': getteamid(teams[0]),
            'team2id': getteamid(teams[1]),
            'tosswin': getteamid(toss.get('winner')),
            'decision': toss.get('decision'),
            'referee': get_p(info.get('officials', {}).get('match_referees', [None])[0]),
            'umpire1': get_p(info.get('officials', {}).get('umpires', [None, None])[0]) if len(info.get('officials', {}).get('umpires', [])) else None,
            'umpire2': get_p(info.get('officials', {}).get('umpires', [None, None])[1]) if len(info.get('officials', {}).get('umpires', [])) > 1 else None,
            'tvumpire': get_p(info.get('officials', {}).get('tv_umpires', [None])[0]),
            'isTie': 1 if outcome.get('result') == 'tie' else 0,
            'winner': getteamid(outcome.get('winner')),
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
                    review = d.get('review', {})
                    
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


                    if info.get('match_type') == 'ODI':
                        phase = "Powerplay" if over_num < 10 else ("Middle" if over_num < 40 else "Death")
                    else:
                        phase = "Powerplay" if over_num < 6 else ("Middle" if over_num < 15 else "Death")

                    deliveries_list.append({
                        'matchid': match_id,
                        'inning': inn_idx,  # 0 indexed
                        # 'gender': match_record['gender'],
                        'battingteam': getteamid(batting_team), # for now we keep team name here, can map to id later...
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
                        'isreview': 1 if review else 0,
                        'review_by': getteamid(review.get('by')) if review else None,
                        'review_umpire': get_p(review.get('umpire')) if review else None,
                        'review_batter_id': get_p(review.get('batter')) if review else None,
                        'review_bowler_id': get_p(review.get('bowler')) if review else None,
                        'review_decision': review.get('decision') if review else None,
                        'review_type': review.get('type') if review else None
                        # 'currentruns': curr_runs,
                        # 'currentwickets': curr_wicks,
                        # 'targetruns': target_runs,
                        # 'current_runrate': round((curr_runs / (total_legal_balls / 6)) if total_legal_balls > 0 else 0, 2),
                        # 'required_runrate': round(((target_runs - curr_runs) / ((match_record['overs']*6 - total_legal_balls) / 6)) if target_runs and (match_record['overs']*6 - total_legal_balls) > 0 else 0, 2),
                        # 'lastboundary': last_boundary_balls,
                        # 'lastwicket': lwballs,
                        # 'totalextras': total_extras,
                        # 'currentstrikerruns': b_stat['runs'],
                        # 'currentstrikerballs': b_stat['balls'],
                        # 'currentnstrikerruns': get_stat(ns_id)['runs'],
                        # 'currentnstrikerballs': get_stat(ns_id)['balls'],
                        # 'currentbowlerwickets': bowl_stat['wicks'],
                        # 'currentbowlerruns': bowl_stat['bowled_runs'],
                        # 'currentbowlerballs': bowl_stat['bowled_balls']
                    })

            if inn_idx == 0:
                target_runs = curr_runs + 1

        REGISTRY.mark_processed(filepath, 'match', file_hash)
        return match_record, deliveries_list, peoplmatches

    except Exception as e:
        print(f"Error flattening {filepath}: {e}", end='\r')
        with open(f"./logs/error_matches.log", "a") as logf:
            logf.write(f"{filepath} error: {e}\n Traceback: {traceback.format_exc()}\n\n")
        return None

def main():
    os.makedirs(STAGED_DELIVERIES, exist_ok=True)
    os.makedirs(STAGED_MATCHES, exist_ok=True)
    os.makedirs(STAGED_PEOPLE, exist_ok=True)
    match_files = glob.glob(f"{MATCHES_DIR}/*.json")
    
    batch_matches, batch_deliveries,batch_ppl = [], [],[]
    batch_counter = 0
    df = pd.DataFrame(batch_matches)
    if 'season' in df.columns:
         df['season'] = df['season'].astype(str)
         
    if 'tourmatch' in df.columns:
            df['tourmatch'] = df['tourmatch'].astype(str)
    try:
        with ProcessPoolExecutor(max_workers=80) as executor:
            for result in executor.map(process_match_file, match_files):
                if result:
                    m_rec, d_list, p_list = result
                    batch_matches.append(m_rec)
                    batch_deliveries.extend(d_list)
                    batch_ppl.extend(p_list)
                    
                    if len(batch_matches) >= 500:
                        df = pd.DataFrame(batch_matches)
                        if 'season' in df.columns:
                            df['season'] = df['season'].astype(str)
                        if 'tourmatch' in df.columns:
                                df['tourmatch'] = df['tourmatch'].astype(str)
                        df_deliveries = pd.DataFrame(batch_deliveries)
                        df.to_parquet(f"{STAGED_MATCHES}/chunk_{batch_counter}.parquet", index=False)
                        df_deliveries.to_parquet(f"{STAGED_DELIVERIES}/chunk_{batch_counter}.parquet", index=False)
                        df_ppl = pd.DataFrame(batch_ppl)
                        df_ppl.to_parquet(f"{STAGED_PEOPLE}/chunk_{batch_counter}.parquet", index=False)
                        batch_matches, batch_deliveries, batch_ppl = [], [], []
                        batch_counter += 1
                        print(f"Dumped chunk {batch_counter}...", end='\r')
    except Exception as e:
        print(f"Error during parallel processing: {e}\n{traceback.format_exc()}")
        # logf.write()
        with open(f"./logs/error_matches.log", "a") as logf:
            logf.write(f"Error during parallel processing: {e}\n{traceback.format_exc()}\n\n")

    if batch_matches:
        df = pd.DataFrame(batch_matches)
        if 'season' in df.columns:
            df['season'] = df['season'].astype(str)
        if 'tourmatch' in df.columns:
            df['tourmatch'] = df['tourmatch'].astype(str)
        df_deliveries = pd.DataFrame(batch_deliveries)
        df_ppl = pd.DataFrame(batch_ppl)
        df.to_parquet(f"{STAGED_MATCHES}/chunk_{batch_counter}.parquet", index=False)
        df_deliveries.to_parquet(f"{STAGED_DELIVERIES}/chunk_{batch_counter}.parquet", index=False)
        df_ppl.to_parquet(f"{STAGED_PEOPLE}/chunk_{batch_counter}.parquet", index=False)

if __name__ == '__main__':
    main()