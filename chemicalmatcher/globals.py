"""Supporting variables and functions used in stewi."""
import pandas as pd
import requests
import json
import urllib
from pathlib import Path

from stewi.globals import config, log

MODULEPATH = Path(__file__).resolve().parent
DATA_PATH = MODULEPATH / 'data'
OUTPUT_PATH = MODULEPATH / 'output'

SRSconfig = config(config_path=MODULEPATH)['databases']['SRS']
base = SRSconfig['url']

# Certain characters return errors or missing results but if replaces
# with '_' this work per advice from Tim Bazel (CGI Federal) on 6/27/2018
srs_replace_group = ['%2B', '/', '.']

inventory_to_SRSlist_acronymns = SRSconfig['inventory_lists']


# Return json object with SRS result
def get_SRSInfo_for_substance_name(name):
    name_for_query = urllib.parse.quote(name)
    nameprefix = 'substance/name/'
    nameprefixexcludeSynonyms = '?excludeSynonyms=True'
    for i in srs_replace_group:
        name_for_query = name_for_query.replace(i, '_')
    url = base + nameprefix + name_for_query + nameprefixexcludeSynonyms
    flow_info = query_SRS_for_flow(url)
    return flow_info


def get_SRSInfo_for_program_list(inventory):
    # See all lists
    # https://cdxnodengn.epa.gov/cdx-srs-rest/reference/substance_lists
    # Base URL for queries
    substancesbylistname = 'substances/list_acronym/'
    srs_flow_df = pd.DataFrame()
    for listname in inventory_to_SRSlist_acronymns[inventory]:
        log.debug('Getting %s', listname)
        lists_of_interest = obtain_list_names(listname)
        url = base + substancesbylistname + urllib.parse.quote(listname)
        flow_info = query_SRS_for_program_list(url, inventory,
                                               lists_of_interest)
        if len(flow_info) == 0:
            log.info(f'No flows found for {listname}')
        srs_flow_df = pd.concat([srs_flow_df, flow_info])
    srs_flow_df.drop_duplicates(inplace=True)
    if(inventory == 'TRI'):
        srs_flow_df['PGM_ID'] = srs_flow_df['PGM_ID'].apply(
            lambda x: str(x).lstrip('0'))
    srs_flow_df.sort_values(by='PGM_ID', inplace=True)
    return srs_flow_df


def obtain_list_names(acronym):
    url = base + 'reference/substance_lists'
    with urllib.request.urlopen(url) as j:
        data = json.loads(j.read().decode())
    names = [d['substanceListName'] for d in data if d['substanceListAcronym'] == acronym]
    return names


# Returns a df
def query_SRS_for_program_list(url, inventory, lists_of_interest):
    try:
        chemicallistresponse = requests.get(url)
        chemicallistjson = json.loads(chemicallistresponse.text)
    except:
        return "Error:404"
    all_chemicals_list = []
    for chemical in chemicallistjson:
        # get cas
        chemicaldict = {}
        chemicaldict['SRS_CAS'] = chemical['currentCasNumber']
        chemicaldict['SRS_ID'] = chemical['subsKey']
        # get synonyms
        # extract from the json
        synonyms = chemical['synonyms']
        # ids are deeply embedded in this list. Go get ids relevant to these lists of interest
        alternateids = []
        for i in synonyms:
            if i['listName'] in lists_of_interest:
                for l in i['alternateIds']:
                    alternateids.append(l['alternateId'])

        # make list of alt ids unique by converting to a set, then back to a list
        alternateids = list(set(alternateids))

        if len(alternateids) > 0:
            for id in range(0, len(alternateids)):
                chemicaldict['PGM_ID'] = alternateids[id]
                all_chemicals_list.append(chemicaldict.copy())
        else:
            all_chemicals_list.append(chemicaldict)

    all_inventory_chemicals = pd.DataFrame(all_chemicals_list)
    return all_inventory_chemicals


def query_SRS_for_flow(url, for_single_flow=False):
    try:
        chemicallistresponse = requests.get(url)
        chemicallistjson = json.loads(chemicallistresponse.text)
    except:
        return "Error:404"
    if len(chemicallistjson) == 0:
        return "Error: No SRS info found"
    else:
        flow_info = process_single_SRS_json_response(chemicallistjson)
        return flow_info


# Processes json response, returns a dataframe with key and CAS
def process_single_SRS_json_response(srs_json_response):
    chemical_srs_info = pd.DataFrame(columns=["SRS_ID", "SRS_CAS"])
    chemical_srs_info.loc[0, "SRS_ID"] = srs_json_response[0]['subsKey']
    chemical_srs_info.loc[0, "SRS_CAS"] = srs_json_response[0]['currentCasNumber']
    return chemical_srs_info


def add_manual_matches(df_matches, include_proxies=True):
    manual_matches = pd.read_csv(DATA_PATH.joinpath('chemicalmatches_manual.csv'),
                                 header=0,
                                 dtype={'FlowID': 'str', 'SRS_ID': 'str'})
    if not include_proxies:
        manual_matches = manual_matches[manual_matches['Proxy_Used'] == 0]
    manual_matches = manual_matches.drop(columns=['Proxy_Used', 'Proxy_Name',
                                                  'FlowName'])
    manual_matches = manual_matches.rename(columns={'SRS_ID': 'SRS_ID_Manual'})
    df_matches = pd.merge(df_matches, manual_matches,
                          on=['FlowID', 'Source'], how='left')
    # Set null SRS_IDs to those manually found. Replaces null with null if not
    df_matches.loc[df_matches['SRS_ID'].isnull(), 'SRS_ID'] = df_matches['SRS_ID_Manual']
    df_matches = df_matches.drop(columns=['SRS_ID_Manual'])
    return df_matches


def read_cm_file(file='match'):
    if file == 'match':
        name = 'ChemicalsByInventorywithSRS_IDS_forStEWI.csv'
    elif file == 'missing':
        name = 'flows_missing_SRS_ID.csv'
    df = pd.read_csv(OUTPUT_PATH.joinpath(name),
                     dtype={"SRS_ID": "str"})
    return df
