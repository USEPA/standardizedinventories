import pandas as pd
import requests
import json
import urllib

#SRS web service docs at https://cdxnodengn.epa.gov/cdx-srs-rest/
#Base URL for queries
base =  'https://cdxnodengn.epa.gov/cdx-srs-rest/'

#for querying more than 1 name at a time
#namelistprefix = 'substances/name?nameList='
#excludeSynonyms = '&excludeSynonyms=True'
sep='%7c' # This is the code for a pipe seperator required between CAS numbers

#Certain characters return errors or missing results but if replaces with '_' this work
#per advice from Tim Bazel (CGI Federal) on 6/27/2018
srs_replace_group = ['%2B','/','.']

#Return json object with SRS result
def query_SRS_by_substance_name(name):
    name_for_query = urllib.parse.quote(name)
    nameprefix = 'substance/name/'
    nameprefixexcludeSynonyms = '?excludeSynonyms=True'
    for i in srs_replace_group:
        name_for_query = name_for_query.replace(i, '_')
    url = base + nameprefix + name_for_query + nameprefixexcludeSynonyms
    flow_info = query_SRS(url)
    return flow_info

stewi_alt_ids = {"RCRAInfo":"10",
                 "TRI":"22",
                 "NEI":"20",
                 "DMR":"16"}

def query_SRS_by_alternate_id(id,inventory):
    alt_id_prefix = 'substance/alt_id/'
    alt_id_type_prefix = '/alt_id_type/'
    list_id = stewi_alt_ids[inventory]
    url = base + alt_id_prefix + id + alt_id_type_prefix + list_id
    flow_info = query_SRS(url)
    return flow_info

def query_SRS(url):
    try:
        chemicallistresponse = requests.get(url)
        chemicallistjson = json.loads(chemicallistresponse.text)
    except:
        return "Error:404"
    if len(chemicallistjson) == 0:
        return "Error: No SRS info found"
    else:
        flow_info = process_SRS_json_response(chemicallistjson)
        return flow_info


#Processes json response, returns a dataframe with key and CAS
def process_SRS_json_response(srs_json_response):
    chemical_srs_info = pd.DataFrame(columns=["SRS_ID", "SRS_CAS"])
    for r in range(0,len(srs_json_response)):
        chemical_srs_info.loc[r, "SRS_ID"] = srs_json_response[0]['subsKey']
        chemical_srs_info.loc[r, "SRS_CAS"] = srs_json_response[0]['currentCasNumber']
    return chemical_srs_info