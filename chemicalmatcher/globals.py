import os, sys, yaml
import pandas as pd
import requests
import json
import urllib

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'chemicalmatcher/'

output_dir = modulepath + 'output/'
data_dir = modulepath + 'data/'

#Base URL for queries
def config():
    configfile = None
    with open(modulepath + 'config.yaml', mode='r') as f:
        configfile = yaml.load(f,Loader=yaml.FullLoader)
    return configfile

base  = config()['databases']['SRS']['url']

#for querying more than 1 name at a time
#namelistprefix = 'substances/name?nameList='
#excludeSynonyms = '&excludeSynonyms=True'
sep='%7c' # This is the code for a pipe seperator required between CAS numbers

#Certain characters return errors or missing results but if replaces with '_' this work
#per advice from Tim Bazel (CGI Federal) on 6/27/2018
srs_replace_group = ['%2B','/','.']

#Return json object with SRS result
def get_SRSInfo_for_substance_name(name):
    name_for_query = urllib.parse.quote(name)
    nameprefix = 'substance/name/'
    nameprefixexcludeSynonyms = '?excludeSynonyms=True'
    for i in srs_replace_group:
        name_for_query = name_for_query.replace(i, '_')
    url = base + nameprefix + name_for_query + nameprefixexcludeSynonyms
    flow_info = query_SRS_for_flow(url)
    return flow_info

stewi_alt_ids = {"RCRAInfo":"10",
                 "TRI":"22",
                 "NEI":"20",
                 "DMR":"16"}

inventory_to_SRSlist_acronymns = {"RCRAInfo":['RCRA F Waste','RCRA U Waste','RCRA F Waste','RCRA P Waste','RCRA T Char'],
                                "TRI":["TRIPS"],
                                "NEI":["EIS"],
                                "DMR":["PCR"]}


def get_SRSInfo_for_alternate_id(id,inventory):
    alt_id_prefix = 'substance/alt_id/'
    alt_id_type_prefix = '/alt_id_type/'
    list_id = stewi_alt_ids[inventory]
    url = base + alt_id_prefix + id + alt_id_type_prefix + list_id
    flow_info = query_SRS_for_flow(url)
    return flow_info

def get_SRSInfo_for_program_list(inventory):
    # See all lists
    # https://cdxnodengn.epa.gov/cdx-srs-rest/reference/substance_lists
    # Base URL for queries
    substancesbylistname = 'substances/list_acronym/'
    srs_flow_df = pd.DataFrame()
    for listname in inventory_to_SRSlist_acronymns[inventory]:
        listname = urllib.parse.quote(listname)
        url = base + substancesbylistname + listname
        print('Getting '+ listname)
        flow_info = query_SRS_for_program_list(url,inventory)
        srs_flow_df = pd.concat([srs_flow_df,flow_info])
    #drop duplicates
    srs_flow_df.drop_duplicates(inplace=True)
    #sort by alt_id
    srs_flow_df.sort_values(by='PGM_ID',inplace=True)
    return srs_flow_df


#SRS list names for inventories of interest

inventory_to_SRSlist = {"RCRAInfo":['Characteristics of Hazardous Waste: Toxicity Characteristic',
                               'Hazardous Wastes From Non-Specific Sources',
                               'Hazardous Wastes From Specific Sources',
                               'Acutely Hazardous Discarded Commercial Chemical Products',
                               'Hazardous Discarded Commercial Chemical Products'],
                        "NEI": ['Emissions Inventory System'],
                        "TRI": ['Toxics Release Inventory Program System']}
#Two other rcra lists are 'Hazardous Constituents', and 'Basis for Listing Hazardous Waste'
#RCRA_wastecodegroup_to_listname: {'D':'Characteristics of Hazardous Waste: Toxicity Characteristic',
##                               'F':'Hazardous Wastes From Non-Specific Sources',
#                               'K':'Hazardous Wastes From Specific Sources',
#                               'P':'Acutely Hazardous Discarded Commercial Chemical Products',
#                               'U':'Hazardous Discarded Commercial Chemical Products'}

#Returns a df
def query_SRS_for_program_list(url, inventory):
    try:
        chemicallistresponse = requests.get(url)
        chemicallistjson = json.loads(chemicallistresponse.text)
    except:
        return "Error:404"
    all_chemicals_list = []
    for chemical in chemicallistjson:
       #get cas
       chemicaldict = {}
       chemicaldict['SRS_CAS'] = chemical['currentCasNumber']
       chemicaldict['SRS_ID'] = chemical['subsKey']
       #get synonyms
       #extract from the json
       synonyms = chemical['synonyms']
       lists_of_interest = inventory_to_SRSlist[inventory]
       #ids are deeply embedded in this list. Go get ids relevant to these lists of interest
       alternateids = []
       for i in synonyms:
           if i['listName'] in lists_of_interest:
               # print('True for' + i['listName'])
               #chem_alt_id_info = {}
               for l in i['alternateIds']:
                   #chem_alt_id_info['alternateId'] = l['alternateId']
                   #chem_alt_id_info['alternateIdTypeName']
                   alternateids.append(l['alternateId'])

       #make list of alt ids unique by converting to a set, then back to a list
       alternateids = list(set(alternateids))

       #id_no = 1
       #for id in alternateids:
       #    chemicaldict['Alt_ID'+str(id_no)] = id
       #    if_no=id_no+1
       if len(alternateids) > 0:
        chemicaldict['PGM_ID'] = alternateids[0]
       # Just use first alternate id for now as a test
       all_chemicals_list.append(chemicaldict)
    #Write it into a df
    all_inventory_chemicals = pd.DataFrame(all_chemicals_list)
    return all_inventory_chemicals


       #synonyms_of_interest = synonyms[synonyms['listName'].isin(lists_of_interest)]
       #['alternateIds']
       #alternate_id = pd.unique()

       #for l in lists_of_interest:
       #    record = synonyms_of_interest[synonyms_of_interest["listName"] == l]["synonymName"]
       #    list_acronym = program_of_interest_to_inventory_mapping[l]
       #    if len(record.values) == 0:
               #no synonym is present
       #        chemicaldict[list_acronym] = None
       #    else:
       #        syn = record.values[0]
       #        chemicaldict[list_acronym] = syn
       #        all_chemical_list.append(chemicaldict)



def query_SRS_for_flow(url,for_single_flow=False):
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


fieldstokeep = ['epaName', 'currentCasNumber', 'internalTrackingNumber', 'subsKey']
#Processes json response, returns a dataframe with key and CAS
def process_single_SRS_json_response(srs_json_response):
    chemical_srs_info = pd.DataFrame(columns=["SRS_ID", "SRS_CAS"])
    #for r in range(0,len(srs_json_response)):
    #Do not loop through response but use first response
    chemical_srs_info.loc[0, "SRS_ID"] = srs_json_response[0]['subsKey']
    chemical_srs_info.loc[0, "SRS_CAS"] = srs_json_response[0]['currentCasNumber']
    return chemical_srs_info


def add_manual_matches(df_matches,include_proxies=True):
    #Read in manual matches
    manual_matches = pd.read_csv(data_dir+'chemicalmatches_manual.csv',header=0,dtype={'FlowID':'str','SRS_ID':'str'})
    if not include_proxies:
        manual_matches = manual_matches[manual_matches['Proxy_Used']==0]
    #drop unneded columns
    manual_matches = manual_matches.drop(columns=['Proxy_Used','Proxy_Name','FlowName'])
    manual_matches = manual_matches.rename(columns={'SRS_ID':'SRS_ID_Manual'})
    #Merge with list
    df_matches = pd.merge(df_matches,manual_matches,on=['FlowID','Source'],how='left')
    #Set null SRS_IDs to those manually found. Replaces null with null if not
    df_matches.loc[df_matches['SRS_ID'].isnull(),'SRS_ID'] = df_matches['SRS_ID_Manual']
    df_matches = df_matches.drop(columns=['SRS_ID_Manual'])
    return df_matches





# Only keep fields of interest
#
#     alllistsdf = pd.DataFrame(columns=fieldstokeep)
#     alllistsdf['program_acronym'] = None
#     alllistsdf['srs_link'] = None
#     # also add in a field to identify the df

    # Loop through, return the list, convert to df, select fields of interest, identify list, write to existing df
# for p in programlists:
#     url = baseurl + p
#     programlistjson = requests.get(url).json()
#     programlistdf = pd.DataFrame(programlistjson)
#     # See first ten
#     programlistdf.head(10)
#     programlistdf = programlistdf[fieldstokeep]
#     programlistdf['srs_link'] = srs_url + programlistdf['subsKey']
#     programlistdf.loc[:, 'program_acronym'] = p
#     alllistsdf = pd.concat([alllistsdf, programlistdf], ignore_index=True)
#
# # Filter out non-chemicals
# alllistsdf = alllistsdf[alllistsdf['substanceType'] == 'Chemical Substance']
