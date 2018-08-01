import zipfile
import io
import requests
import json

stewi_inventories = ["NEI","TRI","eGRID","RCRAInfo"]

inventory_to_FRS_pgm_acronymn = {"NEI":"EIS","TRI":"TRIS","eGRID":"EGRID","GHGRP":"E-GGRT","RCRAInfo":"RCRAINFO","DMR":"NPDES"}

def download_extract_FRS_combined_national(FRSpath):
    url = 'https://www3.epa.gov/enviro/html/fii/downloads/state_files/national_combined.zip'
    request = requests.get(url).content
    zip_file = zipfile.ZipFile(io.BytesIO(request))
    zip_file.extractall(FRSpath)

#Only can be applied before renaming the programs to inventories
def filter_by_program_list(df,program_list):
    df = df[df['PGM_SYS_ACRNM'].isin(program_list)]
    return df

#Only can be applied after renaming the programs to inventories
def filter_by_inventory_list(df,inventory_list):
    df = df[df['Source'].isin(inventory_list)]
    return df

def filter_by_facility_list(df,facility_list):
    df = df[df['FRS_ID'].isin(facility_list)]
    return df

def list_facilities_not_in_bridge(bridges, facility_list):
    facilities = bridges[bridges['REGISTRY_ID'].isin(facility_list)]
    return bridges

#Returns list of acronymns for inventories that correspond to
def get_programs_for_inventory_list(list_of_inventories):
    program_list = []
    for l in list_of_inventories:
        pgm_acronym = inventory_to_FRS_pgm_acronymn[l]
        program_list.append(pgm_acronym)
    return program_list

def invert_inventory_to_FRS():
    FRS_to_inventory_pgm_acronymn = {v: k for k, v in inventory_to_FRS_pgm_acronymn.items()}
    return FRS_to_inventory_pgm_acronymn

#Function to return facility info from FRS web service
#Limitation - the web service only matches on facility at a time
##example
#id='2'
#program_acronym='EGRID'
def callFRSforProgramAcronymandIDfromAPI(program_acronym, id):
    # base url
    base = 'http://ofmpub.epa.gov/enviro/frs_rest_services'
    facilityquery = base + '.get_facilities?'
    pgm_sys_id = 'pgm_sys_id='
    pgm_sys_acrnm = 'pgm_sys_acrnm='
    output = 'output=JSON'
    url = facilityquery + pgm_sys_acrnm + program_acronym + '&' + pgm_sys_id + id + '&' + output
    facilityresponse = requests.get(url)
    facilityjson = json.loads(facilityresponse.text)['Results']
    facilityinfo = facilityjson['FRSFacility']
    return facilityinfo

def getFRSIDfromAPIfaciltyinfo(facilityinfo):
    FRSID = facilityinfo[0]['RegistryId']
    return FRSID