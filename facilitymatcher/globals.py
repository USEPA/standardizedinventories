# globals.py (facilitymatcher)
# !/usr/bin/env python3
# coding=utf-8
"""
Supporting variables and functions used in facilitymatcher
"""

import zipfile
import io
import requests
import json
import pandas as pd
import os
from datetime import datetime
from stewi.globals import log, set_stewi_meta, source_metadata, config,\
    read_source_metadata
import facilitymatcher.WriteFacilityMatchesforStEWI as write_fm
import facilitymatcher.WriteFRSNAICSforStEWI as write_naics
from esupy.processed_data_mgmt import Paths, load_preprocessed_output,\
    write_df_to_file, write_metadata_to_file
from esupy.util import strip_file_extension

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'facilitymatcher/'

data_dir = modulepath + 'data/'

#Common declaration of write format for package data products
write_format = "parquet"

paths = Paths()
paths.local_path = os.path.realpath(paths.local_path + "/facilitymatcher")
output_dir = paths.local_path
ext_folder = '/FRS Data Files/'
FRSpath = paths.local_path + ext_folder

FRS_config = config(modulepath)['databases']['FRS']

inventory_to_FRS_pgm_acronymn = FRS_config['program_dictionary']
stewi_inventories = list(inventory_to_FRS_pgm_acronymn.keys())


def set_facilitymatcher_meta(file_name, category):
    facilitymatcher_meta = set_stewi_meta(file_name, category)
    facilitymatcher_meta.tool = "facilitymatcher"
    facilitymatcher_meta.ext = write_format
    return facilitymatcher_meta


def download_extract_FRS_combined_national(file=None):
    url = FRS_config['url']
    log.info('initiating url request from %s', url)
    request = requests.get(url).content
    zip_file = zipfile.ZipFile(io.BytesIO(request))
    source_dict = dict(source_metadata)
    source_dict['SourceType']='Zip file'
    source_dict['SourceURL']=url
    if file is None:
        log.info('extracting all FRS files from %s', url)
        name = 'FRS_Files'
        zip_file.extractall(FRSpath)
    else:
        log.info('extracting %s from %s', file, url)
        zip_file.extract(file, path = FRSpath)
        source_dict['SourceFileName']=file
        name = strip_file_extension(file)
    source_dict['SourceAcquisitionTime']= datetime.now().strftime('%d-%b-%Y')
    write_metadata(name, source_dict, category=ext_folder)


def read_FRS_file(file_name, col_dict):
    """Retrieves FRS data file stored locally"""
    file_meta = set_facilitymatcher_meta(file_name, category=ext_folder)
    log.info('loading %s from %s', file_meta.name_data, FRSpath)
    file_meta.name_data = strip_file_extension(file_meta.name_data)
    file_meta.ext = 'csv'
    df = load_preprocessed_output(file_meta, paths)
    df_FRS = pd.DataFrame()
    for k, v in col_dict.items():
        df_FRS[k] = df[k].astype(v)
    return df_FRS

def store_fm_file(df, file_name, category='', sources=[]):
    """Stores the facilitymatcher file to local directory"""
    meta = set_facilitymatcher_meta(file_name, category)
    method_path = output_dir + '/' + meta.category
    try:
        log.info('saving ' + meta.name_data + ' to ' + method_path)
        write_df_to_file(df,paths,meta)
        metadata_dict={}
        for source in sources:
            metadata_dict[source] = read_source_metadata(
                output_dir + ext_folder + strip_file_extension(source))['tool_meta']
        write_metadata(file_name, metadata_dict)
    except:
        log.error('Failed to save inventory')

def read_fm_file(file_name):
    """
    Read facilitymatcher file into dataframe. If not present, generate the file
    via script"""
    file_meta = set_facilitymatcher_meta(file_name, category='')
    df = load_preprocessed_output(file_meta, paths)
    if df is None:
        log.info('%s not found in %s, writing facility matches to file',
                 file_name, output_dir)
        if file_name == 'FacilityMatchList_forStEWI':
            write_fm.write_facility_matches()
        elif file_name == 'FRS_NAICSforStEWI':
            write_naics.write_NAICS_matches()
        df = load_preprocessed_output(file_meta, paths)
    col_dict = {"FRS_ID": "str",
                "FacilityID": "str",
                "NAICS": "str"}
    for k, v in col_dict.items():
        if k in df:
            df[k] = df[k].astype(v)
    return df

def write_metadata(file_name, metadata_dict, category=''):
    meta = set_facilitymatcher_meta(file_name, category=category)
    meta.tool_meta = metadata_dict
    write_metadata_to_file(paths, meta)

#Only can be applied before renaming the programs to inventories
def filter_by_program_list(df,program_list):
    df = df[df['PGM_SYS_ACRNM'].isin(program_list)]
    return df

#Only can be applied after renaming the programs to inventories
def filter_by_inventory_list(df,inventory_list):
    df = df[df['Source'].isin(inventory_list)].reset_index(drop = True)
    return df

#Only can be applied after renaming the programs to inventories
def filter_by_inventory_id_list(df,inventories_of_interest,
                                base_inventory,id_list):
    #Find FRS_IDs first
    FRS_ID_list = list(df.loc[(df['Source'] == base_inventory) &
                              (df['FacilityID'].isin(id_list)),"FRS_ID"])
    #Now use that FRS_ID list and list of inventories of interest to get decired matches
    df = df.loc[(df['Source'].isin(inventories_of_interest)) &
                (df['FRS_ID'].isin(FRS_ID_list))]
    return df

def filter_by_facility_list(df,facility_list):
    df = df[df['FRS_ID'].isin(facility_list)]
    return df

#Returns list of acronymns for inventories that correspond to
def get_programs_for_inventory_list(list_of_inventories):
    program_list = []
    for l in list_of_inventories:
        pgm_acronym = inventory_to_FRS_pgm_acronymn[l]
        program_list.append(pgm_acronym)
    return program_list

def invert_inventory_to_FRS():
    FRS_to_inventory_pgm_acronymn = {v: k for k, v in 
                                     inventory_to_FRS_pgm_acronymn.items()}
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
    url = facilityquery + pgm_sys_acrnm + program_acronym + '&'\
        + pgm_sys_id + id + '&' + output
    facilityresponse = requests.get(url)
    facilityjson = json.loads(facilityresponse.text)['Results']
    facilityinfo = facilityjson['FRSFacility']
    return facilityinfo

def getFRSIDfromAPIfaciltyinfo(facilityinfo):
    FRSID = facilityinfo[0]['RegistryId']
    return FRSID

def add_manual_matches(df_matches):
    #Read in manual matches
    manual_matches = pd.read_csv(data_dir+'facilitymatches_manual.csv',
                                 header=0, 
                                 dtype={'FacilityID':'str','FRS_ID':'str'})
    #Append with list
    df_matches = pd.concat([df_matches,manual_matches], sort = False)
    return df_matches

