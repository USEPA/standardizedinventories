# globals.py (facilitymatcher)
# !/usr/bin/env python3
# coding=utf-8
"""
Supporting variables and functions used in facilitymatcher
"""

import zipfile
import io
import requests
import pandas as pd
import os
from datetime import datetime
from stewi.globals import log, set_stewi_meta, source_metadata, config
import facilitymatcher.WriteFacilityMatchesforStEWI as write_fm
import facilitymatcher.WriteFRSNAICSforStEWI as write_naics
from esupy.processed_data_mgmt import Paths, load_preprocessed_output,\
    write_df_to_file, write_metadata_to_file, read_source_metadata
from esupy.util import strip_file_extension

try: MODULEPATH = os.path.dirname(
    os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: MODULEPATH = 'facilitymatcher/'

data_dir = MODULEPATH + 'data/'

paths = Paths()
paths.local_path = os.path.realpath(paths.local_path + "/facilitymatcher")
output_dir = paths.local_path
ext_folder = 'FRS Data Files'
FRSpath = paths.local_path + '/' + ext_folder

FRS_config = config(config_path=MODULEPATH)['databases']['FRS']

inventory_to_FRS_pgm_acronymn = FRS_config['program_dictionary']
stewi_inventories = list(inventory_to_FRS_pgm_acronymn.keys())


def set_facilitymatcher_meta(file_name, category):
    """Create a class of esupy FileMeta."""
    facilitymatcher_meta = set_stewi_meta(file_name, category)
    facilitymatcher_meta.tool = "facilitymatcher"
    return facilitymatcher_meta


def download_extract_FRS_combined_national(file=None):
    """Download and extract file from source to local directory."""
    url = FRS_config['url']
    log.info('initiating url request from %s', url)
    request = requests.get(url).content
    zip_file = zipfile.ZipFile(io.BytesIO(request))
    source_dict = dict(source_metadata)
    source_dict['SourceType'] = 'Zip file'
    source_dict['SourceURL'] = url
    if file is None:
        log.info(f'extracting all FRS files from {url}')
        name = 'FRS_Files'
        zip_file.extractall(FRSpath)
    else:
        log.info('extracting %s from %s', file, url)
        zip_file.extract(file, path=FRSpath)
        source_dict['SourceFileName'] = file
        name = strip_file_extension(file)
    source_dict['SourceAcquisitionTime'] = datetime.now().strftime('%d-%b-%Y')
    write_fm_metadata(name, source_dict, category=ext_folder)


def read_FRS_file(file_name, col_dict):
    """Retrieve FRS data file stored locally."""
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
    """Store the facilitymatcher file to local directory."""
    meta = set_facilitymatcher_meta(file_name, category)
    method_path = output_dir + '/' + meta.category
    try:
        log.info(f'saving {meta.name_data} to {method_path}')
        write_df_to_file(df, paths, meta)
        metadata_dict = {}
        for source in sources:
            metadata_dict[source] = read_source_metadata(paths,
                set_facilitymatcher_meta(strip_file_extension(source),
                                         ext_folder),
                force_JSON=True)['tool_meta']
        write_fm_metadata(file_name, metadata_dict)
    except:
        log.error('Failed to save inventory')


def get_fm_file(file_name):
    """Read facilitymatcher file, if not present, generate it."""
    file_meta = set_facilitymatcher_meta(file_name, category='')
    df = load_preprocessed_output(file_meta, paths)
    if df is None:
        log.info(f'{file_name} not found in {output_dir}, '
                 'writing facility matches to file')
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


def write_fm_metadata(file_name, metadata_dict, category=''):
    """Generate and store metadata for facility matcher file."""
    meta = set_facilitymatcher_meta(file_name, category=category)
    meta.tool_meta = metadata_dict
    write_metadata_to_file(paths, meta)


#Only can be applied before renaming the programs to inventories
def filter_by_program_list(df, program_list):
    df = df[df['PGM_SYS_ACRNM'].isin(program_list)]
    return df


#Only can be applied after renaming the programs to inventories
def filter_by_inventory_list(df, inventory_list):
    df = df[df['Source'].isin(inventory_list)].reset_index(drop=True)
    return df


#Only can be applied after renaming the programs to inventories
def filter_by_inventory_id_list(df, inventories_of_interest,
                                base_inventory, id_list):
    # Find FRS_IDs first
    FRS_ID_list = list(df.loc[(df['Source'] == base_inventory) &
                              (df['FacilityID'].isin(id_list)), "FRS_ID"])
    # Now use that FRS_ID list and list of inventories of interest to get decired matches
    df = df.loc[(df['Source'].isin(inventories_of_interest)) &
                (df['FRS_ID'].isin(FRS_ID_list))]
    return df


def filter_by_facility_list(df, facility_list):
    df = df[df['FRS_ID'].isin(facility_list)]
    return df


def get_programs_for_inventory_list(list_of_inventories):
    """Return list of program acronymns for passed inventories."""
    program_list = [p for i, p in inventory_to_FRS_pgm_acronymn.items() if
                    i in list_of_inventories]
    return program_list


def invert_inventory_to_FRS():
    FRS_to_inventory_pgm_acronymn = {v: k for k, v in
                                     inventory_to_FRS_pgm_acronymn.items()}
    return FRS_to_inventory_pgm_acronymn


def add_manual_matches(df_matches):
    #Read in manual matches
    manual_matches = pd.read_csv(data_dir + 'facilitymatches_manual.csv',
                                 header=0,
                                 dtype={'FacilityID': 'str', 'FRS_ID': 'str'})
    #Append with list and drop any duplicates
    df_matches = pd.concat([df_matches, manual_matches], sort=False)
    df_matches = df_matches[~df_matches.duplicated(keep='first')]
    df_matches = df_matches.reset_index(drop=True)
    return df_matches
