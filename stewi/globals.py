# globals.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Supporting variables and functions used in stewi
"""

import pandas as pd
import json
import logging as log
import os
import yaml
import time
from datetime import datetime

from esupy.processed_data_mgmt import Paths, FileMeta,\
    load_preprocessed_output, remove_extra_files,\
    write_df_to_file, create_paths_if_missing, write_metadata_to_file,\
    read_source_metadata
from esupy.dqi import get_weighted_average
from esupy.util import get_git_hash

try: MODULEPATH = os.path.dirname(os.path.realpath(
    __file__)).replace('\\', '/') + '/'
except NameError: MODULEPATH = 'stewi/'

data_dir = MODULEPATH + 'data/'

log.basicConfig(level=log.INFO, format='%(levelname)s %(message)s')
STEWI_VERSION = '0.9.9'


#Common declaration of write format for package data products
WRITE_FORMAT = "parquet"

paths = Paths()
paths.local_path = os.path.realpath(paths.local_path + "/stewi")
output_dir = paths.local_path

# global variable to replace stored inventory files when saving
REPLACE_FILES = False

git_hash = get_git_hash()

stewi_formats = ['flowbyfacility', 'flow', 'facility', 'flowbyprocess']
inventory_formats = ['flowbyfacility', 'flowbyprocess']

source_metadata = {
    'SourceType': 'Static File',  #Other types are "Web service"
    'SourceFileName':'NA',
    'SourceURL':'NA',
    'SourceVersion':'NA',
    'SourceAcquisitionTime':'NA',
    'StEWI_Version':STEWI_VERSION,
    }

inventory_single_compartments = {"NEI":"air",
                                 "RCRAInfo":"waste",
                                 "GHGRP":"air",
                                 "DMR":"water"}

flowbyfacility_fields = {'FacilityID': [{'dtype': 'str'}, {'required': True}],
                         'FlowName': [{'dtype': 'str'}, {'required': True}],
                         'Compartment': [{'dtype': 'str'}, {'required': True}],
                         'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                         'Unit': [{'dtype': 'str'}, {'required': True}],
                         'DataReliability': [{'dtype': 'float'}, {'required': True}],
                         }

facility_fields = {'FacilityID':[{'dtype': 'str'}, {'required': True}],
                   'FacilityName':[{'dtype': 'str'}, {'required': False}],
                   'Address':[{'dtype': 'str'}, {'required': False}],
                   'City':[{'dtype': 'str'}, {'required': False}],
                   'State':[{'dtype': 'str'}, {'required': True}],
                   'Zip':[{'dtype': 'str'}, {'required': False}],
                   'Latitude':[{'dtype': 'float'}, {'required': False}],
                   'Longitude':[{'dtype': 'float'}, {'required': False}],
                   'County':[{'dtype': 'str'}, {'required': False}],
                   'NAICS':[{'dtype': 'str'}, {'required': False}],
                   'SIC':[{'dtype': 'str'}, {'required': False}],
                   }

flowbyprocess_fields = {'FacilityID': [{'dtype': 'str'}, {'required': True}],
                    'FlowName': [{'dtype': 'str'}, {'required': True}],
                    'Compartment': [{'dtype': 'str'}, {'required': True}],
                    'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                    'Unit': [{'dtype': 'str'}, {'required': True}],
                    'DataReliability': [{'dtype': 'float'}, {'required': True}],
                    'Process': [{'dtype': 'str'}, {'required': True}],
                    'ProcessType': [{'dtype': 'str'}, {'required': False}],
                    }

flow_fields = {'FlowName': [{'dtype': 'str'}, {'required': True}],
               'FlowID': [{'dtype': 'str'}, {'required': True}],
               'CAS':  [{'dtype': 'str'}, {'required': False}],
               'Compartment': [{'dtype': 'str'}, {'required': False}],
               'Unit': [{'dtype': 'str'}, {'required': False}],
               }

format_dict = {'flowbyfacility': flowbyfacility_fields,
               'flowbyprocess': flowbyprocess_fields,
               'facility': facility_fields,
               'flow': flow_fields}

def set_stewi_meta(file_name, inventory_format = ''):
    """Creates a class of esupy FileMeta with the inventory_format assigned
    as category"""
    stewi_meta = FileMeta()
    stewi_meta.name_data = file_name
    stewi_meta.category = inventory_format
    stewi_meta.tool = "StEWI"
    stewi_meta.tool_version = STEWI_VERSION
    stewi_meta.ext = WRITE_FORMAT
    stewi_meta.git_hash = git_hash
    stewi_meta.date_created = datetime.now().strftime('%d-%b-%Y')
    return stewi_meta


def config(config_path=MODULEPATH + 'config.yaml'):
    """Read and return stewi configuration file"""
    configfile = None
    with open(config_path, mode='r') as f:
        configfile = yaml.load(f,Loader=yaml.FullLoader)
    return configfile


def url_is_alive(url):
    """
    Checks that a given URL is reachable.
    :param url: A URL
    :rtype: bool
    """
    import urllib
    request = urllib.request.Request(url)
    request.get_method = lambda: 'HEAD'
    try:
        urllib.request.urlopen(request)
        return True
    except urllib.request.HTTPError:
        return False
    except urllib.error.URLError:
        return False


def download_table(filepath, url, get_time=False, zip_dir=None):
    if not os.path.exists(filepath):
        if url[-4:].lower() == '.zip':
            import zipfile, requests, io
            table_request = requests.get(url).content
            zip_file = zipfile.ZipFile(io.BytesIO(table_request))
            if zip_dir is None:
                zip_dir = os.path.abspath(os.path.join(filepath, "../../.."))
            zip_file.extractall(zip_dir)
        elif 'xls' in url.lower() or url.lower()[-5:] == 'excel':
            import urllib, shutil
            with urllib.request.urlopen(url) as response, open(filepath, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
        elif 'json' in url.lower():
            import pandas as pd
            pd.read_json(url).to_csv(filepath, index=False)
        if get_time:
            try: retrieval_time = os.path.getctime(filepath)
            except: retrieval_time = time.time()
            return time.ctime(retrieval_time)
    elif get_time:
        return time.ctime(os.path.getctime(filepath))
        

def import_table(path_or_reference, skip_lines=0, get_time=False):
    if '.core.frame.DataFrame' in str(type(path_or_reference)):
        df = path_or_reference
    elif path_or_reference[-3:].lower() == 'csv':
        df = pd.read_csv(path_or_reference, low_memory=False)
    elif 'xls' in path_or_reference[-4:].lower():
        import_file = pd.ExcelFile(path_or_reference)
        df = {sheet: import_file.parse(sheet, skiprows=skip_lines,
                                       engine='openpyxl')
              for sheet in import_file.sheet_names}
    if get_time:
        try: retrieval_time = os.path.getctime(path_or_reference)
        except: retrieval_time = time.time()
        return df, time.ctime(retrieval_time)
    return df


def drop_excel_sheets(excel_dict, drop_sheets):
    for s in drop_sheets:
        try:
            excel_dict.pop(s)
        except KeyError:
            continue
    return excel_dict


def aggregate(df, grouping_vars = None):
    """
    Aggregate a 'FlowAmount' in a dataframe based on the passed grouping_vars
    and generating a weighted average for data quality fields
    :param df: dataframe to aggregate
    :param grouping_vars: list of df column headers on which to groupby
    :return: aggregated dataframe with weighted average data reliability score
    """
    if grouping_vars is None:
        grouping_vars = [x for x in df.columns if x not in ['FlowAmount','DataReliability']]
    df_agg = df.groupby(grouping_vars).agg({'FlowAmount': ['sum']})
    df_agg['DataReliability']=get_weighted_average(
        df, 'DataReliability', 'FlowAmount', grouping_vars)
    df_agg = df_agg.reset_index()
    df_agg.columns = df_agg.columns.droplevel(level=1)
    # drop those rows where flow amount is negative, zero, or NaN
    df_agg = df_agg[df_agg['FlowAmount'] > 0]
    df_agg = df_agg[df_agg['FlowAmount'].notna()]
    return df_agg

# Convert amounts. Note this could be replaced with a conversion utility
def unit_convert(df, coln1, coln2, unit, conversion_factor, coln3):
    """
    Converts values in coln3 if coln2 == unit, based on the conversion
    factor, and assigns to coln1
    """
    df.loc[df[coln2] == unit, coln1] = conversion_factor * df[coln3]
    return df

#Conversion factors
USton_kg = 907.18474
lb_kg = 0.4535924
MMBtu_MJ = 1055.056
MWh_MJ = 3600
g_kg = 0.001


def write_metadata(file_name, metadata_dict, category='',
                   datatype="inventory"):
    """writes metadata specific to the inventory in file_name to local
    directory as a JSON file
    :param file_name: str in the form of inventory_year
    :param metadata_dict: dictionary of metadata to save
    :param category: str of a stewi format type e.g. 'flowbyfacility'
        or source category e.g. 'TRI Data Files'
    :param datatype: 'inventory' when saving StEWI output files, 'source'
        when downloading and processing source data, 'validation' for saving
        validation metadata
    """
    if (datatype == "inventory") or (datatype == "source"):
        meta = set_stewi_meta(file_name, inventory_format=category)
        meta.tool_meta = metadata_dict
        write_metadata_to_file(paths, meta)
    elif datatype == "validation":
        with open(output_dir + '/validation/' + file_name + \
                  '_validationset_metadata.json', 'w') as file:
            file.write(json.dumps(metadata_dict, indent=4))


def compile_source_metadata(sourcefile, config, year):
    """Compiles metadata related to the source data downloaded to generate inventory
    :param sourcefile: str or list of source file names
    :param config:
    :param year:
    :returns dictionary in the format of source_metadata
    """
    metadata = dict(source_metadata)
    if isinstance(sourcefile, list):
        filename = sourcefile[0]
    else:
        filename = sourcefile
    data_retrieval_time = time.ctime(os.path.getmtime(filename))
    if data_retrieval_time is not None:
        metadata['SourceAcquisitionTime'] = data_retrieval_time
    metadata['SourceFileName'] = sourcefile
    metadata['SourceURL'] = config['url']
    if year in config:
        metadata['SourceVersion'] = config[year]['file_version']
    else:
        import re
        pattern = 'V[0-9]'
        version = re.search(pattern,filename,flags=re.IGNORECASE)
        if version is not None:
            metadata['SourceVersion'] = version.group(0)
    return metadata

def remove_line_breaks(df, headers_only = True):
    for column in df:
        df.rename(columns={column: column.replace('\r\n',' ')}, inplace=True)
        df.rename(columns={column: column.replace('\n',' ')}, inplace=True)
    if not headers_only:
        df = df.replace(to_replace=['\r\n','\n'],value=[' ', ' '], regex=True)
    return df    


def get_required_fields(inventory_format='flowbyfacility'):
    fields = format_dict[inventory_format]
    required_fields = {key: value[0]['dtype'] for key, value
                       in fields.items() if value[1]['required'] is True}
    return required_fields


def get_optional_fields(inventory_format='flowbyfacility'):
    fields = format_dict[inventory_format]
    optional_fields = {key: value[0]['dtype'] for key, value
                       in fields.items()}
    return optional_fields


def add_missing_fields(df, inventory_acronym, inventory_format='flowbyfacility'):
    """Adds all fields and formats for stewi inventory file
    :param df: dataframe of inventory data
    :param inventory_acronym: str of inventory e.g. 'NEI'
    :param inventory_format: str of a stewi format type e.g. 'flowbyfacility'
    :return: dataframe of inventory containing all relevant columns 
    """
    fields = dict(format_dict[inventory_format])
    # Rename for legacy datasets
    if 'ReliabilityScore' in df.columns:
        df.rename(columns={'ReliabilityScore':'DataReliability'}, inplace=True)
    # Add in units and compartment if not present
    if ('Unit' in fields.keys()) & ('Unit' not in df.columns):
        df['Unit'] = 'kg'
    if ('Compartment' in fields.keys()) & ('Compartment' not in df.columns):
        try:
            compartment = inventory_single_compartments[inventory_acronym]
        except KeyError:
            log.warning('no compartment found in inventory')
            compartment = ''
        df['Compartment'] = compartment
    for key in fields.keys():
        if key not in df.columns:
            df[key] = None
    # Resort
    df = df[fields.keys()]
    return df

def checkforFile(filepath):
    return os.path.exists(filepath)


def store_inventory(df, file_name, inventory_format, replace_files = REPLACE_FILES):
    """Stores the inventory dataframe to local directory based on inventory format
    :param df: dataframe of processed inventory to save
    :param file_name: str of inventory_year e.g. 'TRI_2016'
    :param inventory_format: str of a stewi format type e.g. 'flowbyfacility'
    :param replace_files: bool, True will use esupy function to delete existing
        files of the same name
    """
    meta = set_stewi_meta(file_name, inventory_format)
    method_path = output_dir + '/' + meta.category
    try:
        log.info('saving ' + meta.name_data + ' to ' + method_path)
        write_df_to_file(df,paths,meta)
        if replace_files:
            remove_extra_files(meta, paths)
    except:
        log.error('Failed to save inventory')

def read_inventory(inventory_acronym, year, inventory_format):
    """Returns the inventory as dataframe from local directory. If not found,
    the inventory is generated.
    :param inventory_acronym: like 'TRI'
    :param year: year as number like 2010
    :param inventory_format: str of a stewi format type e.g. 'flowbyfacility'
    :return: dataframe of stored inventory; if not present returns None
    """
    file_name = inventory_acronym + '_' + str(year)
    meta = set_stewi_meta(file_name, inventory_format)
    inventory = load_preprocessed_output(meta, paths)
    method_path = output_dir + '/' + meta.category
    if inventory is None:
        log.info(meta.name_data + ' not found in ' + method_path)
        log.info('requested inventory does not exist in local directory, '
                 'it will be generated...')
        generate_inventory(inventory_acronym, year)
        inventory = load_preprocessed_output(meta, paths)
        if inventory is None:
            log.error('error generating inventory')
    if inventory is not None:
        log.info('loaded ' + meta.name_data + ' from ' + method_path)
        # ensure dtypes
        fields = get_optional_fields(inventory_format)
        fields = {key: value for key, value in fields.items()
                  if key in list(inventory)}
        inventory = inventory.astype(fields)
    return inventory


def generate_inventory(inventory_acronym, year):
    """generates the passed inventory data by running the appropriate modules
    :param inventory_acronym: like 'TRI'
    :param year: year as number like 2010
    """
    if inventory_acronym not in config()['databases']:
        log.error('requested inventory not available')
    year = str(year)
    if inventory_acronym == 'DMR':
        import stewi.DMR as DMR
        DMR.main(Option = 'A', Year = [year])
        DMR.main(Option = 'B', Year = [year])
    elif inventory_acronym == 'eGRID':
        import stewi.egrid as eGRID
        eGRID.main(Option = 'A', Year = [year])
        eGRID.main(Option = 'B', Year = [year])
    elif inventory_acronym == 'GHGRP':
        import stewi.GHGRP as GHGRP
        GHGRP.main(Option = 'A', Year = [year])
        GHGRP.main(Option = 'B', Year = [year])
    elif inventory_acronym == 'NEI':
        import stewi.NEI as NEI
        NEI.main(Option = 'A', Year = [year])
    elif inventory_acronym == 'RCRAInfo':
        import stewi.RCRAInfo as RCRAInfo
        RCRAInfo.main(Option = 'A', Year = [year],
                      Tables = ['BR_REPORTING', 'HD_LU_WASTE_CODE'])
        RCRAInfo.main(Option = 'B', Year = [year],
                      Tables = ['BR_REPORTING'])
        RCRAInfo.main(Option = 'C', Year = [year])
    elif inventory_acronym == 'TRI':
        import stewi.TRI as TRI
        TRI.main(Option = 'A', Year = [year], Files = ['1a', '3a'])
        TRI.main(Option = 'C', Year = [year], Files = ['1a', '3a'])
    

def get_reliability_table_for_source(source):
    """retrieve the reliability table within stewi"""
    dq_file = 'DQ_Reliability_Scores_Table3-3fromERGreport.csv'
    df = pd.read_csv(data_dir + dq_file, usecols=['Source', 'Code',
                                                  'DQI Reliability Score'])
    df = df.loc[df['Source'] == source].reset_index(drop=True)
    df.drop('Source', axis=1, inplace=True)
    return df
