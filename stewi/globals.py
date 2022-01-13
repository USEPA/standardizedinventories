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
import urllib
from datetime import datetime
from pathlib import Path

from esupy.processed_data_mgmt import Paths, FileMeta,\
    load_preprocessed_output, remove_extra_files,\
    write_df_to_file, write_metadata_to_file,\
    read_source_metadata
from esupy.dqi import get_weighted_average
from esupy.util import get_git_hash


MODULEPATH = Path(__file__).resolve().parent
DATA_PATH = MODULEPATH / 'data'

log.basicConfig(level=log.INFO, format='%(levelname)s %(message)s')
STEWI_VERSION = '1.0.1'

# Conversion factors
USton_kg = 907.18474
lb_kg = 0.4535924
MMBtu_MJ = 1055.056
MWh_MJ = 3600
g_kg = 0.001

# Common declaration of write format for package data products
WRITE_FORMAT = "parquet"

paths = Paths()
paths.local_path = os.path.realpath(paths.local_path + "/stewi")

# global variable to replace stored inventory files when saving
REPLACE_FILES = False

GIT_HASH = get_git_hash()

source_metadata = {
    'SourceType': 'Static File',  # Other types are "Web service"
    'SourceFileName': 'NA',
    'SourceURL': 'NA',
    'SourceVersion': 'NA',
    'SourceAcquisitionTime': 'NA',
    'StEWI_Version': STEWI_VERSION,
    }

inventory_single_compartments = {"NEI": "air",
                                 "RCRAInfo": "waste",
                                 "GHGRP": "air",
                                 "DMR": "water"}


def set_stewi_meta(file_name, stewiformat=''):
    """Create a class of esupy FileMeta with stewiformat assigned as category."""
    stewi_meta = FileMeta()
    stewi_meta.name_data = file_name
    stewi_meta.category = stewiformat
    stewi_meta.tool = "StEWI"
    stewi_meta.tool_version = STEWI_VERSION
    stewi_meta.ext = WRITE_FORMAT
    stewi_meta.git_hash = GIT_HASH
    stewi_meta.date_created = datetime.now().strftime('%d-%b-%Y')
    return stewi_meta


def config(config_path=MODULEPATH, file='config.yaml'):
    """Read and return stewi configuration file."""
    configfile = None
    path = config_path.joinpath(file)
    with open(path, mode='r') as f:
        configfile = yaml.load(f, Loader=yaml.FullLoader)
    return configfile


def url_is_alive(url):
    """Check that a given URL is reachable.

    :param url: A URL
    :rtype: bool
    """
    request = urllib.request.Request(url)
    request.get_method = lambda: 'HEAD'
    try:
        urllib.request.urlopen(request)
        return True
    except urllib.request.HTTPError:
        return False
    except urllib.error.URLError:
        return False


def download_table(filepath: Path, url: str, get_time=False):
    """Download file at url to Path if it does not exist."""
    if not filepath.exists():
        if url.lower().endswith('zip'):
            import zipfile, requests, io
            table_request = requests.get(url).content
            zip_file = zipfile.ZipFile(io.BytesIO(table_request))
            zip_file.extractall(filepath)
        elif 'xls' in url.lower() or url.lower().endswith('excel'):
            import shutil
            try:
                with urllib.request.urlopen(url) as response, open(filepath, 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
            except urllib.error.HTTPError:
                log.warning(f'Error downloading {url}')
        elif 'json' in url.lower():
            pd.read_json(url).to_csv(filepath, index=False)
        if get_time:
            try: retrieval_time = filepath.stat().st_ctime
            except: retrieval_time = time.time()
            return time.ctime(retrieval_time)
    elif get_time:
        return time.ctime(filepath.stat().st_ctime)


def import_table(path_or_reference, get_time=False):
    """Read and return time of csv from url or Path."""
    try:
        df = pd.read_csv(path_or_reference, low_memory=False)
    except urllib.error.URLError as exception:
        log.warning(exception.reason)
        log.info('retrying url...')
        time.sleep(3)
        df = pd.read_csv(path_or_reference, low_memory=False)
    if get_time and isinstance(path_or_reference, Path):
        retrieval_time = path_or_reference.stat().st_ctime
        return df, time.ctime(retrieval_time)
    elif get_time:
        retrieval_time = time.time()
        return df, time.ctime(retrieval_time)
    return df


def aggregate(df, grouping_vars=None):
    """Aggregate a 'FlowAmount' in a dataframe based on the passed grouping_vars
    and generating a weighted average for data quality fields.

    :param df: dataframe to aggregate
    :param grouping_vars: list of df column headers on which to groupby
    :return: aggregated dataframe with weighted average data reliability score
    """
    if grouping_vars is None:
        grouping_vars = [x for x in df.columns if x not in ['FlowAmount', 'DataReliability']]
    df_agg = df.groupby(grouping_vars).agg({'FlowAmount': ['sum']})
    df_agg['DataReliability'] = get_weighted_average(
        df, 'DataReliability', 'FlowAmount', grouping_vars)
    df_agg = df_agg.reset_index()
    df_agg.columns = df_agg.columns.droplevel(level=1)
    # drop those rows where flow amount is negative, zero, or NaN
    df_agg = df_agg[df_agg['FlowAmount'] > 0]
    df_agg = df_agg[df_agg['FlowAmount'].notna()]
    return df_agg


def unit_convert(df, coln1, coln2, unit, conversion_factor, coln3):
    """Convert values in coln3 if coln2 == unit, based on the conversion
    factor, and assigns to coln1.
    """
    df.loc[df[coln2] == unit, coln1] = conversion_factor * df[coln3]
    return df


def write_metadata(file_name, metadata_dict, category='',
                   datatype="inventory"):
    """Write JSON metadata specific to inventory to local directory.

    :param file_name: str in the form of inventory_year
    :param metadata_dict: dictionary of metadata to save
    :param category: str of a stewi format type e.g. 'flowbyfacility'
        or source category e.g. 'TRI Data Files'
    :param datatype: 'inventory' when saving StEWI output files, 'source'
        when downloading and processing source data, 'validation' for saving
        validation metadata
    """
    if (datatype == "inventory") or (datatype == "source"):
        meta = set_stewi_meta(file_name, stewiformat=category)
        meta.tool_meta = metadata_dict
        write_metadata_to_file(paths, meta)
    elif datatype == "validation":
        with open(paths.local_path + '/validation/' + file_name +
                  '_validationset_metadata.json', 'w') as file:
            file.write(json.dumps(metadata_dict, indent=4))


def compile_source_metadata(sourcefile, config, year):
    """Compile metadata related to the source data downloaded to generate inventory.

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
        version = re.search(pattern, filename, flags=re.IGNORECASE)
        if version is not None:
            metadata['SourceVersion'] = version.group(0)
    return metadata


def remove_line_breaks(df, headers_only=True):
    for column in df:
        df.rename(columns={column: column.replace('\r\n', ' ')}, inplace=True)
        df.rename(columns={column: column.replace('\n', ' ')}, inplace=True)
    if not headers_only:
        df = df.replace(to_replace=['\r\n', '\n'], value=[' ', ' '], regex=True)
    return df


def add_missing_fields(df, inventory_acronym, f, maintain_columns=False):
    """Add all fields and formats for stewi inventory file.

    :param df: dataframe of inventory data
    :param inventory_acronym: str of inventory e.g. 'NEI'
    :param f: object of class StewiFormat
    :param maintain_columns: bool, if True do not delete any existing columns,
        useful for inventories or inventory formats that may have custom fields
    :return: dataframe of inventory containing all relevant columns
    """
    # Rename for legacy datasets
    if 'ReliabilityScore' in df:
        df.rename(columns={'ReliabilityScore': 'DataReliability'}, inplace=True)
    # Add in units and compartment if not present
    if 'Unit' in f.fields() and 'Unit' not in df:
        df['Unit'] = 'kg'
    if 'Compartment' in f.fields() and 'Compartment' not in df:
        try:
            compartment = inventory_single_compartments[inventory_acronym]
        except KeyError:
            log.warning('no compartment found in inventory')
            compartment = ''
        df['Compartment'] = compartment
    for field in f.fields():
        if field not in df:
            df[field] = None
    # Resort
    col_list = f.fields()
    if maintain_columns:
        col_list = col_list + [c for c in df if c not in f.fields()]
    df = df[col_list]
    df.reset_index(drop=True, inplace=True)
    return df


def store_inventory(df, file_name, f, replace_files=REPLACE_FILES):
    """Store inventory to local directory based on inventory format.

    :param df: dataframe of processed inventory to save
    :param file_name: str of inventory_year e.g. 'TRI_2016'
    :param f: object of class StewiFormat
    :param replace_files: bool, True will use esupy function to delete existing
        files of the same name
    """
    meta = set_stewi_meta(file_name, str(f))
    method_path = paths.local_path + '/' + meta.category
    try:
        log.info(f'saving {meta.name_data} to {method_path}')
        write_df_to_file(df, paths, meta)
        if replace_files:
            remove_extra_files(meta, paths)
    except:
        log.error('Failed to save inventory')


def read_inventory(inventory_acronym, year, f):
    """Return the inventory from local directory. If not found, generate it.

    :param inventory_acronym: like 'TRI'
    :param year: year as number like 2010
    :param f: object of class StewiFormat
    :return: dataframe of stored inventory; if not present returns None
    """
    file_name = inventory_acronym + '_' + str(year)
    meta = set_stewi_meta(file_name, str(f))
    inventory = load_preprocessed_output(meta, paths)
    method_path = paths.local_path + '/' + meta.category
    if inventory is None:
        log.info(f'{meta.name_data} not found in {method_path}')
        log.info('requested inventory does not exist in local directory, '
                 'it will be generated...')
        generate_inventory(inventory_acronym, year)
        inventory = load_preprocessed_output(meta, paths)
        if inventory is None:
            log.error('error generating inventory')
    if inventory is not None:
        log.info(f'loaded {meta.name_data} from {method_path}')
        # ensure dtypes
        fields = f.field_types()
        fields = {key: value for key, value in fields.items()
                  if key in list(inventory)}
        inventory = inventory.astype(fields)
    return inventory


def generate_inventory(inventory_acronym, year):
    """Generate inventory data by running the appropriate modules.

    :param inventory_acronym: like 'TRI'
    :param year: year as number like 2010
    """
    if inventory_acronym not in config()['databases']:
        log.error('requested inventory not available')
    year = str(year)
    if inventory_acronym == 'DMR':
        import stewi.DMR as DMR
        DMR.main(Option= 'A', Year = [year])
        DMR.main(Option= 'B', Year = [year])
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
    """Retrieve the reliability table within stewi."""
    dq_file = 'DQ_Reliability_Scores_Table3-3fromERGreport.csv'
    df = pd.read_csv(DATA_PATH.joinpath(dq_file), usecols=['Source', 'Code',
                                                          'DQI Reliability Score'])
    df = df.loc[df['Source'] == source].reset_index(drop=True)
    df.drop('Source', axis=1, inplace=True)
    return df
