# globals.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Supporting variables and functions used in stewi.
"""

import json
import logging as log
import os
import time
import copy
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import yaml

from esupy.processed_data_mgmt import Paths, FileMeta,\
    load_preprocessed_output, remove_extra_files,\
    write_df_to_file, write_metadata_to_file,\
    download_from_remote
from esupy.dqi import get_weighted_average
from esupy.util import get_git_hash
import stewi.exceptions


MODULEPATH = Path(__file__).resolve().parent
DATA_PATH = MODULEPATH / 'data'

log.basicConfig(level=log.INFO, format='%(levelname)s %(message)s')
STEWI_VERSION = '1.2.0'

# Conversion factors
USton_kg = 907.18474
lb_kg = 0.4535924
MMBtu_MJ = 1055.056
MWh_MJ = 3600
g_kg = 0.001

# Common declaration of write format for package data products
WRITE_FORMAT = "parquet"

paths = Paths()
paths.local_path = paths.local_path / 'stewi'
# TODO: rename `paths` to `path_data` and other `DATA_PATH` vars to `path_data_local`
# paths = paths.local / 'stewi'

# global variable to replace stored inventory files when saving
REPLACE_FILES = False

GIT_HASH_LONG = os.environ.get('GITHUB_SHA') or get_git_hash('long')
if GIT_HASH_LONG:
    GIT_HASH = GIT_HASH_LONG[0:7]
else:
    GIT_HASH = None

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

STEWI_DATA_VINTAGES = {
    'DMR': [x for x in range(2011, 2024, 1)],
    'GHGRP': [x for x in range(2011, 2024, 1)],
    'eGRID': [2014, 2016, 2018, 2019, 2020, 2021, 2022, 2023],
    'NEI': [2011, 2014, 2017, 2020, 2021, 2022],
    'RCRAInfo': [x for x in range(2011, 2024, 2)],
    'TRI': [x for x in range(2011, 2024, 1)],
}
'''A dictionary of StEWI inventories and their available vintages.'''


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


def linear_search(lst, target):
    """Backwards search a list for index less than or equal to a given value.

    :param lst: (list) A list of numerically sorted data (lowest to highest).
    :param target : (int, float) A target value (e.g., year).
    :return: (int)
        The index of the search list associated with the value equal to or
        less than the target, else -1 for a target out-of-range (i.e., smaller than the smallest entry in the list).

    :Example:

    >>> NEI_YEARS = [2011, 2014, 2017, 2020]
    >>> linear_search(NEI_YEARS, 2020)
    3
    >>> linear_search(NEI_YEARS, 2019)
    2
    >>> linear_search(NEI_YEARS, 2018)
    2
    >>> linear_search(NEI_YEARS, 2010)
    -1
    """
    for i in range(len(lst) - 1, -1, -1):
        if lst[i] <= target:
            return i
    return -1


def unit_convert(df, coln1, coln2, unit, conversion_factor, coln3):
    """Convert values in coln3 if coln2 == unit, based on the conversion
    factor, and assigns to coln1.
    """
    df[coln1] = np.where(df[coln2] == unit,
                         conversion_factor * df[coln3],
                         df[coln1])
    return df


def write_metadata(file_name, metadata_dict, category='',
                   datatype="inventory", parameters=None):
    """Write JSON metadata specific to inventory to local directory.

    :param file_name: str, in the form of inventory_year
    :param metadata_dict: dictionary of metadata to save
    :param category: str of a stewi format type e.g. 'flowbyfacility'
        or source category e.g. 'TRI Data Files'
    :param datatype: 'inventory' when saving StEWI output files, 'source'
        when downloading and processing source data, 'validation' for saving
        validation metadata
    :param parameters: list of parameters (str) to add to metadata
    """
    if (datatype == "inventory") or (datatype == "source"):
        meta = set_stewi_meta(file_name, stewiformat=category)
        if datatype == 'inventory':
            meta.tool_meta = {"parameters": parameters,
                              "sources": metadata_dict}
        else:
            meta.tool_meta = metadata_dict
        write_metadata_to_file(paths, meta)
    elif datatype == "validation":
        file = (paths.local_path / 'validation' /
                f'{file_name}_validationset_metadata.json')
        with file.open('w') as fi:
            fi.write(json.dumps(metadata_dict, indent=4))


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
    df.columns = df.columns.str.replace('\r|\n', ' ', regex=True)
    if not headers_only:
        df = df.replace('\r\n', ' ').replace('\n', ' ')
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
        df = df.rename(columns={'ReliabilityScore': 'DataReliability'})
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
    df = df[col_list].reset_index(drop=True)
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
    try:
        log.info(f'saving {meta.name_data} to {paths.local_path / meta.category}')
        write_df_to_file(df, paths, meta)
        if replace_files:
            remove_extra_files(meta, paths)
    except OSError:
        log.error('Failed to save inventory')


def read_inventory(inventory_acronym, year, f, download_if_missing=False):
    """Return the inventory from local directory. If not found, generate it.

    :param inventory_acronym: like 'TRI'
    :param year: year as number like 2010
    :param f: object of class StewiFormat
    :param download_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: dataframe of stored inventory; if not present returns None
    """
    file_name = f'{inventory_acronym}_{year}'
    meta = set_stewi_meta(file_name, str(f))
    inventory = load_preprocessed_output(meta, paths)
    method_path = paths.local_path / meta.category
    if inventory is None:
        log.info(f'{meta.name_data} not found in {method_path}')
        if download_if_missing:
            meta.tool = meta.tool.lower() # lower case for remote access
            download_from_remote(meta, paths)
            # download metadata file
            metadata_meta = copy.copy(meta)
            metadata_meta.category = ''
            metadata_meta.ext = 'json'
            download_from_remote(metadata_meta, paths)
        else:
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
        raise stewi.exceptions.InventoryNotAvailableError(
            message=f'"{inventory_acronym}" is not an available inventory')
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
    df = (pd.read_csv(DATA_PATH.joinpath(dq_file),
                      usecols=['Source', 'Code', 'DQI Reliability Score'])
            .query('Source == @source')
            .reset_index(drop=True)
            .drop(columns='Source'))
    return df


def assign_secondary_context(df, year, *args):
    """
    Wrapper for esupy.context_secondary.main(), which flexibly assigns
    urban/rural (pass 'urb' as positional arg) and/or release height ('rh')
    secondary compartments. Also choose whether to concatenate primary +
    secondary compartments by passing 'concat'.
    :param df: pd.DataFrame
    :param year: int, data year
    :param args: str, flag(s) for compartment assignment + skip_concat option
    """
    from esupy import context_secondary as e_c_s
    parameters = []
    df = e_c_s.main(df, year, *args)  # if e_c_s.has_geo_pkgs == False, returns unaltered df
    if 'cmpt_urb' in df.columns:  # rename before storage w/ facilities
        df = df.rename(columns={'cmpt_urb': 'UrbanRural'})
        parameters.append('urban_rural')
    if 'cmpt_rh' in df.columns:
        parameters.append('release_height')
    if 'concat' in args:
        df = concat_compartment(df)
    return df, parameters


def concat_compartment(df):
    """
    Concatenate primary & secondary compartment cols sequentially. If both
    'urb' and 'rh' are passed, return Compartment w/ order 'primary/urb/rh'.
    :param df: pd.DataFrame, including compartment cols
    """
    if 'UrbanRural' in df:
        df['Compartment'] = df['Compartment'] + '/' + df['UrbanRural']
    if 'cmpt_rh' in df:
        df['Compartment'] = df['Compartment'] + '/' + df['cmpt_rh']
    df['Compartment'] = df['Compartment'].str.replace('/unspecified','')
    return df
