#!/usr/bin/env python

import pandas as pd
pd.options.mode.chained_assignment = None
import json
import logging as log
import os
import numpy as np
import yaml
import time
import subprocess
from datetime import datetime
from esupy.processed_data_mgmt import Paths, FileMeta, load_preprocessed_output,\
    write_df_to_file, create_paths_if_missing, write_metadata_to_file
from esupy.dqi import get_weighted_average

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'stewi/'

data_dir = modulepath + 'data/'

log.basicConfig(level=log.INFO, format='%(levelname)s %(message)s')
stewi_version = '0.9.8'

#Common declaration of write format for package data products
write_format = "parquet"

paths = Paths()
paths.local_path = os.path.realpath(paths.local_path + "/stewi")
output_dir = paths.local_path

try:
    git_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip().decode(
        'ascii')[0:7]
except:
    git_hash = None

reliability_table = pd.read_csv(data_dir + 'DQ_Reliability_Scores_Table3-3fromERGreport.csv',
                                usecols=['Source', 'Code', 'DQI Reliability Score'])

stewi_formats = ['flowbyfacility', 'flow', 'facility', 'flowbyprocess']
inventory_formats = ['flowbyfacility', 'flowbyprocess']

source_metadata = {
    'SourceType': 'Static File',  #Other types are "Web service"
    'SourceFileName':'NA',
    'SourceURL':'NA',
    'SourceVersion':'NA',
    'SourceAcquisitionTime':'NA',
    'StEWI_Version':stewi_version,
    }

inventory_single_compartments = {"NEI":"air","RCRAInfo":"waste","GHGRP":"air"}


def set_stewi_meta(file_name, category):
    stewi_meta = FileMeta()
    stewi_meta.name_data = file_name
    stewi_meta.category = category
    stewi_meta.tool = "StEWI"
    stewi_meta.tool_version = stewi_version
    stewi_meta.ext = write_format
    stewi_meta.git_hash = git_hash
    return stewi_meta


def config(config_path=modulepath):
    configfile = None
    with open(config_path + 'config.yaml', mode='r') as f:
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
        


def set_dir(directory_name):
    path = os.path.realpath(directory_name + '/').replace('\\', '/') + '/'
    if os.path.exists(path): pathname = path
    else:
        pathname = path
        os.makedirs(pathname)
    return pathname


def import_table(path_or_reference, skip_lines=0, get_time=False):
    if '.core.frame.DataFrame' in str(type(path_or_reference)): df = path_or_reference
    elif path_or_reference[-3:].lower() == 'csv':
        df = pd.read_csv(path_or_reference, low_memory=False)
    elif 'xls' in path_or_reference[-4:].lower():
        import_file = pd.ExcelFile(path_or_reference)
        df = {sheet: import_file.parse(sheet, skiprows=skip_lines) for sheet in import_file.sheet_names}
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


def filter_inventory(inventory, criteria_table, filter_type, marker=None):
    """
    :param inventory_df: DataFrame to be filtered
    :param criteria_file: Can be a list of items to drop/keep, or a table of FlowName, FacilityID, etc. with columns
                          marking rows to drop
    :param filter_type: drop, keep, mark_drop, mark_keep
    :param marker: Non-empty fields are considered marked by default. Option to specify 'x', 'yes', '1', etc.
    :return: DataFrame
    """
    inventory = import_table(inventory); criteria_table = import_table(criteria_table)
    if filter_type in ('drop', 'keep'):
        for criteria_column in criteria_table:
            for column in inventory:
                if column == criteria_column:
                    criteria = set(criteria_table[criteria_column])
                    if filter_type == 'drop': inventory = inventory[~inventory[column].isin(criteria)]
                    elif filter_type == 'keep': inventory = inventory[inventory[column].isin(criteria)]
    elif filter_type in ('mark_drop', 'mark_keep'):
        standard_format = import_table(data_dir + 'flowbyfacility_format.csv')
        must_match = standard_format['Name'][standard_format['Name'].isin(criteria_table.keys())]
        for criteria_column in criteria_table:
            if criteria_column in must_match: continue
            for field in must_match:
                if filter_type == 'mark_drop':
                    if marker is None: inventory = inventory[~inventory[field].isin(criteria_table[field][criteria_table[criteria_column] != ''])]
                    else: inventory = inventory[~inventory[field].isin(criteria_table[field][criteria_table[criteria_column] == marker])]
                if filter_type == 'mark_keep':
                    if marker is None: inventory = inventory[inventory[field].isin(criteria_table[field][criteria_table[criteria_column] != ''])]
                    else: inventory = inventory[inventory[field].isin(criteria_table[field][criteria_table[criteria_column] == marker])]
    return inventory.reset_index(drop=True)


def filter_states(inventory_df, include_states=True, include_dc=True, include_territories=False):
    states_df = pd.read_csv(data_dir + 'state_codes.csv')
    states_filter = pd.DataFrame()
    states_list = []
    if include_states: states_list += list(states_df['states'].dropna())
    if include_dc: states_list += list(states_df['dc'].dropna())
    if include_territories: states_list += list(states_df['territories'].dropna())
    states_filter['State'] = states_list
    output_inventory = filter_inventory(inventory_df, states_filter, filter_type='keep')
    return output_inventory


def validate_inventory(inventory_df, reference_df, group_by='flow', tolerance=5.0, filepath=''):
    """
    Compare inventory resulting from script output with a reference DataFrame from another source
    :param inventory_df: DataFrame of inventory resulting from script output
    :param reference_df: Reference DataFrame to compare emission quantities against. Must have same keys as inventory_df
    :param group_by: 'flow' for species summed across facilities, 'facility' to check species by facility
    :param tolerance: Maximum acceptable percent difference between inventory and reference values
    :return: DataFrame containing 'Conclusion' of statistical comparison and 'Percent_Difference'
    """
    if pd.api.types.is_string_dtype(inventory_df['FlowAmount']):
        inventory_df['FlowAmount'] = inventory_df['FlowAmount'].str.replace(',', '')
        inventory_df['FlowAmount'] = pd.to_numeric(inventory_df['FlowAmount'])
    if pd.api.types.is_string_dtype(reference_df['FlowAmount']):
        reference_df['FlowAmount'] = reference_df['FlowAmount'].str.replace(',', '')
        reference_df['FlowAmount'] = pd.to_numeric(reference_df['FlowAmount'])
    if group_by == 'flow':
        group_by_columns = ['FlowName']
        if 'Compartment' in inventory_df.keys(): group_by_columns += ['Compartment']
    elif group_by == 'state':
        group_by_columns = ['State']
    elif group_by == 'facility':
        group_by_columns = ['FlowName', 'FacilityID']
    elif group_by == 'subpart':
        group_by_columns = ['FlowName', 'SubpartName']
    inventory_df['FlowAmount'] = inventory_df['FlowAmount'].fillna(0.0)
    reference_df['FlowAmount'] = reference_df['FlowAmount'].fillna(0.0)
    inventory_sums = inventory_df[group_by_columns + ['FlowAmount']].groupby(group_by_columns).sum().reset_index()
    reference_sums = reference_df[group_by_columns + ['FlowAmount']].groupby(group_by_columns).sum().reset_index()
    if filepath: reference_sums.to_csv(filepath, index=False)
    validation_df = inventory_sums.merge(reference_sums, how='outer', on=group_by_columns).reset_index(drop=True)
    validation_df = validation_df.fillna(0.0)
    amount_x_list = []
    amount_y_list = []
    pct_diff_list = []
    conclusion = []
    error_count = 0
    for index, row in validation_df.iterrows():
        amount_x = float(row['FlowAmount_x'])
        amount_y = float(row['FlowAmount_y'])
        if amount_x == 0.0:
            amount_x_list.append(amount_x)
            if amount_y == 0.0:
                pct_diff_list.append(0.0)
                amount_y_list.append(amount_y)
                conclusion.append('Both inventory and reference are zero or null')
            elif amount_y == np.inf:
                amount_y_list.append(np.nan)
                pct_diff_list.append(100.0)
                conclusion.append('Reference contains infinity values. Check prior calculations.')
            else:
                amount_y_list.append(amount_y)
                pct_diff_list.append(100.0)
                conclusion.append('Inventory value is zero or null')
                error_count += 1
            continue
        elif amount_y == 0.0:
            amount_x_list.append(amount_x)
            amount_y_list.append(amount_y)
            pct_diff_list.append(100.0)
            conclusion.append('Reference value is zero or null')
            continue
        elif amount_y == np.inf:
            amount_x_list.append(amount_x)
            amount_y_list.append(np.nan)
            pct_diff_list.append(100.0)
            conclusion.append('Reference contains infinity values. Check prior calculations.')
        else:
            pct_diff = 100.0 * abs(amount_y - amount_x) / amount_y
            pct_diff_list.append(pct_diff)
            amount_x_list.append(amount_x)
            amount_y_list.append(amount_y)
            if pct_diff == 0.0: conclusion.append('Identical')
            elif pct_diff <= tolerance: conclusion.append('Statistically similar')
            elif pct_diff > tolerance:
                conclusion.append('Percent difference exceeds tolerance')
                error_count += 1
    validation_df['Inventory_Amount'] = amount_x_list
    validation_df['Reference_Amount'] = amount_y_list
    validation_df['Percent_Difference'] = pct_diff_list
    validation_df['Conclusion'] = conclusion
    validation_df = validation_df.drop(['FlowAmount_x', 'FlowAmount_y'], axis=1)
    if error_count > 0:
        log.warning('%s potential issues in validation exceeding tolerance', str(error_count))

    return validation_df


def read_ValidationSets_Sources():
    df = pd.read_csv(data_dir + 'ValidationSets_Sources.csv',header=0,
                     dtype={"Year":"str"})
    return df


def write_validation_result(inventory_acronym,year,validation_df):
    """Writes the validation result and associated metadata to the output"""
    directory = output_dir + '/validation/'
    create_paths_if_missing(directory)
    log.info('writing validation result to ' + directory)
    validation_df.to_csv(directory + inventory_acronym + '_' + year + '.csv',index=False)
    #Get metadata on validation dataset
    validation_set_info_table = read_ValidationSets_Sources()
    #Get record for year and
    validation_set_info = validation_set_info_table[(validation_set_info_table['Inventory']==inventory_acronym)&
                                                     (validation_set_info_table['Year']==year)]
    if len(validation_set_info)!=1:
        log.error('no validation metadata found')
        return
    #Convert to Series
    validation_set_info = validation_set_info.iloc[0,]
    #Use the same format an inventory metadata to described the validation set data
    validation_metadata = dict(source_metadata)
    validation_metadata['SourceFileName'] = validation_set_info['Name']
    validation_metadata['SourceVersion'] = validation_set_info['Version']
    validation_metadata['SourceURL'] = validation_set_info['URL']
    validation_metadata['SourceAcquisitionTime'] = validation_set_info['Date Acquired']
    validation_metadata['Criteria'] = validation_set_info['Criteria']
    #Write metadata to file
    write_metadata(inventory_acronym + '_' + year, validation_metadata, datatype="validation")


def update_validationsets_sources(validation_dict, date_acquired=False):
    if not date_acquired:
        date = datetime.today().strftime('%d-%b-%Y')
        validation_dict['Date Acquired'] = date
    v_table = read_ValidationSets_Sources()
    existing = v_table.loc[(v_table['Inventory'] == validation_dict['Inventory']) &
                       (v_table['Year'] == validation_dict['Year'])]
    if len(existing)>0:
        i = existing.index[0]
        v_table = v_table.loc[~v_table.index.isin(existing.index)]
        line = pd.DataFrame.from_records([validation_dict], index=[(i)])
    else:
        inventories = list(v_table['Inventory'])
        i = max(loc for loc, val in enumerate(inventories) if val == validation_dict['Inventory'])
        line = pd.DataFrame.from_records([validation_dict], index=[(i+0.5)])
    v_table = v_table.append(line, ignore_index=False)
    v_table = v_table.sort_index().reset_index(drop=True)
    log.info('updating ValidationSets_Sources.csv with %s %s', 
             validation_dict['Inventory'], validation_dict['Year'])
    v_table.to_csv(data_dir + 'ValidationSets_Sources.csv', index=False)
    

def validation_summary(validation_df, filepath=''):
    """
    Summarized output of validate_inventory function
    :param validation_df:
    :return: DataFrame containing 'Count' of each statistical conclusion and 'Avg_Pct_Difference'
    """
    validation_df['Count'] = validation_df['Conclusion']
    validation_summary_df = validation_df[['Count', 'Conclusion']].groupby('Conclusion').count()
    validation_summary_df['Avg_Pct_Difference'] = validation_df[['Percent_Difference', 'Conclusion']].groupby('Conclusion').mean()
    validation_summary_df.reset_index(inplace=True)
    if filepath: validation_summary_df.to_csv(filepath, index=False)
    return validation_summary_df

def aggregate(df, grouping_vars):
    """
    Aggregate a 'FlowAmount' in a dataframe based on the passed grouping_vars
    and generating a weighted average for data quality fields
    """
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
    df[coln1][df[coln2] == unit] = conversion_factor * df[coln3]
    return df
#Conversion factors
USton_kg = 907.18474
lb_kg = 0.4535924
MMBtu_MJ = 1055.056
MWh_MJ = 3600
g_kg = 0.001

# Writes the metadata dictionary to a JSON file
def write_metadata(file_name, metadata_dict, metapath=None, category='', datatype="inventory"):
    if (datatype == "inventory") or (datatype == "source"):
        meta = set_stewi_meta(file_name, category=category)
        meta.tool_meta = metadata_dict
        write_metadata_to_file(paths, meta)
        #with open(output_dir + '/' + inventoryname + '_' + report_year + '_metadata.json', 'w') as file:
        #    file.write(json.dumps(metadata_dict))
    elif datatype == "validation":
        with open(output_dir + '/validation/' + file_name + '_validationset_metadata.json', 'w') as file:
            file.write(json.dumps(metadata_dict, indent=4))


# Returns the metadata dictionary for an inventory
def read_source_metadata(path):
    try:
        with open(path + '_metadata.json', 'r') as file:
            file_contents = file.read()
            metadata = json.loads(file_contents)
            return metadata
    except FileNotFoundError:
        log.warning("metadata not found for source data")
        return None

def compile_source_metadata(sourcefile, config, year):
    """Compiles metadata related to the source data downloaded to generate inventory
    :param sourcefile: str or list of source file names
    :param config:
    :param year:
    returns: dictionary in the format of source_metadata
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

# ReliabilityScore maintained in dict for accessing legacy datasets
flowbyfacility_fields = {'FlowName': [{'dtype': 'str'}, {'required': True}],
                         'Compartment': [{'dtype': 'str'}, {'required': True}],
                         'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                         'FacilityID': [{'dtype': 'str'}, {'required': True}],
                         'DataReliability': [{'dtype': 'float'}, {'required': True}],
                         'Unit': [{'dtype': 'str'}, {'required': True}],
                         'ReliabilityScore': [{'dtype': 'float'}, {'required': False}],
                         }

facility_fields = {'FacilityID':[{'dtype': 'str'}, {'required': True}],
                   'FacilityName':[{'dtype': 'str'}, {'required': False}],
                   'Address':[{'dtype': 'str'}, {'required': False}],
                   'City':[{'dtype': 'str'}, {'required': False}],
                   'State':[{'dtype': 'str'}, {'required': True}],
                   'Zip':[{'dtype': 'int'}, {'required': False}],
                   'Latitude':[{'dtype': 'float'}, {'required': False}],
                   'Longitude':[{'dtype': 'float'}, {'required': False}],
                   'County':[{'dtype': 'str'}, {'required': False}],
                   'NAICS':[{'dtype': 'str'}, {'required': False}],
                   'SIC':[{'dtype': 'str'}, {'required': False}],
                   }

flowbyprocess_fields = {'FlowName': [{'dtype': 'str'}, {'required': True}],
                    'Compartment': [{'dtype': 'str'}, {'required': True}],
                    'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                    'FacilityID': [{'dtype': 'str'}, {'required': True}],
                    'DataReliability': [{'dtype': 'float'}, {'required': True}],
                    'Unit': [{'dtype': 'str'}, {'required': True}],
                    'Process': [{'dtype': 'str'}, {'required': True}],
                    'ProcessType': [{'dtype': 'str'}, {'required': False}],
                    'ReliabilityScore': [{'dtype': 'float'}, {'required': False}],                    
                    }

format_dict = {'flowbyfacility': flowbyfacility_fields,
               'flowbyprocess': flowbyprocess_fields}

def get_required_fields(format='flowbyfacility'):
    fields = format_dict[format]
    required_fields = {key: value[0]['dtype'] for key, value in fields.items() if value[1]['required'] is True}
    return required_fields


def get_optional_fields(format='flowbyfacility'):
    fields = format_dict[format]
    optional_fields = {key: value[0]['dtype'] for key, value in fields.items()}
    return optional_fields


def add_missing_fields(df, inventory_acronym, format='flowbyfacility'):
    fields = dict(format_dict[format])
    # Rename for legacy datasets
    if 'ReliabilityScore' in df.columns:
        df.rename(columns={'ReliabilityScore':'DataReliability'}, inplace=True)
    del fields['ReliabilityScore']
    # Add in units and compartment if not present
    if 'Unit' not in df.columns:
        df['Unit'] = 'kg'
    if 'Compartment' not in df.columns:
        df['Compartment'] = inventory_single_compartments[inventory_acronym]
    for key in fields.keys():
        if key not in df.columns:
            df[key] = None
    # Resort
    df = df[fields.keys()]
    return df

def checkforFile(filepath):
    return os.path.exists(filepath)


def get_relpath(filepath):
    return os.path.relpath(filepath, '.').replace('\\', '/') + '/'


def storeInventory(df, file_name, category):
    """Stores the inventory dataframe to local directory based on category"""
    meta = set_stewi_meta(file_name, category)
    method_path = output_dir + '/' + meta.category
    try:
        log.info('saving ' + meta.name_data + ' to ' + method_path)
        write_df_to_file(df,paths,meta)
    except:
        log.error('Failed to save inventory')

def readInventory(file_name, category):
    """Returns the inventory as dataframe from local directory"""
    meta = set_stewi_meta(file_name, category)
    inventory = load_preprocessed_output(meta, paths)
    method_path = output_dir + '/' + meta.category
    if inventory is None:
        log.info(meta.name_data + ' not found in ' + method_path)
    else:
        log.info('loaded ' + meta.name_data + ' from ' + method_path)    
    return inventory
