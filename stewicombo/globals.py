# globals.py (stewicombo)
# !/usr/bin/env python3
# coding=utf-8
"""
Supporting variables and functions used in stewicombo
"""
import re
import os
import pandas as pd
from datetime import datetime

import chemicalmatcher
import stewi
from stewi.globals import log, set_stewi_meta, flowbyfacility_fields
from esupy.processed_data_mgmt import Paths, write_df_to_file,\
    write_metadata_to_file, load_preprocessed_output, read_into_df,\
    download_from_remote

try: modulepath = os.path.dirname(
    os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'stewicombo/'

data_dir = modulepath + 'data/'

#Common declaration of write format for package data products
write_format = "parquet"

paths = Paths()
paths.local_path = os.path.realpath(paths.local_path + "/stewicombo")
output_dir = paths.local_path

INVENTORY_PREFERENCE_BY_COMPARTMENT = {"air":["eGRID","GHGRP","NEI","TRI"],
                                       "water":["DMR", "TRI"],
                                       "soil":["TRI"],
                                       "waste":["RCRAInfo","TRI"],
                                       "output":["eGRID"]}

LOOKUP_FIELDS = ["FRS_ID", "Compartment", "SRS_ID"]
# pandas might infer wrong type, force cast skeptical columns
FORCE_COLUMN_TYPES = {
    "SRS_CAS": "str"
    }

KEEP_ALL_DUPLICATES =  True
INCLUDE_ORIGINAL =  True
KEEP_ROW_WITHOUT_DUPS = True
SOURCE_COL = "Source"
COMPARTMENT_COL = "Compartment"
COL_FUNC_PAIRS = {
    "FacilityID": "join_with_underscore",
    "FlowAmount": "sum",
    "DataReliability": "reliablity_weighted_sum:FlowAmount"
    }
COL_FUNC_DEFAULT = "get_first_item"

VOC_srs = pd.read_csv(data_dir+'VOC_SRS_IDs.csv',
                      dtype=str,index_col=False,header=0)
VOC_srs = VOC_srs['SRS_IDs']

def set_stewicombo_meta(file_name, category=''):
    """Creates a class of esupy FileMeta; category used for optional
    categorization"""
    stewicombo_meta = set_stewi_meta(file_name, category)
    stewicombo_meta.tool = "stewicombo"
    stewicombo_meta.ext = write_format
    return stewicombo_meta


def get_id_before_underscore(inventory_id):
    """Removes substring from inventory name"""
    underscore_match = re.search('_', inventory_id)
    if underscore_match is not None:
        inventory_id = inventory_id[0:underscore_match.start()]
    return inventory_id


def getInventoriesforFacilityMatches(inventory_dict, facilitymatches,
                                     filter_for_LCI, base_inventory=None):
    """
    Retrieves stored flowbyfacility datasets based on passed dictionary
    and filters them if necessary. Returns only those facilities with an FRS_ID
    except for those in the base_inventory where all are returned
    
    : param inventory_dict: 
    : param facilitymatches: 
    : param filter_for_LCI: 
    : param base_inventory:
    """
    if base_inventory is not None:
        # Identify the FRS in the base inventory and keep only those
        # base_inventory_FRS = facilitymatches[
        #     facilitymatches['Source'] == base_inventory]
        base_FRS_list = list(pd.unique(facilitymatches[
            facilitymatches['Source'] == base_inventory]['FRS_ID']))

    columns_to_keep = list(flowbyfacility_fields.keys()) + ['Source',
                                                            'Year','FRS_ID']
    inventories = pd.DataFrame()
    for k in inventory_dict.keys():
        inventory = stewi.getInventory(k, inventory_dict[k],
                                       'flowbyfacility', filter_for_LCI)
        if inventory is None:
            break
        inventory["Source"] = k
        # Merge in FRS_ID
        inventory = pd.merge(inventory,
                             facilitymatches[facilitymatches['Source'] == k],
                             on=['FacilityID', 'Source'], how='left')
        if inventory['FRS_ID'].isna().sum() > 0:
            log.debug('Some facilities missing FRS_ID')

        # If this isn't the base inventory, filter records for facilities not
        # found in the base inventory
        if (k is not base_inventory) & (base_inventory is not None):
            inventory = inventory[inventory['FRS_ID'].isin(
                base_FRS_list)]

        # Add metadata
        inventory["Year"] = inventory_dict[k]
        cols_to_keep = [c for c in columns_to_keep if c in inventory]
        inventory = inventory[cols_to_keep]
        inventories = pd.concat([inventories,inventory])

    return inventories


def addChemicalMatches(inventories_df):
    """Adds data for chemical matches to inventory or combined inventory df
    """
    #Bring in chemical matches
    inventory_list = list(inventories_df['Source'].unique())
    chemicalmatches = chemicalmatcher.get_matches_for_StEWI(
        inventory_list = inventory_list)
    chemicalmatches = chemicalmatches[
        chemicalmatches['Source'].isin(inventory_list)]
    chemicalmatches = chemicalmatches.drop(columns=['FlowID'])
    chemicalmatches = chemicalmatches.drop_duplicates(subset=['FlowName',
                                                              'Source'])
    inventories = pd.merge(inventories_df,
                           chemicalmatches,
                           on=['FlowName','Source'],
                           how='left')
    # Compare unmatched flows to flows_missing_SRS_ list to ensure none missing
    missing_flows = inventories.loc[
        inventories['SRS_ID'].isna()][['FlowName','Source']].drop_duplicates()
    cm_missing = chemicalmatcher.read_cm_file('missing')
    missing_flows = missing_flows.assign(missing = missing_flows['FlowName'].\
                                         isin(cm_missing['FlowName'])==False)
    if sum(missing_flows.missing)>0:
        log.warning('New unknown flows identified, run chemicalmatcher')
        
    return inventories


def addBaseInventoryIDs(inventories,facilitymatches,base_inventory):
    #Add in base program ids
    base_inventory_FRS = facilitymatches[
        facilitymatches['Source'] == base_inventory]
    base_inventory_FRS = base_inventory_FRS[['FacilityID','FRS_ID']]

    #If there are more than one PGM_SYS_ID duplicates, choose only the first
    base_inventory_FRS_first = base_inventory_FRS.drop_duplicates(
        subset='FRS_ID',keep='first')
    colname_base_inventory_id = base_inventory + '_ID'
    base_inventory_FRS_first = base_inventory_FRS_first.rename(
        columns={"FacilityID":colname_base_inventory_id})
    #Merge this based with inventories
    inventories = pd.merge(inventories,base_inventory_FRS_first,on='FRS_ID',
                           how='left')
    #Put original facilityID into the new column when its is the source of 
    # the emission. This corrects mismatches in the case of more than 
    # one base inventory id to FRS_ID
    inventory_acronyms = pd.unique(inventories['Source'])
    if base_inventory in inventory_acronyms:
        #The presence of an underscore indicates more than one facilityid 
        # was used. If this is the case, get it before the underscore
        inventories['FacilityID_first'] = inventories['FacilityID']
        inventories['FacilityID_first'] = inventories['FacilityID_first'].\
            apply(lambda x: get_id_before_underscore(x))
        inventories.loc[inventories['Source']==base_inventory,
                        colname_base_inventory_id] = inventories['FacilityID_first']
        inventories = inventories.drop(columns='FacilityID_first')
    return inventories

def storeCombinedInventory(df, file_name, category=''):
    """Stores the inventory dataframe to local directory based on category"""
    meta = set_stewicombo_meta(file_name, category)
    method_path = output_dir + '/' + meta.category
    try:
        log.info('saving ' + meta.name_data + ' to ' + method_path)
        write_df_to_file(df,paths,meta)
    except:
        log.error('Failed to save inventory')

def getCombinedInventory(name, category=''):
    """Reads the inventory dataframe from local directory
    :param name: str, name of dataset or name of file
    """
    if ("."+write_format) in name:
        method_path = output_dir + '/' + category
        inventory = read_into_df(method_path + name)
    else:
        meta = set_stewicombo_meta(name, category)
        method_path = output_dir + '/' + meta.category
        inventory = load_preprocessed_output(meta, paths)
    if inventory is None:
        log.info('%s not found in %s', name, method_path)
    else:
        log.info('loaded %s from %s',name, method_path)    
    return inventory

def download_stewicombo_from_remote(name):
    """Prepares metadata and downloads file via esupy"""
    meta = set_stewicombo_meta(name, category = '')
    log.info('attempting download of %s from %s', name, paths.remote_path)
    download_from_remote(meta, paths)
    

def write_metadata(file_name, metadata_dict, category=''):
    meta = set_stewicombo_meta(file_name, category=category)
    meta.tool_meta = metadata_dict
    write_metadata_to_file(paths, meta)

def compile_metadata(inventory_dict):
    inventory_meta = {}
    #inventory_meta['InventoryDictionary'] = inventory_dict
    creation_time = datetime.now().strftime('%d-%b-%Y')
    if creation_time is not None:
        inventory_meta['InventoryGenerationDate'] = creation_time
    for source, year in inventory_dict.items():
        inventory_meta[source] = stewi.getMetadata(source, year)
    
    return inventory_meta

def filter_by_compartment(df, compartments):
    #TODO disaggregate compartments to include all children
    df = df[df['Compartment'].isin(compartments)]
    return df

