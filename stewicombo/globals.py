import re
import os
import pandas as pd
from datetime import datetime

import chemicalmatcher
import stewi
from stewi.globals import log, set_stewi_meta, read_source_metadata,\
    flowbyfacility_fields
from esupy.processed_data_mgmt import Paths, write_df_to_file, write_metadata_to_file

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
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

VOC_srs = pd.read_csv(data_dir+'VOC_SRS_IDs.csv',dtype=str,index_col=False,header=0)
VOC_srs = VOC_srs['SRS_IDs']

def set_stewicombo_meta(file_name, category):
    stewicombo_meta = set_stewi_meta(file_name, category)
    stewicombo_meta.tool = "stewicombo"
    stewicombo_meta.ext = write_format
    return stewicombo_meta


#Remove substring from inventory name
def get_id_before_underscore(inventory_id):
    underscore_match = re.search('_', inventory_id)
    if underscore_match is not None:
        inventory_id = inventory_id[0:underscore_match.start()]
    return inventory_id


def getInventoriesforFacilityMatches(inventory_dict,facilitymatches,filter_for_LCI,base_inventory=None):

    if base_inventory is not None:
        base_inventory_FRS = facilitymatches[facilitymatches['Source'] == base_inventory]
        base_inventory_FRS_list = list(pd.unique(base_inventory_FRS['FRS_ID']))

    columns_to_keep = list(flowbyfacility_fields.keys()) + ['Source','Year','FRS_ID']
    inventories = pd.DataFrame()
    for k in inventory_dict.keys():
        inventory = stewi.getInventory(k,inventory_dict[k],'flowbyfacility',filter_for_LCI)
        #Get facilities from that matching table to filter this with
        inventory_facilitymatches = facilitymatches[facilitymatches['Source'] == k]
        inventory["Source"] = k

        # Merge inventories based on facility matches
        inventory = pd.merge(inventory, inventory_facilitymatches, on=['FacilityID', 'Source'], how='left')

        # If this isn't the base inventory, remove records not for the FRS_IDs of interest
        if (k is not base_inventory) & (base_inventory is not None):
            inventory = inventory[inventory['FRS_ID'].isin(base_inventory_FRS_list)]

        #Add metadata
        inventory["Year"] = inventory_dict[k]
        cols_to_keep = [c for c in columns_to_keep if c in inventory]
        inventory = inventory[cols_to_keep]
        inventories = pd.concat([inventories,inventory])

    #drop duplicates - not sure why there are duplicates - none found in recent attempts
    inventories = inventories.drop_duplicates()
    return inventories


def addChemicalMatches(inventories_df):
    #Bring in chemical matches
    inventory_list = list(inventories_df['Source'].unique())
    chemicalmatches = chemicalmatcher.get_matches_for_StEWI(inventory_list = inventory_list)
    chemicalmatches = chemicalmatches.drop(columns=['FlowID'])
    chemicalmatches = chemicalmatches.drop_duplicates(subset=['FlowName','Source'])
    inventories = pd.merge(inventories_df,chemicalmatches,on=(['FlowName','Source']),how='left')
    return inventories


def addBaseInventoryIDs(inventories,facilitymatches,base_inventory):
    #Add in base program ids
    base_inventory_FRS = facilitymatches[facilitymatches['Source'] == base_inventory]
    base_inventory_FRS = base_inventory_FRS[['FacilityID','FRS_ID']]

    #If there are more than one PGM_SYS_ID duplicates, choose only the first
    base_inventory_FRS_first = base_inventory_FRS.drop_duplicates(subset='FRS_ID',keep='first')
    colname_base_inventory_id = base_inventory + '_ID'
    base_inventory_FRS_first = base_inventory_FRS_first.rename(columns={"FacilityID":colname_base_inventory_id})
    #Merge this based with inventories
    inventories = pd.merge(inventories,base_inventory_FRS_first,on='FRS_ID',how='left')
    #Put original facilityID into the new column when its is the source of the emission. This corrects mismatches
    #in the case of more than one base inventory id to FRS_ID
    inventory_acronyms = pd.unique(inventories['Source'])
    if base_inventory in inventory_acronyms:
        #The presence of an underscore indicates more than one facilityid was used. If this is the case, get it before the underscore
        inventories['FacilityID_first'] = inventories['FacilityID']
        inventories['FacilityID_first'] = inventories['FacilityID_first'].apply(lambda x: get_id_before_underscore(x))
        inventories.loc[inventories['Source']==base_inventory,colname_base_inventory_id] = inventories['FacilityID_first']
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
