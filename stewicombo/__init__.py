import pandas as pd
import os
import logging
import stewi
import facilitymatcher
import chemicalmatcher

from stewicombo.globals import inventory_preference_by_compartment

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'stewicombo/'

inventory_to_FRS_pgm_acrynoym = {"NEI":"EIS","TRI":"TRIS","eGRID":"EGRID","GHGRP":"E-GGRT","RCRAInfo":"RCRAINFO"}

compartments = {"NEI":"air","RCRAInfo":"waste"}
columns_to_keep = ['FacilityID', 'FlowAmount', 'FlowName','Compartment','ReliabilityScore','Source','Year']

#For testing
#inventory_dict = {"TRI":"2014","NEI":"2014","RCRAInfo":"2015"}

def combineFullInventories(inventory_dict):
    inventories = pd.DataFrame()
    for k in inventory_dict.keys():
        inventory = stewi.getInventory(k,inventory_dict[k],include_optional_fields=True)
        inventory["Source"] = k
        inventory["Year"] = inventory_dict[k]
        if 'Compartment' not in inventory.columns:
            inventory["Compartment"] = compartments[k]
        inventory = inventory[columns_to_keep]
        inventories = pd.concat([inventories,inventory])

    #Bring in facility matches
    #get only those inventorytoFRS_pgm correspondence that are needed
    inventory_dict_keylist = list(inventory_dict.keys())
    facilitymatches = facilitymatcher.get_matches_for_inventories(inventory_dict_keylist)

    facilitymatches.rename(columns={"REGISTRY_ID":"FRS_ID"},inplace=True)
    inventories = pd.merge(inventories,facilitymatches,left_on=(['FacilityID','Source']),right_on=(['PGM_SYS_ID','PGM_SYS_ACRNM']),how='left')
    inventories.drop(columns=['PGM_SYS_ID','PGM_SYS_ACRNM'],inplace=True)

    #Bring in chemical matches
    chemicalmatches = chemicalmatcher.get_matches_for_StEWI()
    inventories = pd.merge(inventories,chemicalmatches,on=(['FlowName','Source']),how='left')
    return(inventories)

#For testing
#base_inventory = "eGRID"
def combineInventoriesforFacilitiesinOneInventory(base_inventory, inventory_dict):
    #Bring in facility matches
    #get only those inventorytoFRS_pgm correspondence that are needed
    inventory_acronyms = list(inventory_dict.keys())
    facilitymatches = facilitymatcher.get_table_of_matches_from_inventory_to_inventories_of_interest(base_inventory,inventory_dict)
    facilitymatches.rename(columns={"REGISTRY_ID":"FRS_ID"},inplace=True)

    inventories = pd.DataFrame()
    for k in inventory_dict.keys():
        inventory = stewi.getInventory(k,inventory_dict[k],include_optional_fields=True)
        #Get facilities from that matching table to filter this with
        inventory_facilitymatches = facilitymatches[facilitymatches['PGM_SYS_ACRNM_y'] == k]
        inventory_facilitylist = list(inventory_facilitymatches['PGM_SYS_ID_y'])
        #Filter inventory by facility list
        inventory = inventory[inventory['FacilityID'].isin(inventory_facilitylist)]
        #Add metadata
        inventory["Source"] = k
        inventory["Year"] = inventory_dict[k]
        if 'Compartment' not in inventory.columns:
            inventory["Compartment"] = compartments[k]
        inventory = inventory[columns_to_keep]
        inventories = pd.concat([inventories,inventory])

    #Merge inventories based on facility matches
    inventories = pd.merge(inventories,facilitymatches,left_on=(['FacilityID','Source']),right_on=(['PGM_SYS_ID_y','PGM_SYS_ACRNM_y']),how='left')
    #drop duplicates - not sure why there are duplicates
    inventories = inventories.drop_duplicates()
    inventories.drop(columns=['PGM_SYS_ID_y','PGM_SYS_ACRNM_y'],inplace=True)

    #Bring in chemical matches
    chemicalmatches = chemicalmatcher.get_matches_for_StEWI()
    inventories = pd.merge(inventories,chemicalmatches,on=(['FlowName','Source']),how='left')
    colname_base_inventory_id = base_inventory + '_ID'
    inventories.rename(columns={"PGM_SYS_ID_x":colname_base_inventory_id},inplace=True)
    inventories.drop(columns=['PGM_ID','PGM_SYS_ACRNM_x'],inplace=True)
    return(inventories)


def pivotCombinedInventories(combinedinventory_df):
    #Group the results by facility,flow,and compartment
    #Use a pivot table
    combinedinventory_df_pt  = combinedinventory_df.pivot_table(values=['FlowAmount','ReliabilityScore'],index=['FRS_ID','SRS_ID','Compartment'],columns='Source')
    return(combinedinventory_df_pt)
    #len(inventories_withFRSID_SRSID_pt[(inventories_withFRSID_SRSID_pt['FlowAmount']['TRI'] > 0) & (inventories_withFRSID_SRSID_pt['FlowAmount']['NEI'] > 0)])
    #inventories_withFRSID_SRSID_pt.to_csv('inventories_withFRSID_SRSID_pt.csv')
