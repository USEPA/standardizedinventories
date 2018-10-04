import pandas as pd
#import os
import stewi
import facilitymatcher
import chemicalmatcher

from stewicombo.overlaphandler import aggregate_and_remove_overlap
from stewicombo.globals import get_id_before_underscore

columns_to_keep = ['FacilityID', 'FlowAmount', 'FlowName','Compartment','Unit','ReliabilityScore','Source','Year','FRS_ID']

#For testing
#inventory_dict = {"TRI":"2014","NEI":"2014","RCRAInfo":"2015"}
#This function has not been updated
# def combineFullInventories(inventory_dict):
#     inventories = pd.DataFrame()
#     for k in inventory_dict.keys():
#         inventory = stewi.getInventory(k,inventory_dict[k],include_optional_fields=True)
#         inventory["Source"] = k
#         inventory["Year"] = inventory_dict[k]
#         if 'Compartment' not in inventory.columns:
#             inventory["Compartment"] = compartments[k]
#         inventory = inventory[columns_to_keep]
#         inventories = pd.concat([inventories,inventory])
#
#     #Bring in facility matches
#     #get only those inventorytoFRS_pgm correspondence that are needed
#     inventory_dict_keylist = list(inventory_dict.keys())
#     facilitymatches = facilitymatcher.get_matches_for_inventories(inventory_dict_keylist)
#
#     facilitymatches.rename(columns={"REGISTRY_ID":"FRS_ID"},inplace=True)
#     inventories = pd.merge(inventories,facilitymatches,left_on=(['FacilityID','Source']),right_on=(['PGM_SYS_ID','PGM_SYS_ACRNM']),how='left')
#     inventories.drop(columns=['PGM_SYS_ID','PGM_SYS_ACRNM'],inplace=True)
#
#     #Bring in chemical matches
#     chemicalmatches = chemicalmatcher.get_matches_for_StEWI()
#     inventories = pd.merge(inventories,chemicalmatches,on=(['FlowName','Source']),how='left')
#     return(inventories)

def combineInventoriesforFacilitiesinOneInventory(base_inventory, inventory_dict, filter_for_LCI=True,remove_overlap=True ):
    #Bring in facility matches
    #get only those inventorytoFRS_pgm correspondence that are needed
    inventory_acronyms = list(inventory_dict.keys())
    #facilitymatches = facilitymatcher.get_table_of_matches_from_inventory_to_inventories_of_interest(base_inventory,inventory_dict)
    facilitymatches = facilitymatcher.get_matches_for_inventories(inventory_acronyms)
    #facilitymatches.rename(columns={"REGISTRY_ID":"FRS_ID"},inplace=True)

    #Get FRS ids for the base inventory
    base_inventory_FRS = facilitymatches[facilitymatches['Source']==base_inventory]
    base_inventory_FRS_list = list(pd.unique(base_inventory_FRS['FRS_ID']))

    inventories = pd.DataFrame()
    for k in inventory_dict.keys():
        inventory = stewi.getInventory(k,inventory_dict[k],filter_for_LCI=filter_for_LCI)
        #Get facilities from that matching table to filter this with
        inventory_facilitymatches = facilitymatches[facilitymatches['Source'] == k]
        inventory_facilitylist = list(inventory_facilitymatches['FacilityID'])
        #Filter inventory by facility list
        #inventory = inventory[inventory['FacilityID'].isin(inventory_facilitylist)]
        inventory["Source"] = k

        #Bring in facility matches
        # Merge inventories based on facility matches
        inventory = pd.merge(inventory, inventory_facilitymatches, on=['FacilityID', 'Source'], how='left')

        # Filter out inventories based on FRS_IDs present in base inventory matches
        if k is not base_inventory:
            inventory = inventory[inventory['FRS_ID'].isin(base_inventory_FRS_list)]

        #Add metadata
        inventory["Year"] = inventory_dict[k]
        inventory = inventory[columns_to_keep]
        inventories = pd.concat([inventories,inventory])


    #drop duplicates - not sure why there are duplicates
    inventories = inventories.drop_duplicates()

    #Bring in chemical matches
    chemicalmatches = chemicalmatcher.get_matches_for_StEWI()
    chemicalmatches = chemicalmatches.drop_duplicates()
    inventories = pd.merge(inventories,chemicalmatches,on=(['FlowName','Source']),how='left')
    inventories = inventories.drop(columns=['FlowID'])

    #Aggregate and remove overlap if requested
    if remove_overlap:
        inventories = aggregate_and_remove_overlap(inventories)

    #Add in base program ids

    base_inventory_FRS = base_inventory_FRS[['FacilityID','FRS_ID']]
    #If there are more than one PGM_SYS_ID duplicates, choose only the first
    base_inventory_FRS_first = base_inventory_FRS.drop_duplicates(subset='FRS_ID',keep='first')
    colname_base_inventory_id = base_inventory + '_ID'
    base_inventory_FRS_first = base_inventory_FRS_first.rename(columns={"FacilityID":colname_base_inventory_id})
    #Merge this based with inventories
    inventories = pd.merge(inventories,base_inventory_FRS_first,on='FRS_ID',how='left')
    #Put original facilityID into the new column when its is the source of the emission. This corrects mismatches
    #in the case of more than one base inventory id to FRS_ID
    if base_inventory in inventory_acronyms:
        #The presence of an underscore indicates more than one facilityid was used. If this is the case, get it before the underscore
        inventories['FacilityID_first'] = inventories['FacilityID']
        inventories['FacilityID_first'] = inventories['FacilityID_first'].apply(lambda x: get_id_before_underscore(x))
        inventories.loc[inventories['Source']==base_inventory,colname_base_inventory_id] = inventories['FacilityID_first']
        inventories = inventories.drop(columns='FacilityID_first')
    return(inventories)


def pivotCombinedInventories(combinedinventory_df):
    #Group the results by facility,flow,and compartment
    #Use a pivot table
    combinedinventory_df_pt  = combinedinventory_df.pivot_table(values=['FlowAmount','ReliabilityScore'],index=['FRS_ID','SRS_ID','Compartment'],columns='Source')
    return(combinedinventory_df_pt)
    #len(inventories_withFRSID_SRSID_pt[(inventories_withFRSID_SRSID_pt['FlowAmount']['TRI'] > 0) & (inventories_withFRSID_SRSID_pt['FlowAmount']['NEI'] > 0)])
    #inventories_withFRSID_SRSID_pt.to_csv('inventories_withFRSID_SRSID_pt.csv')

