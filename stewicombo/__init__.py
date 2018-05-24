import pandas as pd
import os
import logging
import stewi
import facilitymatcher
import chemicalmatcher

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'stewicombo/'

inventory_to_FRS_pgm_acrynoym = {"NEI":"EIS","TRI":"TRIS","eGRID":"EGRID"}

compartments = {"NEI":"air"}
columns_to_keep = ['Compartment', 'FacilityID', 'FlowAmount', 'FlowName', 'ReliabilityScore', 'Source']

def combineInventories(inventory_dict):
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
    inventory_to_FRS_keylist = list(inventory_to_FRS_pgm_acrynoym.keys())
    inventory_dict_keylist = list(inventory_dict.keys())
    ununsed_inventory_to_FRS = set(inventory_to_FRS_keylist)-set(inventory_dict_keylist)
    if len(ununsed_inventory_to_FRS) > 0:
        for i in list(ununsed_inventory_to_FRS):
            inventory_to_FRS_pgm_acrynoym.pop(i)

    program_list = []
    for k in inventory_dict.keys():
        pgm_acronym = inventory_to_FRS_pgm_acrynoym[k]
        program_list.append(pgm_acronym)
    facilitymatches = facilitymatcher.get_matches_for_StEWI(program_list)

    #Invert the inventory_to_FRS_pgm_acrynoym dict
    FRS_to_inventory_pgm_acrynoym = {v: k for k, v in inventory_to_FRS_pgm_acrynoym.items()}

    #Substitute in inventory acronym for frs program acrynoym
    facilitymatches['PGM_SYS_ACRNM'].replace(FRS_to_inventory_pgm_acrynoym, inplace=True)

    facilitymatches.rename(columns={"REGISTRY_ID":"FRS_ID"},inplace=True)
    inventories = pd.merge(inventories,facilitymatches,left_on=(['FacilityID','Source']),right_on=(['PGM_SYS_ID','PGM_SYS_ACRNM']),how='left')
    inventories.drop(columns=['PGM_SYS_ID','PGM_SYS_ACRNM'],inplace=True)

    #Bring in chemical matches
    chemicalmatches = chemicalmatcher.get_matches_for_StEWI()
    inventories = pd.merge(inventories,chemicalmatches,on=(['FlowName','Source']),how='left')
    return(inventories)


def pivotCombinedInventories(combinedinventory_df):
    #Group the results by facility,flow,and compartment
    #Use a pivot table
    combinedinventory_df_pt  = combinedinventory_df.pivot_table(values=['FlowAmount','ReliabilityScore'],index=['FRS_ID','SRS_ID','Compartment'],columns='Source')
    return(combinedinventory_df_pt)
    #len(inventories_withFRSID_SRSID_pt[(inventories_withFRSID_SRSID_pt['FlowAmount']['TRI'] > 0) & (inventories_withFRSID_SRSID_pt['FlowAmount']['NEI'] > 0)])
    #inventories_withFRSID_SRSID_pt.to_csv('inventories_withFRSID_SRSID_pt.csv')
