import pandas as pd
import os
import logging
from facilitymatcher.globals import filter_by_inventory_list,\
    get_programs_for_inventory_list,invert_inventory_to_FRS,stewi_inventories, filter_by_facility_list

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'facilitymatcher/'

output_dir = modulepath + 'output/'

def get_matches_for_inventories(inventory_list=stewi_inventories):
    facilitymatches = pd.read_csv(output_dir+'FacilityMatchList_forStEWI.csv',dtype={"FRS_ID":"str","FacilityID":"str"})
    facilitymatches = filter_by_inventory_list(facilitymatches,inventory_list)
    return facilitymatches

#Returns the FRS_NAICS csv file filters for IDs of interest. Optionally it will also filter by programs of interest
def get_FRS_NAICSInfo_for_facility_list(frs_id_list,inventories_of_interest_list=None):
    all_NAICS = pd.read_csv(output_dir+'FRS_NAICSforStEWI.csv',header=0,dtype={"FRS_ID":"str","NAICS":"str"})
    NAICS_of_interest = filter_by_facility_list(all_NAICS,frs_id_list)
    if inventories_of_interest_list is not None:
        NAICS_of_interest = filter_by_inventory_list(NAICS_of_interest,inventories_of_interest_list)
    return NAICS_of_interest

# def get_matches_from_inventory_to_inventories_of_interest(from_inventory_acronym,list_of_to_inventory_acronyms):
#     inventory_acronyms = []
#     inventory_acronyms.append(from_inventory_acronym)
#     for l in list_of_to_inventory_acronyms:
#         inventory_acronyms.append(l)
#     facilitymatches = get_matches_for_inventories(inventory_acronyms)
#     from_inventory_acronym_list = []
#     from_inventory_acronym_list.append(from_inventory_acronym)
#     from_program_bridge = filter_bridges_by_inventory_list(facilitymatches,from_inventory_acronym_list)
#     to_program_bridge = filter_bridges_by_inventory_list(facilitymatches,list_of_to_inventory_acronyms)
#     matches = pd.merge(from_program_bridge,to_program_bridge,on='REGISTRY_ID')
#     #Remove cases when 'PGM_SYS_ACRNM_x' and 'PGM_SYS_ACRNM_y' are the same, but IDs are different
#     matches = matches[((matches['PGM_SYS_ACRNM_x']==matches['PGM_SYS_ACRNM_y'])&
#                               (matches['PGM_SYS_ID_x']==matches['PGM_SYS_ID_y']))|
#                               (matches['PGM_SYS_ACRNM_x']!=matches['PGM_SYS_ACRNM_y'])]
#     return matches
#
# #This function is not doing anything unique
# def get_table_of_matches_from_inventory_to_inventories_of_interest(from_inventory_acronym,list_of_to_inventory_acronyms):
#     matches = get_matches_from_inventory_to_inventories_of_interest(from_inventory_acronym, list_of_to_inventory_acronyms)
#     table_of_matches = matches[['REGISTRY_ID','PGM_SYS_ACRNM_x','PGM_SYS_ID_x','PGM_SYS_ACRNM_y','PGM_SYS_ID_y']]
#     return table_of_matches

# #A table of match counts
# def count_matches_from_inventory_to_inventories_of_interest(from_inventory_acronym,list_of_to_inventory_acronyms):
#     matches = get_matches_from_inventory_to_inventories_of_interest(from_inventory_acronym, list_of_to_inventory_acronyms)
#     matches_group = matches.groupby(['PGM_SYS_ACRNM_x','PGM_SYS_ACRNM_y'])['REGISTRY_ID'].count()
#     matches_flat = matches_group.reset_index()
#     matches_flat.columns = ['From_Inventory','To_Inventory','Count_of_Facility_Matches']
#     return matches_flat

