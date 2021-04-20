#This script gets FRS data in the form of the FRS combined national files
#It uses the bridges in the 'NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV'
#It writes facility matching file for StEWI (github.com/usepa/standardizedinventories) inventories

import pandas as pd
import os

from facilitymatcher.globals import stewi_inventories,get_programs_for_inventory_list,\
    filter_by_program_list,download_extract_FRS_combined_national,\
    invert_inventory_to_FRS,add_manual_matches,\
    FRSpath, FRS_config, read_FRS_file, store_FRS_file

file = FRS_config['FRS_bridge_file']
file_path = FRSpath + file

#Check to see if file exists
if not(os.path.exists(file_path)):
    download_extract_FRS_combined_national(file)

#Import FRS bridge which provides ID matches
col_dict = {'REGISTRY_ID':"str",
            'PGM_SYS_ACRNM':"str",
            'PGM_SYS_ID':"str"}
FRS_Bridges = read_FRS_file(file, col_dict)

#Programs of interest
stewi_programs = get_programs_for_inventory_list(stewi_inventories)

#Limit to EPA programs of interest for StEWI
stewi_bridges = filter_by_program_list(FRS_Bridges,stewi_programs)

#Separate out eGRID and EIA-860 matches to identify EIA matches to add to eGRID set
eia_bridges = filter_by_program_list(FRS_Bridges,['EIA-860'])
egrid_bridges = filter_by_program_list(FRS_Bridges,['EGRID'])

#get a list of all FRS in each
eia_unique_frs = set(list(pd.unique(eia_bridges['REGISTRY_ID'])))
egrid_unique_frs = set(list(pd.unique(egrid_bridges['REGISTRY_ID'])))

eia_not_in_egrid = eia_unique_frs - egrid_unique_frs
eia_to_add = eia_bridges[eia_bridges['REGISTRY_ID'].isin(eia_not_in_egrid)]
len(eia_to_add)
#1781

#Rename to EGRID
eia_to_add['PGM_SYS_ACRNM'] = 'EGRID'
#Now add this subset back
stewi_bridges = pd.concat([stewi_bridges,eia_to_add])
len(stewi_bridges[stewi_bridges['PGM_SYS_ACRNM'] == 'EGRID'])

#Drop duplicates
stewi_bridges = stewi_bridges.drop_duplicates()

#Replace program acronymn with inventory acronymn
program_to_inventory = invert_inventory_to_FRS()
stewi_bridges['PGM_SYS_ACRNM'] = stewi_bridges['PGM_SYS_ACRNM'].replace(to_replace=program_to_inventory)

#Rename fields
stewi_bridges = stewi_bridges.rename(columns={'REGISTRY_ID':'FRS_ID',
                                              'PGM_SYS_ACRNM':'Source',
                                              'PGM_SYS_ID':'FacilityID'})

#Add in manual matches
stewi_bridges = add_manual_matches(stewi_bridges)

#Add in smart matching here

#Write matches to bridge
store_FRS_file(stewi_bridges,'FacilityMatchList_forStEWI', sources=[file])
