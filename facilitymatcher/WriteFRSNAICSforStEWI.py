# link: https://www.epa.gov/frs/epa-state-combined-csv-download-files

import os
from facilitymatcher.globals import stewi_inventories,get_programs_for_inventory_list,\
    filter_by_program_list,invert_inventory_to_FRS,\
    FRSpath, FRS_config, read_FRS_file, download_extract_FRS_combined_national,\
    store_FRS_file

file = FRS_config['FRS_NAICS_file']
file_path = FRSpath + file
FRS_NAICS_file_path = FRSpath + FRS_config['FRS_NAICS_file']

#Check to see if file exists
if not(os.path.exists(file_path)):
    download_extract_FRS_combined_national(file)

col_dict = {'REGISTRY_ID':'str',
            'PGM_SYS_ACRNM':'str',
            'NAICS_CODE':'str',
            'PRIMARY_INDICATOR':'str'}
FRS_NAICS = read_FRS_file(file, col_dict)

#Filter this list for stewi
#Programs of interest
stewi_programs = get_programs_for_inventory_list(stewi_inventories)

#Limit to EPA programs of interest for StEWI
stewi_NAICS = filter_by_program_list(FRS_NAICS,stewi_programs)

#Drop duplicates
stewi_NAICS = stewi_NAICS.drop_duplicates()

#Replace program acronymn with inventory acronymn
program_to_inventory = invert_inventory_to_FRS()
stewi_NAICS['PGM_SYS_ACRNM'] = stewi_NAICS['PGM_SYS_ACRNM'].replace(to_replace=program_to_inventory)

#Rename columns to be consistent with standards
stewi_NAICS = stewi_NAICS.rename(columns={'REGISTRY_ID':'FRS_ID',
                                          'PGM_SYS_ACRNM':'Source',
                                          'NAICS_CODE':'NAICS'})

store_FRS_file(stewi_NAICS,'FRS_NAICSforStEWI', sources=[file])