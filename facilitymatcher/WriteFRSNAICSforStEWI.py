# link: https://www.epa.gov/frs/epa-state-combined-csv-download-files

import pandas as pd
from facilitymatcher.globals import stewi_inventories,get_programs_for_inventory_list,\
    filter_by_program_list,invert_inventory_to_FRS, output_dir

FRSpath = '../FRS/'
FRS_NAICS_file = 'NATIONAL_NAICS_FILE.CSV'
FRS_NAICS_file_path = FRSpath + FRS_NAICS_file

FRS_NAICS = pd.read_csv(FRS_NAICS_file_path, header=0, nrows=100)
columns_to_keep = ['REGISTRY_ID', 'PGM_SYS_ACRNM', 'NAICS_CODE', 'PRIMARY_INDICATOR']
dtype_dict = {'REGISTRY_ID':'str','NAICS_CODE':'str'}
FRS_NAICS = pd.read_csv(FRS_NAICS_file_path, header=0, usecols=columns_to_keep, dtype=dtype_dict)

#Load from pickle
#FRS_NAICS = pd.read_pickle('work/FRS_NAICS.pk')

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
stewi_NAICS = stewi_NAICS.rename(columns={'REGISTRY_ID':'FRS_ID','PGM_SYS_ACRNM':'Source','NAICS_CODE':'NAICS'})

stewi_NAICS.to_csv(output_dir + 'FRS_NAICSforStEWI.csv',index=False)
