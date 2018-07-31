import pandas as pd

FRSpath = '../FRS/'
FRS_NAICS_file = 'NATIONAL_NAICS_FILE.CSV'
FRS_NAICS_file_path = FRSpath + FRS_NAICS_file

FRS_NAICS = pd.read_csv(FRS_NAICS_file_path, header=0, nrows=100)
columns_to_keep = ['REGISTRY_ID', 'PGM_SYS_ACRNM', 'NAICS_CODE', 'PRIMARY_INDICATOR']
dtype_dict = {'REGISTRY_ID':'str','NAICS_CODE':'str'}
FRS_NAICS = pd.read_csv(FRS_NAICS_file_path, header=0, usecols=columns_to_keep, dtype=dtype_dict)

FRS_NAICS.to_csv('facilitymatcher/output/FRS_NAICS.csv',index=False)

