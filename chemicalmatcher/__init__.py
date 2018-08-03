import pandas as pd

from chemicalmatcher.programsynonymlookupbyCAS import programsynonymlookupbyCAS
from chemicalmatcher.globals import output_dir

def get_matches_for_StEWI():
    chemicalmatches = pd.read_csv(output_dir+'ChemicalsByInventorywithSRS_IDS_forStEWI.csv',dtype={"SRS_ID":"str"})
    return chemicalmatches

def get_program_synomyms_for_CAS_list(cas_list,inventories_of_interest):
    df_of_synonyms_by_cas = programsynonymlookupbyCAS(cas_list,inventories_of_interest)
    return df_of_synonyms_by_cas
