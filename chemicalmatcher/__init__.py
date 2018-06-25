import pandas as pd
import os
import logging

from chemicalmatcher.programsynonymlookupbyCAS import programsynonymlookupbyCAS

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'chemicalmatcher/'

output_dir = modulepath + 'output/'

def get_matches_for_StEWI():
    chemicalmatches = pd.read_csv(output_dir+'ChemicalsByInventorywithSRS_IDS_forStEWI.csv',dtype={"SRS_ID":"str"})
    return chemicalmatches

def get_program_synomyms_for_CAS_list(cas_list,inventories_of_interest):
    df_of_synonyms_by_cas = programsynonymlookupbyCAS(cas_list,inventories_of_interest)
    return df_of_synonyms_by_cas
