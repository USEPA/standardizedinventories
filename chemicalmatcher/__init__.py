"""
Functions to allow retrieval of chemical and substance matches from precompiled chemical match lists
or the EPA SRS web service
"""
import pandas as pd

from chemicalmatcher.programsynonymlookupbyCAS import programsynonymlookupbyCAS
from chemicalmatcher.globals import output_dir

def get_matches_for_StEWI():
    """Retrieves all precompiled chemical matches
    :return: dataframe in ChemicalMatches standard output format
    """
    chemicalmatches = pd.read_csv(output_dir+'ChemicalsByInventorywithSRS_IDS_forStEWI.csv',
                                  dtype={"SRS_ID":"str"})
    return chemicalmatches

def get_program_synomyms_for_CAS_list(cas_list, inventories_of_interest):
    """Gets program synonym names for chemicals by CAS using SRS web service
    :param cas_list: a list of CAS numbers as strings, e.g. ['124-38-9', '74-82-8']
    :param inventories_of_interest: inventory acronym, e.g. ['TRI'].
    Valid for 'TRI','NEI', or 'DMR'
    :return: dataframe with columns 'CAS' and inventory acronym with program names
    """
    df_of_synonyms_by_cas = programsynonymlookupbyCAS(cas_list, inventories_of_interest)
    return df_of_synonyms_by_cas
