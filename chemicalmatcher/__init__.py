# __init__.py (chemicalmatcher)
# !/usr/bin/env python3
# coding=utf-8

"""
Public API for chemicalymatcher. Functions to allow retrieval of chemical and
substance matches from precompiled chemical match lists or the EPA
SRS web service
"""

from chemicalmatcher.programsynonymlookupbyCAS import programsynonymlookupbyCAS
from chemicalmatcher.writeStEWIchemicalmatchesbyinventory import\
    writeChemicalMatches
from chemicalmatcher.globals import log, read_cm_file


def get_matches_for_StEWI(inventory_list=None):
    """Retrieve all precompiled chemical matches.

    :param inventory_list: optional list of inventories, if passed will check
        for their presence in the chemical matcher output
    :return: dataframe in ChemicalMatches standard output format
    """
    chemicalmatches = read_cm_file()
    if inventory_list is not None:
        inventories = set(chemicalmatches['Source'].unique())
        if set(inventory_list).issubset(inventories):
            log.debug('all inventories found in chemical matcher')
        else:
            log.info('inventories missing in chemical matcher, regenerating '
                     'chemical matches')
            writeChemicalMatches()
            chemicalmatches = read_cm_file()
    return chemicalmatches


def get_program_synomyms_for_CAS_list(cas_list, inventories_of_interest):
    """Get program synonym names for chemicals by CAS using SRS web service.

    :param cas_list: a list of CAS numbers as strings, e.g. ['124-38-9', '74-82-8']
    :param inventories_of_interest: inventory acronym, e.g. ['TRI'].
        Valid for 'TRI','NEI', or 'DMR'
    :return: dataframe with columns 'CAS' and inventory acronym with program names
    """
    df_of_synonyms_by_cas = programsynonymlookupbyCAS(
        cas_list, inventories_of_interest)
    return df_of_synonyms_by_cas
