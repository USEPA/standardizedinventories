"""
This script gets FRS data in the form of the FRS combined national files
It uses the bridges in the 'NATIONAL_NAICS_FILE.CSV'
It writes NAICS by facility for StEWI
"""
import os

import facilitymatcher.globals as glob


def write_NAICS_matches():
    file = glob.FRS_config['FRS_NAICS_file']
    file_path = glob.FRSpath + '/' + file

    # Check to see if file exists
    if not(os.path.exists(file_path)):
        glob.download_extract_FRS_combined_national(file)

    col_dict = {'REGISTRY_ID': 'str',
                'PGM_SYS_ACRNM': 'str',
                'NAICS_CODE': 'str',
                'PRIMARY_INDICATOR': 'str'}
    FRS_NAICS = glob.read_FRS_file(file, col_dict)

    # Filter this list for stewi
    # Programs of interest
    stewi_programs = glob.get_programs_for_inventory_list(glob.stewi_inventories)

    # Limit to EPA programs of interest for StEWI
    stewi_NAICS = glob.filter_by_program_list(FRS_NAICS, stewi_programs)

    # Drop duplicates
    stewi_NAICS = stewi_NAICS.drop_duplicates()

    # Replace program acronymn with inventory acronymn
    program_to_inventory = glob.invert_inventory_to_FRS()
    stewi_NAICS['PGM_SYS_ACRNM'] = stewi_NAICS['PGM_SYS_ACRNM'].replace(to_replace=program_to_inventory)

    # Rename columns to be consistent with standards
    stewi_NAICS = stewi_NAICS.rename(columns={'REGISTRY_ID': 'FRS_ID',
                                              'PGM_SYS_ACRNM': 'Source',
                                              'NAICS_CODE': 'NAICS'})

    glob.store_fm_file(stewi_NAICS, 'FRS_NAICSforStEWI', sources=[file])


if __name__ == '__main__':
    write_NAICS_matches()
