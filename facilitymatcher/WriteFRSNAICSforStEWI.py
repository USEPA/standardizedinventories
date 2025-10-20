"""
This script gets FRS data in the form of the FRS combined national files
It uses the bridges in the 'NATIONAL_NAICS_FILE.CSV'
It writes NAICS by facility for StEWI
"""
import facilitymatcher.globals as fmg


def write_NAICS_matches():
    FRS_NAICS = fmg.download_and_read_frs_file('FRS_NAICS_file')

    # Filter this list for stewi
    # Programs of interest
    stewi_programs = fmg.get_programs_for_inventory_list(fmg.stewi_inventories)

    # Limit to EPA programs of interest for StEWI
    stewi_NAICS = fmg.filter_by_program_list(FRS_NAICS, stewi_programs)

    # Drop duplicates
    stewi_NAICS = stewi_NAICS.drop_duplicates()

    # Replace program acronymn with inventory acronymn
    program_to_inventory = fmg.invert_inventory_to_FRS()
    stewi_NAICS['PGM_SYS_ACRNM'] = stewi_NAICS['PGM_SYS_ACRNM'].replace(to_replace=program_to_inventory)

    # Rename columns to be consistent with standards
    stewi_NAICS = stewi_NAICS.rename(columns={'REGISTRY_ID': 'FRS_ID',
                                              'PGM_SYS_ACRNM': 'Source',
                                              'NAICS_CODE': 'NAICS'})
    file = fmg.FRS_config['FRS_NAICS_file']
    fmg.store_fm_file(stewi_NAICS, 'FRS_NAICSforStEWI', sources=[file])


if __name__ == '__main__':
    write_NAICS_matches()
