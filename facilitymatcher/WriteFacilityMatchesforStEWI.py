"""
This script gets FRS data in the form of the FRS combined national files
It uses the bridges in the 'NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV'
It writes facility matching file for StEWI
(github.com/usepa/standardizedinventories) inventories
"""

import pandas as pd
import os

import facilitymatcher.globals as glob


def write_facility_matches():
    file = glob.FRS_config['FRS_bridge_file']
    file_path = glob.FRSpath + '/' + file

    # Check to see if file exists
    if not(os.path.exists(file_path)):
        glob.download_extract_FRS_combined_national(file)

    # Import FRS bridge which provides ID matches
    col_dict = {'REGISTRY_ID': "str",
                'PGM_SYS_ACRNM': "str",
                'PGM_SYS_ID': "str"}
    FRS_Bridges = glob.read_FRS_file(file, col_dict)

    # Programs of interest
    stewi_programs = glob.get_programs_for_inventory_list(glob.stewi_inventories)

    # Limit to EPA programs of interest for StEWI
    stewi_bridges = glob.filter_by_program_list(FRS_Bridges, stewi_programs)

    # Separate out eGRID and EIA-860 matches to identify EIA matches to
    # add to eGRID set
    eia_bridges = glob.filter_by_program_list(FRS_Bridges, ['EIA-860'])
    egrid_bridges = glob.filter_by_program_list(FRS_Bridges, ['EGRID'])

    # get a list of all FRS in each
    eia_unique_frs = set(list(pd.unique(eia_bridges['REGISTRY_ID'])))
    egrid_unique_frs = set(list(pd.unique(egrid_bridges['REGISTRY_ID'])))

    eia_not_in_egrid = eia_unique_frs - egrid_unique_frs
    eia_to_add = eia_bridges[eia_bridges['REGISTRY_ID'].isin(
        eia_not_in_egrid)].reset_index(drop=True)

    # Rename to EGRID and add the subset back at the top so that EIA-860 is
    # preferred to eGRID data for improved matching
    eia_to_add['PGM_SYS_ACRNM'] = 'EGRID'
    stewi_bridges = pd.concat([eia_to_add, stewi_bridges], ignore_index=True)
    stewi_bridges = stewi_bridges.drop_duplicates()
    stewi_bridges.reset_index(drop=True)

    # Replace program acronymn with inventory acronymn
    program_to_inventory = glob.invert_inventory_to_FRS()
    stewi_bridges['PGM_SYS_ACRNM'] = stewi_bridges['PGM_SYS_ACRNM'].replace(
        to_replace=program_to_inventory)

    stewi_bridges = stewi_bridges.rename(columns={'REGISTRY_ID': 'FRS_ID',
                                                  'PGM_SYS_ACRNM': 'Source',
                                                  'PGM_SYS_ID': 'FacilityID'})

    # Add in manual matches
    stewi_bridges = glob.add_manual_matches(stewi_bridges)

    # Add in smart matching here

    # Write matches to bridge
    glob.store_fm_file(stewi_bridges, 'FacilityMatchList_forStEWI',
                       sources=[file])


if __name__ == '__main__':
    write_facility_matches()
