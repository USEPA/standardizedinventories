# Retrieves all unique flow names from the StEWI flow list, uses SRS web
# service to find their SRSname and CAS
import pandas as pd

import stewi
from stewi.globals import log
from chemicalmatcher.globals import OUTPUT_PATH, get_SRSInfo_for_substance_name,\
    get_SRSInfo_for_program_list, add_manual_matches

flowlist_cols = {"RCRAInfo": ['FlowName', 'FlowID'],
                 "eGRID": ['FlowName'],
                 "TRI": ['FlowName', 'FlowID'],
                 "NEI": ['FlowName', 'FlowID'],
                 "DMR": ['FlowName', 'FlowID'],
                 "GHGRP": ['FlowName', 'FlowID']}


def writeChemicalMatches():
    all_list_names = extract_flows_for_chemical_matcher()
    if len(all_list_names) == 0:
        log.error('no local flows found, chemical matches can not be assessed, '
                  'generate local inventories before continuing.')
        return

    # Determine whether to use the id or name to query SRS
    inventory_query_type = {"RCRAInfo": "list",
                            "TRI": "list",
                            "NEI": "list",
                            "eGRID": "name",
                            "DMR": "list",
                            "GHGRP": "name"}

    # Create a df to store the results
    all_lists_srs_info = pd.DataFrame(columns=["FlowName", "SRS_ID",
                                               "SRS_CAS", "Source"])
    errors_srs = pd.DataFrame(columns=["FlowName", "Source", "ErrorType"])

    # Loop through sources, querying SRS by the query type defined for the
    # source, merge the results with the flows for that inventory.
    # Store errors in a separate dataframe
    sources = list(pd.unique(all_list_names['Source']))
    for source in sources:
        log.info('accessing SRS for ' + source)
        # Get df with inventory flows
        inventory_flows = (all_list_names
                           .query('Source == @source')
                           .reset_index(drop=True))

        if inventory_query_type[source] == 'list':
            # make sure flowid is a string
            inventory_flows['FlowID'] = inventory_flows['FlowID'].map(str)
            # query SRS to get entire list and then merge with it
            list_srs_info = get_SRSInfo_for_program_list(source)
            # merge this with the original list using FlowID
            list_srs_info = pd.merge(inventory_flows, list_srs_info,
                                     left_on='FlowID', right_on='PGM_ID',
                                     how='left')
        elif inventory_query_type[source] == 'name':
            # For names, query SRS one by one to get results
            list_srs_info = pd.DataFrame(columns=["FlowName", "SRS_ID",
                                                  "SRS_CAS", "Source"])
            errors_srs = pd.DataFrame(columns=["FlowName", "Source", "ErrorType"])
            # Cycle through names one by one
            for index, row in inventory_flows.iterrows():
                chemical_srs_info = pd.DataFrame(columns=["FlowName", "SRS_ID",
                                                          "SRS_CAS", "Source"])
                error_srs = pd.DataFrame(columns=["FlowName", "Source",
                                                  "ErrorDescription"])
                name = row["FlowName"]
                result = get_SRSInfo_for_substance_name(name)
                if isinstance(result, str):
                    # This is an error
                    error_srs.loc[0, 'FlowName'] = name
                    #error_srs.loc[0, 'FlowID'] = id
                    error_srs.loc[0, 'Source'] = source
                    error_srs.loc[0, 'ErrorDescription'] = result
                else:
                    chemical_srs_info = result
                    chemical_srs_info.loc[0, "FlowName"] = name
                    #chemical_srs_info.loc[0, "FlowID"] = name
                    chemical_srs_info.loc[0, "Source"] = source

                errors_srs = pd.concat([errors_srs, error_srs], sort=False)
                list_srs_info = pd.concat([list_srs_info, chemical_srs_info],
                                          sort=False)

        all_lists_srs_info = pd.concat([all_lists_srs_info, list_srs_info],
                                       sort=False)

    # Remove waste code and PGM_ID
    all_lists_srs_info = all_lists_srs_info.drop(columns=['PGM_ID'])

    # Add in manually found matches
    all_lists_srs_info = add_manual_matches(all_lists_srs_info)

    # Write to csv
    filepath = OUTPUT_PATH.joinpath('ChemicalsByInventorywithSRS_IDS_forStEWI.csv')
    flows_list = pd.read_csv(filepath, dtype={'SRS_ID': str})
    flows_list = pd.concat([flows_list,
                            all_lists_srs_info[['FlowID', 'FlowName', 'SRS_CAS',
                                                'SRS_ID', 'Source']]
                            ], ignore_index=True)
    flows_list = flows_list.drop_duplicates(['FlowID', 'FlowName', 'Source'])
    flows_list = flows_list.sort_values(['Source', 'FlowName',
                                         'SRS_ID', 'FlowID'])

    flows_list.to_csv(filepath, index=False)
    #errors_srs.to_csv('work/ErrorsSRS.csv',index=False)

    # Write flows missing srs_ids to file for more inspection
    filepath = OUTPUT_PATH.joinpath('flows_missing_SRS_ID.csv')
    flows_missing_SRS_ID = flows_list.query('SRS_ID.isnull()')
    missing_list = (pd.read_csv(filepath)
                    .query('FlowID not in list(@flows_list.query( \
                           "SRS_ID.notnull()").FlowID)')
                    )
    missing_list = (pd.concat([missing_list, flows_missing_SRS_ID],
                             ignore_index=True)
                    .drop_duplicates(['FlowID', 'FlowName', 'Source'])
                    .sort_values(['Source', 'FlowName',
                                  'SRS_ID', 'FlowID'])
                    )
    missing_list.to_csv(filepath, index=False)


def extract_flows_for_chemical_matcher():
    log.info('generating chemical matches from local flow lists')
    # First loop through flows lists to create a list of all unique flows
    source_dict = stewi.getAvailableInventoriesandYears(stewiformat='flow')
    all_list_names = pd.DataFrame(columns=["FlowName", "FlowID"])
    for source in source_dict.keys():
        list_names_years = pd.DataFrame()
        for year in source_dict[source]:
            list_names = pd.DataFrame()
            list_names = stewi.getInventoryFlows(source, year)
            list_names = list_names[flowlist_cols[source]]
            list_names = list_names.drop_duplicates()
            list_names_years = pd.concat([list_names_years, list_names],
                                         sort=False)
        if source == 'TRI':
            list_names_years['FlowID'] = (
                list_names_years['FlowID'].apply(
                    lambda x: x.lstrip('0').replace('-', '')))
        list_names_years = list_names_years.drop_duplicates()
        list_names_years['Source'] = source
        all_list_names = pd.concat([all_list_names, list_names_years],
                                   sort=False)

    # Drop duplicates from lists with same names
    all_list_names = (all_list_names
                      .drop_duplicates()
                      .reset_index(drop=True))
    return all_list_names


if __name__ == '__main__':
    writeChemicalMatches()
