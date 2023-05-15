# DMR.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Queries DMR data by state, temporarily saves them,
Web service documentation found at
https://echo.epa.gov/system/files/ECHO%20All%20Data%20Search%20Services_v3.pdf

This file requires paramaters be passed like:
    Option -Y Year
    A -Y 2016
Option:
A - for downloading DMR data by state
B - for generating StEWI output files and validation from downloaded data
C - for downloading and generating state totals file

Year:
    2014-2021
"""

import requests
import pandas as pd
import argparse
import urllib
import time
from pathlib import Path

from esupy.processed_data_mgmt import read_source_metadata
from stewi.globals import unit_convert,\
    DATA_PATH, lb_kg, write_metadata, get_reliability_table_for_source,\
    log, compile_source_metadata, config, store_inventory, set_stewi_meta,\
    paths, aggregate
from stewi.validate import update_validationsets_sources, validate_inventory,\
    write_validation_result
from stewi.filter import filter_states, filter_config
import stewi.exceptions


_config = config()['databases']['DMR']
DMR_DATA_PATH = DATA_PATH / 'DMR'
EXT_DIR = 'DMR Data Files'
OUTPUT_PATH = Path(paths.local_path).joinpath(EXT_DIR)

states_df = pd.read_csv(DATA_PATH.joinpath('state_codes.csv'))
STATES = list(states_df['states']) + list(states_df['dc']) +\
    list(states_df['territories'])
STATES = tuple(x for x in STATES if str(x) != 'nan')

# Values used for StEWI query
PARAM_GROUP = True
DETECTION = 'HALF'
ESTIMATION = True


def generate_url(url_params):
    """Generate the url for DMR query.

    :param kwargs: potential arguments include:
        p_year: 4 digit string of year
        p_sic2: sic region 2 digit code
        p_st: state 2 letter abbreviation
        p_poll_cat: N or P
        p_nutrient_agg: Y or N
        responseset: int
        pageno: int

    See web service documentation for details
    https://echo.epa.gov/tools/web-services/loading-tool#/Custom%20Search/get_dmr_rest_services_get_custom_data_annual
    """
    params = {k: v for k, v in url_params.items() if v}

    params['p_nd'] = DETECTION
    params['output'] = 'JSON'
    if 'p_poll_cat' in params:
        params['p_poll_cat'] = 'Nut' + params['p_poll_cat']
    if PARAM_GROUP:
        params['p_param_group'] = 'Y'  # default is N
    if not ESTIMATION:
        params['p_est'] = 'N'  # default is Y
    if 'responseset' not in params:
        params['responseset'] = '20000'

    url = _config['base_url'] + urllib.parse.urlencode(params)

    return url


def query_dmr(year, sic_list=None, state_list=STATES, nutrient=''):
    """Loop through a set of states and sics to download and pickle DMR data.

    :param year: str, year of data
    :param sic_list: Option to break up queries further by list of 2-digit
        SIC codes
    :param state_list: List of states to include in query
    :param nutrient: Option to query by nutrient category with aggregation.
        Input 'N' or 'P'
    :return: results dictionary
    """
    path = OUTPUT_PATH.joinpath(year)
    path.mkdir(parents=True, exist_ok=True)
    results = {}
    filestub = ''
    url_params = {'p_year': year,
                  'p_st': '',
                  'p_poll_cat': nutrient,
                  'p_nutrient_agg': 'N',
                  'responseset': '9000',
                  'pageno': '1',
                  }
    if nutrient:
        filestub = nutrient + "_"
        url_params['p_nutrient_agg'] = 'Y'
    for state in state_list:
        filename = f"{filestub}state_{state}.pickle"
        filepath = path.joinpath(filename)
        if check_for_file(filepath, state):
            results[state] = 'success'
        else:
            url_params['p_st'] = state
            results[state] = download_data(url_params, filepath, sic_list)
    return results


def check_for_file(filepath: Path, state) -> bool:
    if filepath.is_file():
        log.debug(f'file already exists for {state}, skipping')
        return True
    else:
        log.info(f'executing query for {state}')
        return False


def download_data(url_params, filepath: Path, sic_list) -> str:
    df = pd.DataFrame()
    if sic_list:
        skip_errors = True
    else:
        skip_errors = False
        sic_list = ['']
    for sic in sic_list:
        url_params['p_sic2'] = sic
        counter = 1
        pages = 1
        while counter <= pages:
            url_params['pageno'] = counter
            url = generate_url(url_params)
            log.debug(url)
            for attempt in range(3):
                try:
                    r = requests.get(url)
                    r.raise_for_status()
                    result = pd.DataFrame(r.json())
                    break
                except requests.exceptions.HTTPError as err:
                    log.info(err)
                    time.sleep(20)
                    pass
            else:
                log.warning("exceeded max attempts")
                return 'other_error'
            if 'Error' in result.index:
                if skip_errors:
                    log.debug(f"error in sic_{sic}")
                    break
                elif result['Results'].astype(str).str.contains('Maximum').any():
                    return 'max_error'
                else:
                    return 'other_error'
            elif 'NoDataMsg' in result.index:
                if skip_errors:
                    log.debug(f"no data in sic_{sic}")
                    break
                else:
                    return 'no_data'
            else:
                df = pd.concat([df, pd.DataFrame(result['Results']['Results'])],
                               ignore_index=True)
                # set page count
                pages = int(result['Results']['PageCount'])
                counter += 1
    log.debug(f"saving to {filepath}")
    pd.to_pickle(df, filepath)
    return 'success'


def standardize_df(input_df):
    """Modify DMR data to meet StEWI specifications."""
    dmr_required_fields = pd.read_csv(DMR_DATA_PATH
                                      .joinpath('DMR_required_fields.txt'),
                                      header=None)[0]
    output_df = input_df[dmr_required_fields].copy()
    dmr_reliability_table = get_reliability_table_for_source('DMR')
    dmr_reliability_table.drop(columns=['Code'], inplace=True)
    output_df['DataReliability'] = dmr_reliability_table[
        'DQI Reliability Score'].values[0]

    # Rename with standard column names
    field_dictionary = {'ExternalPermitNmbr': 'FacilityID',
                              'Siccode': 'SIC',
                              'NaicsCode': 'NAICS',
                              'StateCode': 'State',
                              'PollutantLoad': 'FlowAmount',
                              'CountyName': 'County',
                              'GeocodeLatitude': 'Latitude',
                              'GeocodeLongitude': 'Longitude'}
    if PARAM_GROUP:
        field_dictionary['PollutantDesc'] = 'FlowName'
        field_dictionary['PollutantCode'] = 'FlowID'
    else:
        field_dictionary['ParameterDesc'] = 'FlowName'
        field_dictionary['ParameterCode'] = 'FlowID'
    output_df.rename(columns=field_dictionary, inplace=True)
    # Drop flow amount of '--'
    output_df = output_df[output_df['FlowAmount'] != '--']
    # Already in kg/yr, so no conversion necessary

    # FlowAmount is not a number, remove commas and convert to numeric
    output_df['FlowAmount'] = output_df['FlowAmount'].replace({',': ''},
                                                              regex=True)
    output_df['FlowAmount'] = pd.to_numeric(output_df['FlowAmount'],
                                            errors='coerce')

    if PARAM_GROUP:
        flows = read_pollutant_parameter_list()
        dmr_flows = flows[['FlowName', 'FlowID']
                          ].drop_duplicates(subset=['FlowName'])
        output_df = output_df.merge(dmr_flows, on='FlowName', how='left')
        output_df.loc[output_df.FlowID_x.isin(
            flows.PARAMETER_CODE), ['FlowID']] = output_df['FlowID_x']
        output_df.loc[~output_df.FlowID_x.isin(
            flows.PARAMETER_CODE), ['FlowID']] = output_df['FlowID_y']
        output_df.drop(columns=['FlowID_x', 'FlowID_y'], inplace=True)

    return output_df


def combine_DMR_inventory(year, nutrient=''):
    """Loop through pickled data and combines into a dataframe."""
    path = OUTPUT_PATH.joinpath(year)
    if not path.is_dir():
        raise stewi.exceptions.DataNotFoundError
    output_df = pd.DataFrame()
    filestub = ''
    if nutrient:
        filestub = nutrient + '_'
        log.info(f'reading stored DMR queries by state for {nutrient}...')
    else:
        log.info('reading stored DMR queries by state...')
    for state in STATES:
        log.debug(f'accessing data for {state}')
        filepath = path.joinpath(f'{filestub}state_{state}.pickle')
        result = unpickle(filepath)
        if result is None:
            log.warning(f'No data found for {state}. Retrying query...')
            if (query_dmr(year=year, sic_list=None,
                         state_list=[state],
                         nutrient=nutrient).get(state) == 'success'):
                result = unpickle(filepath)
        if result is not None:
            output_df = pd.concat([output_df, result], ignore_index=True)
    return output_df


def unpickle(filepath):
    try:
        return pd.read_pickle(filepath)
    except FileNotFoundError:
        log.exception(f'error reading {filepath}')
        return None


def download_state_totals_validation(year):
    """Generate file of state totals downloaded from echo as csv for validation.

    Annual totals are stored in the repository.
    """
    log.info('generating state totals')
    # https://echo.epa.gov/trends/loading-tool/get-data/state-statistics
    url = _config['state_url'].replace("__year__", year)
    state_csv = pd.read_csv(url, header=2)
    state_totals = pd.DataFrame()
    state_totals['state_name'] = state_csv['State']
    state_totals['FlowName'] = 'All'
    state_totals['Compartment'] = 'water'
    state_totals['Amount'] = state_csv['Total Pollutant Pounds (lb/yr) for Majors'] +\
        state_csv['Total Pollutant Pounds (lb/yr) for Non-Majors']
    state_totals['Unit'] = 'lb'
    state_names = states_df[['states', 'state_name']]
    state_totals = state_totals.merge(state_names, how='left',
                                      on='state_name')
    state_totals.drop(columns=['state_name'], inplace=True)
    state_totals.dropna(subset=['states'], inplace=True)
    state_totals.rename(columns={'states': 'State'}, inplace=True)
    log.info(f'saving DMR_{year}_StateTotals.csv to {DATA_PATH}')
    state_totals.to_csv(DATA_PATH.joinpath(f"DMR_{year}_StateTotals.csv"),
                        index=False)

    # Update validationSets_Sources.csv
    validation_dict = {'Inventory': 'DMR',
                       #'Version': '',
                       'Year': year,
                       'Name': 'State statistics',
                       'URL': 'https://echo.epa.gov/trends/loading-tool/'
                       'get-data/state-statistics',
                       'Criteria': 'Check totals by state',
                       }
    update_validationsets_sources(validation_dict)


def validate_state_totals(df, year):
    """Generate validation by state, sums across species.

    Details on results by state can be found in the search results help website
    https://echo.epa.gov/help/loading-tool/water-pollution-search/search-results-help-dmr
    """
    filepath = DATA_PATH.joinpath(f"DMR_{year}_StateTotals.csv")
    if not filepath.is_file():
        download_state_totals_validation(year)
    log.info('validating against state totals')
    reference_df = pd.read_csv(filepath)
    reference_df['FlowAmount'] = 0.0
    reference_df = unit_convert(reference_df, 'FlowAmount',
                                'Unit', 'lb', lb_kg, 'Amount')
    reference_df = reference_df[['FlowName', 'State', 'FlowAmount']]

    # to match the state totals, only compare NPD facilities, and remove some flows
    flow_exclude = pd.read_csv(DMR_DATA_PATH.joinpath('DMR_state_filter_list.csv'))
    state_flow_exclude_list = flow_exclude['POLLUTANT_DESC'].to_list()

    dmr_by_state = df[~df['FlowName'].isin(state_flow_exclude_list)]
    dmr_by_state = dmr_by_state[dmr_by_state['PermitTypeCode'] == 'NPD']

    dmr_by_state = dmr_by_state[['State', 'FlowAmount']]
    dmr_by_state = dmr_by_state[['State', 'FlowAmount']
                                ].groupby('State').sum().reset_index()
    dmr_by_state['FlowName'] = 'All'
    validation_df = validate_inventory(dmr_by_state, reference_df,
                                       group_by=["State"])
    write_validation_result('DMR', year, validation_df)


def generate_metadata(year, datatype='inventory'):
    """Generate metadata and write to json for datatypes 'inventory' or 'source'."""
    if datatype == 'source':
        source_path = str(OUTPUT_PATH.joinpath(year))
        source_meta = compile_source_metadata(source_path, _config, year)
        source_meta['SourceType'] = 'Web Service'
        write_metadata(f"DMR_{year}", source_meta, category=EXT_DIR,
                       datatype='source')
    else:
        source_meta = read_source_metadata(paths, set_stewi_meta(f"DMR_{year}",
                                           EXT_DIR),
                                           force_JSON=True)['tool_meta']
        write_metadata(f"DMR_{year}", source_meta, datatype=datatype)


def read_pollutant_parameter_list(parameter_grouping=PARAM_GROUP):
    """Read and parse the DMR pollutant parameter list."""
    url = _config['pollutant_list_url']
    flows = pd.read_csv(url, header=1, usecols=['POLLUTANT_CODE',
                                                'POLLUTANT_DESC',
                                                'PARAMETER_CODE',
                                                'PARAMETER_DESC',
                                                'SRS_ID', 'NITROGEN',
                                                'PHOSPHORUS',
                                                'ORGANIC_ENRICHMENT'],
                        dtype=str)
    if parameter_grouping:
        flows.rename(columns={'POLLUTANT_DESC': 'FlowName',
                              'POLLUTANT_CODE': 'FlowID'}, inplace=True)
    else:
        flows.rename(columns={'PARAMETER_DESC': 'FlowName',
                              'PARAMETER_CODE': 'FlowID'}, inplace=True)
    return flows


def consolidate_nutrients(df, drop_list, nutrient):
    """Rename flows following nutrient aggregation to better handle flow overlaps."""
    if nutrient == 'P':
        flow = ['Phosphorus', 'PHOSP']
    elif nutrient == 'N':
        flow = ['Nitrogen', 'N']
    df.loc[(df['PollutantDesc'].isin(drop_list)), ['PollutantDesc',
                                                   'PollutantCode']] = flow

    return df


def remove_duplicate_organic_enrichment(df):
    """Remove duplicate organic enrichment parameters.

    Facilities can report multiple forms of organic enrichment, BOD and COD,
    which represent duplicate accounting of oxygen depletion. See Meyer et al.
    2020
    """
    flow_preference = filter_config[
        'remove_duplicate_organic_enrichment']['parameters']['flow_preference']

    org_flow_list = read_pollutant_parameter_list()
    org_flow_list = org_flow_list[org_flow_list['ORGANIC_ENRICHMENT'] == 'Y']
    org_flow_list = org_flow_list[['FlowName']].drop_duplicates()
    org_flow_list = org_flow_list['FlowName'].to_list()

    cod_list = [flow for flow in org_flow_list if 'COD' in flow]
    bod_list = [flow for flow in org_flow_list if 'BOD' in flow]
    if flow_preference == 'COD':
        keep_list = cod_list
    else:
        keep_list = bod_list

    df_org = df.loc[df['FlowName'].isin(org_flow_list)]
    df_duplicates = df_org[df_org.duplicated(subset='FacilityID', keep=False)]
    if len(df_duplicates) == 0:
        return df

    df = df.loc[~df['FlowName'].isin(org_flow_list)]
    df_org = df_org[~df_org.duplicated(subset='FacilityID', keep=False)]
    to_be_concat = []
    to_be_concat.append(df)
    to_be_concat.append(df_org)

    df_duplicates['PrefList'] = df_duplicates[
        'FlowName'].apply(lambda x: x in keep_list)
    df_duplicates['NonPrefList'] = df_duplicates[
        'FlowName'].apply(lambda x: x not in keep_list)
    grouped = df_duplicates.groupby(['FacilityID'])
    for name, frame in grouped:
        if not frame['NonPrefList'].all():
            frame = frame[frame['PrefList']]
        to_be_concat.append(frame)
    df = pd.concat(to_be_concat)
    df.sort_index(inplace=True)
    df.drop(columns=['PrefList', 'NonPrefList'], inplace=True)
    df.reset_index(inplace=True)
    return df


def remove_nutrient_overlap_TRI(df, preference):
    """Consolidate overlap of nutrient flows in a df containing both TRI and DMR.

    :param df: dataframe in flowbyfacility combined format
    :param preference: str 'DMR' or 'TRI'
    :returns: dataframe with nonpreferred flows removed
    """
    tri_list = ['Ammonia',
                'Nitrate Compounds',
                'Nitrate compounds (water dissociable; reportable only when in aqueous solution)']
    dmr_list = ['Nitrogen']
    combined_list = tri_list + dmr_list

    # for facilities where the FRS and compartment match
    if preference == 'DMR':
        keep_list = dmr_list
    else:
        keep_list = tri_list

    df_nutrients = df.loc[((df['FlowName'].isin(combined_list)) &
                           (df['Compartment'] == 'water'))]
    df_duplicates = df_nutrients[df_nutrients.duplicated(subset='FRS_ID',
                                                         keep=False)]
    if len(df_duplicates) == 0:
        return df

    df = df.loc[~((df['FlowName'].isin(combined_list)) &
                  (df['Compartment'] == 'water'))]
    df_nutrients = df_nutrients[~df_nutrients.duplicated(subset='FRS_ID',
                                                         keep=False)]
    to_be_concat = []
    to_be_concat.append(df)
    to_be_concat.append(df_nutrients)

    df_duplicates['PrefList'] = df_duplicates[
        'FlowName'].apply(lambda x: x in keep_list)
    df_duplicates['NonPrefList'] = df_duplicates[
        'FlowName'].apply(lambda x: x not in keep_list)
    grouped = df_duplicates.groupby(['FRS_ID'])
    for name, frame in grouped:
        if not frame['NonPrefList'].all():
            frame = frame[frame['PrefList']]
        to_be_concat.append(frame)
    df = pd.concat(to_be_concat)
    df.sort_index(inplace=True)
    df.drop(columns=['PrefList', 'NonPrefList'], inplace=True)
    df.reset_index(inplace=True, drop=True)

    return df


def main(**kwargs):
    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Download DMR files from web\
                        [B] Generate StEWI inventory outputs and\
                            validate to state totals\
                        [C] Download state totals',
                        type = str)

    parser.add_argument('-Y', '--Year', nargs = '+',
                        help = 'What DMR year(s) you want to retrieve',
                        type = str)

    if len(kwargs) == 0:
        kwargs = vars(parser.parse_args())

    for year in kwargs['Year']:
        year = str(year)
        if kwargs['Option'] == 'A':
            log.info(f"Querying for {year}")

            # two digit SIC codes from advanced search drop down stripped and formatted as a list
            sic2 = list(pd.read_csv(DMR_DATA_PATH.joinpath('2_digit_SIC.csv'),
                        dtype={'SIC2': str})['SIC2'])
            # Query by state, then by SIC-state where necessary
            result_dict = query_dmr(year=year)
            log.debug('possible errors: ' + ', '.join(
                [s for s in result_dict.keys()
                 if result_dict[s] != 'success']))
            state_max_error_list = [s for s in result_dict.keys()
                                    if result_dict[s] == 'max_error']
            state_no_data_list = [s for s in result_dict.keys()
                                  if result_dict[s] == 'no_data']
            if (len(state_max_error_list) == 0) and (len(state_no_data_list) == 0):
                log.info('all states succesfully downloaded')
            else:
                if (len(state_max_error_list) > 0):
                    log.error(f"Max error: {' '.join(state_max_error_list)}")
                if (len(state_no_data_list) > 0):
                    log.error(f"No data error: {' '.join(state_no_data_list)}")
                log.info('Breaking up queries further by SIC')
                result_dict = query_dmr(year=year, sic_list=sic2,
                                        state_list=state_max_error_list)
                sic_state_max_error_list = [s for s in result_dict.keys()
                                            if result_dict[s] == 'max_error']
                if len(sic_state_max_error_list) > 0:
                    log.error(f"Max error: {' '.join(sic_state_max_error_list)}")

            log.info(f"Querying nutrients for {year}")
            # Query aggregated nutrients data
            for nutrient in ['N', 'P']:
                result_dict = query_dmr(year=year, nutrient=nutrient)
                log.debug('possible errors: ' + ', '.join(
                    [s for s in result_dict.keys()
                     if result_dict[s] != 'success']))
                state_max_error_list = [s for s in result_dict.keys()
                                        if result_dict[s] == 'max_error']
                state_no_data_list = [s for s in result_dict.keys()
                                      if result_dict[s] == 'no_data']
                if (len(state_max_error_list) == 0) and (len(state_no_data_list) == 0):
                    log.info(f'all states succesfully downloaded for {nutrient}')
                else:
                    result_dict = query_dmr(year=year, sic_list=sic2,
                                            state_list=state_max_error_list,
                                            nutrient=nutrient)
            # write metadata
            generate_metadata(year, datatype='source')

        if kwargs['Option'] == 'B':
            log.info(f'generating inventories for DMR {year}')
            state_df = combine_DMR_inventory(year)
            state_df = filter_states(standardize_df(state_df))

            # Validation against state totals is done prior to combining
            # with aggregated nutrients
            validate_state_totals(state_df, year)

            P_df = combine_DMR_inventory(year, nutrient='P')
            N_df = combine_DMR_inventory(year, nutrient='N')

            nut_drop_list = read_pollutant_parameter_list()
            nut_drop_list = nut_drop_list[(nut_drop_list['NITROGEN'] == 'Y') |
                                          (nut_drop_list['PHOSPHORUS'] == 'Y')]
            nut_drop_list = list(set(nut_drop_list['FlowName']))

            # Consolidate N and P based flows to reflect nutrient aggregation
            P_df = consolidate_nutrients(P_df, nut_drop_list, 'P')
            N_df = consolidate_nutrients(N_df, nut_drop_list, 'N')

            nutrient_agg_df = pd.concat([P_df, N_df])
            nutrient_agg_df = filter_states(standardize_df(nutrient_agg_df))

            # Filter out nitrogen and phosphorus flows before combining
            # with aggregated nutrients
            dmr_nut_filtered = state_df[~state_df['FlowName'].isin(nut_drop_list)]
            dmr_df = pd.concat([dmr_nut_filtered,
                                nutrient_agg_df]).reset_index(drop=True)

            # PermitTypeCode needed for state validation but not maintained
            dmr_df = dmr_df.drop(columns=['PermitTypeCode'])

            # generate output for facility
            facility_columns = ['FacilityID', 'FacilityName', 'City',
                                'State', 'Zip', 'Latitude', 'Longitude',
                                'County', 'NAICS', 'SIC'] # 'Address' not in DMR
            dmr_facility = dmr_df[facility_columns].drop_duplicates()
            store_inventory(dmr_facility, f'DMR_{year}', 'facility')

            # generate output for flow
            flow_columns = ['FlowID', 'FlowName']
            dmr_flow = dmr_df[flow_columns].drop_duplicates()
            dmr_flow.sort_values(by=['FlowName'], inplace=True)
            dmr_flow['Compartment'] = 'water'
            dmr_flow['Unit'] = 'kg'
            store_inventory(dmr_flow, f'DMR_{year}', 'flow')

            # generate output for flowbyfacility
            fbf_columns = ['FlowName', 'FlowAmount', 'FacilityID',
                           'DataReliability']
            dmr_fbf = dmr_df[fbf_columns].reset_index(drop=True)
            dmr_fbf = aggregate(dmr_fbf, ['FacilityID', 'FlowName'])
            dmr_fbf['Compartment'] = 'water'
            dmr_fbf['Unit'] = 'kg'
            store_inventory(dmr_fbf, f'DMR_{year}', 'flowbyfacility')

            # write metadata
            generate_metadata(year, datatype='inventory')

        if kwargs['Option'] == 'C':
            download_state_totals_validation(year)


if __name__ == '__main__':
    main(Option='A', Year = [2021])
