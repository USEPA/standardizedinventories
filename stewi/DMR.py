#!/usr/bin/env python
# Queries DMR data by SIC or by SIC and Region (for large sets), temporarily saves them,
# Web service documentation found at https://echo.epa.gov/system/files/ECHO%20All%20Data%20Search%20Services_v3.pdf


import os, requests
import pandas as pd
import stewi.globals as globals
from stewi.globals import set_dir, filter_inventory, filter_states, validate_inventory, write_validation_result, unit_convert

data_source = 'DMR'
output_dir = globals.output_dir
data_dir = globals.data_dir
dmr_data_dir = data_dir + data_source + '/'
dmr_external_dir = set_dir(data_dir + '/../../../DMR Data Files')

# two digit SIC codes from advanced search drop down stripped and formatted as a list
report_year = '2015'  # year of data requested
sic2 = list(pd.read_csv(dmr_data_dir + '2_digit_SIC.csv', dtype={'SIC2': str})['SIC2'])
epa_region = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']
states_df = pd.read_csv(data_dir + 'state_codes.csv')
states = list(states_df['states']) + list(states_df['dc']) + list(states_df['territories'])
states = [x for x in states if str(x) != 'nan']
base_url = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_annual?'  # base url


def generate_url(base_url=base_url, report_year=report_year, sic='', region='', state='', nutrient='', nutrient_agg=False, responseset='100000', pageno='1', output_type='JSON'):
    url = base_url + 'p_year=' + report_year
    if sic: url += '&p_sic2=' + sic
    if region: url += '&p_reg=' + region
    if state: url += '&p_st=' + state
    if nutrient: url += '&p_poll_cat=Nut' + nutrient
    if nutrient_agg: url += '&p_nutrient_agg=Y'
    if responseset: url += '&ResponseSet=' + str(responseset)
    if pageno: url += '&PageNo=' + str(pageno)
    if output_type: url += '&output=' + output_type
    return url
# 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_annual?p_year=2016&p_sic2=02&responseset=500&p_poll_cat=NutN&p_nutrient_agg=Y&output=JSON'
# 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_annual?p_year=2016&p_sic2=02&responseset=500&p_poll_cat=NutP&p_nutrient_agg=Y&output=JSON'


def query_dmr(sic_list=sic2, region_list=[], state_list=[], nutrient='', path=dmr_external_dir):
    """
    :param sic_list: Uses a predefined list of 2-digit SIC codes default
    :param region_list: Option to break up queries further by list of EPA regions
    :param state_list: Option to break up queries further by list of states
    :param nutrient: Option to query by nutrient category with aggregation. Input 'N' or 'P'
    :param path: Path to save raw data as pickle files. Set to external directory one level above 'standardizedinventories' by default
    :return: output_df, max_error_list, no_data_list, success_list
    """
    output_df = pd.DataFrame()
    max_error_list, no_data_list, success_list = [], [], []
    param_list = []
    #
    if region_list: param_list = [[sic, r] for sic in sic_list for r in region_list]
    if state_list: param_list = [[sic, state] for sic in sic_list for state in state_list]
    if param_list:
        print('Breaking up queries further')
        for params in param_list:
            print(params)
            sic = params[0]
            state_or_region = params[1]
            if params in (['12', 'KY'], ['12', 'WV']): # Pagination for SIC 12 (coal mining) in KY and WV
                if nutrient: url = generate_url(sic=sic, region=state_or_region, nutrient=nutrient, nutrient_agg=True)
                else: url = generate_url(sic=sic, region=state_or_region)
                counter = 1
                pages = 1
                while counter <= pages:
                    page = str(counter)
                    if region_list:
                        if nutrient:
                            filepath = path + nutrient + '_sic_' + sic + '_' + state_or_region + '_' + page + '.pickle'
                            url = generate_url(sic=sic, region=state_or_region, nutrient=nutrient, nutrient_agg=True, responseset='500', pageno=page)
                        else:
                            filepath = path + 'sic_' + sic + '_' + state_or_region + '_' + page + '.pickle'
                            url = generate_url(sic=sic, region=state_or_region, responseset=500, pageno=page)
                    elif state_list:
                        if nutrient:
                            filepath = path + nutrient + '_sic_' + sic + '_' + state_or_region + '_' + page + '.pickle'
                            url = generate_url(sic=sic, state=state_or_region, nutrient=nutrient, nutrient_agg=True, responseset='500', pageno=page)
                        else:
                            filepath = path + 'sic_' + sic + '_' + state_or_region + '_' + page + '.pickle'
                            url = generate_url(sic=sic, state=state_or_region, responseset=500, pageno=page)
                    print(url)
                    if os.path.exists(filepath):
                        result = pd.read_pickle(filepath)
                        if str(type(result)) == "<class 'NoneType'>": raise Exception('Problem with saved dataframe')
                        if counter == 1: pages = int(result['Results']['PageCount'])
                        success_list.append(sic + '_' + state_or_region + '_' + page)
                        result = pd.DataFrame(result['Results']['Results'])
                        output_df = pd.concat([output_df, result])
                    else:
                        result = execute_query(url)
                        if str(type(result)) == "<class 'str'>":
                            if result == 'no_data': no_data_list.append(sic + '_' + state_or_region + '_' + page)
                            elif result == 'max_error': max_error_list.append(sic + '_' + state_or_region + '_' + page)
                        else:
                            if counter == 1: pages = int(result['Results']['PageCount'])
                            pd.to_pickle(result, filepath)
                            result = pd.DataFrame(result['Results']['Results'])
                            success_list.append(sic + '_' + state_or_region + '_' + page)
                            output_df = pd.concat([output_df, result])
                    counter += 1
            else: # Pagination not necessary
                if region_list:
                    if nutrient:
                        filepath = path + nutrient + '_sic_' + sic + '_' + state_or_region + '.pickle'
                        url = generate_url(sic=sic, region=state_or_region, nutrient=nutrient, nutrient_agg=True)
                    else:
                        filepath = path + 'sic_' + sic + '_' + state_or_region + '.pickle'
                        url = generate_url(sic=sic, region=state_or_region)
                elif state_list:
                    if nutrient:
                        filepath = path + nutrient + '_sic_' + sic + '_' + state_or_region + '.pickle'
                        url = generate_url(sic=sic, state=state_or_region, nutrient=nutrient, nutrient_agg=True)
                    else:
                        filepath = path + 'sic_' + sic + '_' + state_or_region + '.pickle'
                        url = generate_url(sic=sic, state=state_or_region)
                print(url)
                if os.path.exists(filepath):
                    result = pd.read_pickle(filepath)
                    if str(type(result)) == "<class 'NoneType'>": raise Exception('Problem with saved dataframe')
                    result = pd.DataFrame(result['Results']['Results'])
                    success_list.append(sic + '_' + state_or_region)
                    output_df = pd.concat([output_df, result])
                else:
                    result = execute_query(url)
                    if str(type(result)) == "<class 'str'>":
                        if result == 'no_data': no_data_list.append(sic + '_' + state_or_region)
                        elif result == 'max_error': max_error_list.append(sic + '_' + state_or_region)
                    else:
                        pd.to_pickle(result, filepath)
                        result = pd.DataFrame(result['Results']['Results'])
                        success_list.append(sic + '_' + state_or_region)
                        output_df = pd.concat([output_df, result])
    else: # 1st run through SIC codes
        for sic in sic_list:
            if sic in ['12', '49']: # Assuming SIC 12 & 49 are too big for all reporting years. Skipped for now, broken up by state later.
                max_error_list.append(sic)
                continue
            print(sic)
            if nutrient:
                filepath = path + nutrient + '_sic_' + sic + '.pickle'
                url = generate_url(sic=sic, nutrient=nutrient, nutrient_agg=True)
            else:
                filepath = path + 'sic_' + sic + '.pickle'
                url = generate_url(sic=sic)
            print(url)
            if os.path.exists(filepath):
                result = pd.read_pickle(filepath)
                if str(type(result)) == "<class 'NoneType'>": raise Exception('Problem with saved dataframe')
                result = pd.DataFrame(result['Results']['Results'])
                success_list.append(sic)
                output_df = pd.concat([output_df, result])
            else:
                result = execute_query(url)
                if str(type(result)) == "<class 'str'>":
                    if result == 'no_data': no_data_list.append(sic)
                    elif result == 'max_error': max_error_list.append(sic)
                else:
                    pd.to_pickle(result, filepath)
                    result = pd.DataFrame(result['Results']['Results'])
                    success_list.append(sic)
                    output_df = pd.concat([output_df, result])
    return output_df, max_error_list, no_data_list, success_list


def execute_query(url):
    while True:
        try:
            json_data = requests.get(url).json()
            result = pd.DataFrame(json_data)
            break
        except: pass
    #Exception handling for http 500 server error still needed
    if 'Error' in result.index:
        if result['Results'].astype(str).str.contains('Maximum').any(): return 'max_error'
        else: return 'other_error'
    elif 'NoDataMsg' in result.index: return 'no_data'
    else: return result


def standardize_df(input_df):
    dmr_required_fields = pd.read_csv(data_dir + 'DMR_required_fields.txt', header=None)[0]
    output_df = input_df[dmr_required_fields]
    reliability_table = globals.reliability_table
    dmr_reliability_table = reliability_table[reliability_table['Source'] == 'DMR']
    dmr_reliability_table.drop(['Source', 'Code'], axis=1, inplace=True)
    output_df['ReliabilityScore'] = dmr_reliability_table['DQI Reliability Score']

    # Rename with standard column names
    output_df.rename(columns={'ExternalPermitNmbr': 'FacilityID'}, inplace=True)
    output_df.rename(columns={'Siccode': 'SIC'}, inplace=True)
    output_df.rename(columns={'NaicsCode': 'NAICS'}, inplace=True)
    output_df.rename(columns={'StateCode': 'State'}, inplace=True)
    output_df.rename(columns={'ParameterDesc': 'FlowName'}, inplace=True)
    output_df.rename(columns={'DQI Reliability Score': 'ReliabilityScore'}, inplace=True)
    output_df.rename(columns={'PollutantLoad': 'FlowAmount'}, inplace=True)
    output_df.rename(columns={'CountyName': 'County'}, inplace=True)
    output_df.rename(columns={'GeocodeLatitude': 'Latitude'}, inplace=True)
    output_df.rename(columns={'GeocodeLongitude': 'Longitude'}, inplace=True)
    # Drop flow amount of '--'
    output_df = output_df[output_df['FlowAmount'] != '--']
    # Already in kg/yr, so no conversion necessary

    # FlowAmount is not a number
    # First remove commas
    output_df['FlowAmount'] = output_df['FlowAmount'].replace({',': ''}, regex=True)
    # Then convert to numeric
    output_df['FlowAmount'] = pd.to_numeric(output_df['FlowAmount'], errors='coerce')
    return output_df


print(report_year)

# Query by SIC, then by SIC-state where necessary
sic_df, sic_max_error_list, sic_no_data_list, sic_success_list = query_dmr()
sic_state_df, sic_state_max_error_list, sic_state_no_data_list, sic_state_success_list = query_dmr(sic_list=sic_max_error_list, state_list=states)
sic_df = pd.concat([sic_df, sic_state_df])
sic_df = filter_states(standardize_df(sic_df))# TODO: Skip querying of US territories for optimization

# Query and combine aggregated nutrients data
n_sic_df, n_sic_max_error_list, n_sic_no_data_list, n_sic_success_list = query_dmr(nutrient='N')
n_sic_state_df, n_sic_state_max_error_list, n_sic_state_no_data_list, n_sic_state_success_list = query_dmr(sic_list=n_sic_max_error_list, state_list=states, nutrient='N')
p_sic_df, p_sic_max_error_list, p_sic_no_data_list, p_sic_success_list = query_dmr(nutrient='P')
p_sic_state_df, p_sic_state_max_error_list, p_sic_state_no_data_list, p_sic_state_success_list = query_dmr(sic_list=p_sic_max_error_list, state_list=states, nutrient='P')
nutrient_agg_df = pd.concat([n_sic_df, p_sic_df, n_sic_state_df, p_sic_state_df])
nutrient_agg_df = filter_states(standardize_df(nutrient_agg_df))# TODO: Skip querying of US territories for optimization

# Quit here if the resulting DataFrame is empty
if len(sic_df) == 0:
    print('No data found for this year.')
    exit()


# Validation by state sums across species
filepath = data_dir + 'DMR_' + report_year + '_StateTotals.csv'
if os.path.exists(filepath):
    reference_df = pd.read_csv(filepath)
    reference_df['FlowAmount'] = 0.0
    reference_df = unit_convert(reference_df, 'FlowAmount', 'Unit', 'lb', 0.4535924, 'Amount')
    reference_df = reference_df[['FlowName', 'State', 'FlowAmount']]
    dmr_by_state = sic_df[['State', 'FlowAmount']].groupby('State').sum().reset_index()
    dmr_by_state['FlowName'] = 'All'
    validation_df = validate_inventory(dmr_by_state, reference_df)
    write_validation_result(data_source, report_year, validation_df)
else: print('State totals for validation not found for ' + report_year)

# Filter out nitrogen and phosphorus flows before combining with aggregated nutrients
dmr_unagg_nut = sic_df
nut_drop_list = pd.read_csv(dmr_data_dir + 'DMR_Parameter_List_10302018.csv')
nut_drop_list.rename(columns={'PARAMETER_DESC': 'ParameterDesc'}, inplace=True)
nut_drop_list = nut_drop_list[(nut_drop_list['NITROGEN'] == 'Y') | (nut_drop_list['PHOSPHORUS'] == 'Y')]
nut_drop_list = nut_drop_list[['ParameterDesc']]
dmr_nut_filtered = filter_inventory(dmr_unagg_nut, nut_drop_list, 'drop')
dmr_df = pd.concat([dmr_nut_filtered, nutrient_agg_df]).reset_index(drop=True)

# if output_format == 'facility':
facility_columns = ['FacilityID', 'FacilityName', 'City', 'State', 'Zip', 'Latitude', 'Longitude',
                    'County', 'NAICS', 'SIC'] #'Address' not in DMR
dmr_facility = dmr_df[facility_columns].drop_duplicates()
dmr_facility.to_csv(set_dir(output_dir + 'facility/')+'DMR_' + report_year + '.csv', index=False)
# # elif output_format == 'flow':
flow_columns = ['FlowName']
dmr_flow = dmr_df[flow_columns].drop_duplicates()
dmr_flow['Compartment'] = 'water'
dmr_flow['Unit'] = 'kg'
dmr_flow.to_csv(output_dir + 'flow/DMR_' + report_year + '.csv', index=False)
# elif output_format == 'flowbyfacility':
fbf_columns = ['FlowName', 'FlowAmount', 'FacilityID', 'ReliabilityScore']
dmr_fbf = dmr_df[fbf_columns].drop_duplicates()
dmr_fbf['Compartment'] = 'water'
dmr_fbf['Unit'] = 'kg'
dmr_fbf.to_csv(set_dir(output_dir + 'flowbyfacility/')+'DMR_' + report_year + '.csv', index=False)



