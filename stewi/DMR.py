#!/usr/bin/env python 
'''
Queries DMR data by SIC or by SIC and Region (for large sets), temporarily saves them,
Web service documentation found at https://echo.epa.gov/system/files/ECHO%20All%20Data%20Search%20Services_v3.pdf

Updates:
-use argeparse to set up similar to TRI and breakout steps, sequentially
-pass year as parameter
-store data downloads by year
-adjust state validation to only check facilities of type NPD


'''

import os, requests, csv
import pandas as pd
import stewi.globals as globals
from stewi.globals import set_dir, filter_inventory, filter_states,\
    validate_inventory, write_validation_result, unit_convert, modulepath,\
    output_dir, data_dir, lb_kg    
import argparse

dmr_data_dir = data_dir + 'DMR/'
dmr_external_dir = set_dir(modulepath+'../../DMR Data Files')

# two digit SIC codes from advanced search drop down stripped and formatted as a list
sic2 = list(pd.read_csv(dmr_data_dir + '2_digit_SIC.csv', dtype={'SIC2': str})['SIC2'])
#sic2 = sic2[0:5] # for QA
epa_region = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']
states_df = pd.read_csv(data_dir + 'state_codes.csv')
states = list(states_df['states']) + list(states_df['dc']) + list(states_df['territories'])
states = [x for x in states if str(x) != 'nan']
base_url = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_annual?'  # base url


def generate_url(report_year, base_url=base_url, sic='', region='', state='', 
                 nutrient='', nutrient_agg=False, param_group=False, detection='', estimation=True,
                 responseset='100000', pageno='1', output_type='JSON'):
    # web service documentation: https://echo.epa.gov/tools/web-services/loading-tool#/Custom%20Search/get_dmr_rest_services_get_custom_data_facility
    url = base_url + 'p_year=' + report_year
    if sic: url += '&p_sic2=' + sic
    if region: url += '&p_reg=' + region
    if state: url += '&p_st=' + state
    if nutrient: url += '&p_poll_cat=Nut' + nutrient
    if nutrient_agg: url += '&p_nutrient_agg=Y' # default is N
    if param_group: url += '&p_param_group=Y' # default is N
    if detection: url += '&p_nd=' + detection # default is ZERO
    if not estimation: url += '&p_est=N' # default is Y
    if responseset: url += '&ResponseSet=' + str(responseset)
    if pageno: url += '&PageNo=' + str(pageno)
    if output_type: url += '&output=' + output_type
    return url


def query_dmr(year, sic_list=sic2, region_list=[], state_list=[], nutrient='', path=dmr_external_dir):
    """
    :param sic_list: Uses a predefined list of 2-digit SIC codes default
    :param region_list: Option to break up queries further by list of EPA regions
    :param state_list: Option to break up queries further by list of states
    :param nutrient: Option to query by nutrient category with aggregation. Input 'N' or 'P'
    :param path: Path to save raw data as pickle files. Set to external directory one level above 'standardizedinventories' by default
    :return: output_df, max_error_list, no_data_list, success_list
    """
    max_error_list, no_data_list, success_list = [], [], []
    param_list = []
    path = path+str(year)+'/'
    if not os.path.exists(path):
        os.mkdir(path)
    #
    if region_list: param_list = [[sic, r] for sic in sic_list for r in region_list]
    if state_list: param_list = [[sic, state] for sic in sic_list for state in state_list]
    if param_list:
        print('Breaking up queries further')
        for params in param_list:
            sic = params[0]
            state_or_region = params[1]
            if params in (['12', 'KY'], ['12', 'WV']): # Pagination for SIC 12 (coal mining) in KY and WV
                if nutrient: url = generate_url(report_year=year, sic=sic, region=state_or_region, nutrient=nutrient, nutrient_agg=True)
                else: url = generate_url(report_year=year, sic=sic, region=state_or_region)
                counter = 1
                pages = 1
                while counter <= pages:
                    page = str(counter)
                    if region_list:
                        if nutrient:
                            filepath = path + nutrient + '_sic_' + sic + '_' + state_or_region + '_' + page + '.pickle'
                            url = generate_url(report_year=year, sic=sic, region=state_or_region, nutrient=nutrient, nutrient_agg=True, responseset='500', pageno=page)
                        else:
                            filepath = path + 'sic_' + sic + '_' + state_or_region + '_' + page + '.pickle'
                            url = generate_url(report_year=year, sic=sic, region=state_or_region, responseset=500, pageno=page)
                    elif state_list:
                        if nutrient:
                            filepath = path + nutrient + '_sic_' + sic + '_' + state_or_region + '_' + page + '.pickle'
                            url = generate_url(report_year=year, sic=sic, state=state_or_region, nutrient=nutrient, nutrient_agg=True, responseset='500', pageno=page)
                        else:
                            filepath = path + 'sic_' + sic + '_' + state_or_region + '_' + page + '.pickle'
                            url = generate_url(report_year=year, sic=sic, state=state_or_region, responseset=500, pageno=page)
                    if os.path.exists(filepath):
                        print('file already exists for '+ str(params) +' page '+str(counter)+', skipping query')
                        if counter == 1:
                            result = pd.read_pickle(filepath)
                            pages = int(result['Results']['PageCount'])
                        success_list.append(sic + '_' + state_or_region + '_' + str(page))
                    else:
                        print('executing query for '+ str(params) +' page '+ str(counter))
                        result = execute_query(url)
                        if str(type(result)) == "<class 'str'>":
                            if result == 'no_data': no_data_list.append(sic + '_' + state_or_region + '_' + page)
                            elif result == 'max_error': max_error_list.append(sic + '_' + state_or_region + '_' + page)
                        else:
                            if counter == 1: pages = int(result['Results']['PageCount'])
                            pd.to_pickle(result, filepath)
                            success_list.append(sic + '_' + state_or_region + '_' + str(page))
                    counter += 1
            else: # Pagination not necessary
                if region_list:
                    if nutrient:
                        filepath = path + nutrient + '_sic_' + sic + '_' + state_or_region + '.pickle'
                        url = generate_url(report_year=year, sic=sic, region=state_or_region, nutrient=nutrient, nutrient_agg=True)
                    else:
                        filepath = path + 'sic_' + sic + '_' + state_or_region + '.pickle'
                        url = generate_url(report_year=year, sic=sic, region=state_or_region)
                elif state_list:
                    if nutrient:
                        filepath = path + nutrient + '_sic_' + sic + '_' + state_or_region + '.pickle'
                        url = generate_url(report_year=year, sic=sic, state=state_or_region, nutrient=nutrient, nutrient_agg=True)
                    else:
                        filepath = path + 'sic_' + sic + '_' + state_or_region + '.pickle'
                        url = generate_url(report_year=year, sic=sic, state=state_or_region)
                if os.path.exists(filepath):
                    print('file already exists for '+ str(params) +', , skipping')
                    success_list.append(sic + '_' + state_or_region)
                else:
                    print('executing query for '+ str(params))
                    result = execute_query(url)
                    if str(type(result)) == "<class 'str'>":
                        if result == 'no_data': no_data_list.append(sic + '_' + state_or_region)
                        elif result == 'max_error': max_error_list.append(sic + '_' + state_or_region)
                    else:
                        pd.to_pickle(result, filepath)
                        success_list.append(sic + '_' + state_or_region)
    else: # 1st run through SIC codes
        for sic in sic_list:
            if sic in ['12', '49']: # Assuming SIC 12 & 49 are too big for all reporting years. Skipped for now, broken up by state later.
                max_error_list.append(sic)
                continue
            if nutrient:
                filepath = path + nutrient + '_sic_' + sic + '.pickle'
                url = generate_url(report_year=year, sic=sic, nutrient=nutrient, nutrient_agg=True)
            else:
                filepath = path + 'sic_' + sic + '.pickle'
                url = generate_url(report_year=year, sic=sic)
            if os.path.exists(filepath):
                print('file already exists for '+ sic +', skipping')
                success_list.append(sic)
            else:
                print('executing query for '+ sic)
                result = execute_query(url)
                if str(type(result)) == "<class 'str'>":
                    if result == 'no_data': no_data_list.append(sic)
                    elif result == 'max_error': max_error_list.append(sic)
                else:
                    pd.to_pickle(result, filepath)
                    success_list.append(sic)
    return max_error_list, no_data_list, success_list


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
    output_df.rename(columns={'ExternalPermitNmbr': 'FacilityID',
                              'Siccode': 'SIC',
                              'NaicsCode': 'NAICS',
                              'StateCode': 'State',
                              'ParameterDesc': 'FlowName',
                              'DQI Reliability Score': 'ReliabilityScore',
                              'PollutantLoad': 'FlowAmount',
                              'CountyName': 'County',
                              'GeocodeLatitude': 'Latitude',
                              'GeocodeLongitude': 'Longitude'}, inplace=True)
    # Drop flow amount of '--'
    output_df = output_df[output_df['FlowAmount'] != '--']
    # Already in kg/yr, so no conversion necessary

    # FlowAmount is not a number
    # First remove commas
    output_df['FlowAmount'] = output_df['FlowAmount'].replace({',': ''}, regex=True)
    # Then convert to numeric
    output_df['FlowAmount'] = pd.to_numeric(output_df['FlowAmount'], errors='coerce')
    return output_df

def generateDMR(year, nutrient='', path=dmr_external_dir):
    path += str(year)+'/'
    output_df = pd.DataFrame()
    if nutrient:
        path += nutrient+'_'
    for sic in sic2:
        print('accessing data for ' + sic)
        # cycle through combination of options:
        filepath = path + 'sic_' + sic + '.pickle'
        if sic == '12' or sic == '49':
            for state in states:
                if sic == '12' and (state == 'WV' or state == 'KY'):
                    print('todo get WV/KY')
                filepath = path + 'sic_' + sic + '_'+state+'.pickle'
                result = unpickle(filepath)
                output_df = pd.concat([output_df, result])
        else:
            result = unpickle(filepath)
            output_df = pd.concat([output_df, result])
    return output_df

def unpickle(filepath):
    try:
        result = pd.read_pickle(filepath)
    except:
        print(filepath.rsplit('/', 1)[-1]+' does not exist')
        return None
    if str(type(result)) == "<class 'NoneType'>":
        raise Exception('Problem with saved dataframe')
    result = pd.DataFrame(result['Results']['Results'])
    return result

def generateStateTotal(year):
    print('generating state totals')
    # https://echo.epa.gov/trends/loading-tool/get-data/state-statistics
    # https://ofmpub.epa.gov/echo/dmr_rest_services.get_state_stats?p_year=2020&output=csv
    url = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_state_stats?p_year=' + year + '&output=csv'
    
    state_csv = pd.read_csv(url, header=2)
    state_totals = pd.DataFrame()
    state_totals['Amount']=state_csv['Total Pollutant Pounds (lb/yr) for Majors']+state_csv['Total Pollutant Pounds (lb/yr) for Non-Majors']
    state_totals['FlowName']='All'
    state_totals['Compartment']='All'
        
    return

if __name__ == '__main__':

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Extract DMR files from web.\
                        [B] State Totals for DMR.\
                        [C] Organize files',
                        type = str)

    parser.add_argument('-Y', '--Year', nargs = '+',
                        help = 'What DMR year you want to retrieve',
                        type = str)

    args = parser.parse_args()

    DMRyears = args.Year
    
    for DMRyear in DMRyears:
        
        if args.Option == 'A':
            print("Querying for "+DMRyear)
        
            # Query by SIC, then by SIC-state where necessary
            sic_max_error_list, sic_no_data_list, sic_success_list = query_dmr(year = DMRyear)
            sic_state_max_error_list, sic_state_no_data_list, sic_state_success_list = query_dmr(year = DMRyear, sic_list=sic_max_error_list, state_list=states)
            
            # Query aggregated nutrients data
            n_sic_max_error_list, n_sic_no_data_list, n_sic_success_list = query_dmr(year = DMRyear, nutrient='N')
            n_sic_state_max_error_list, n_sic_state_no_data_list, n_sic_state_success_list = query_dmr(year = DMRyear, sic_list=n_sic_max_error_list, state_list=states, nutrient='N')
            p_sic_max_error_list, p_sic_no_data_list, p_sic_success_list = query_dmr(year = DMRyear, nutrient='P')
            p_sic_state_max_error_list, p_sic_state_no_data_list, p_sic_state_success_list = query_dmr(year = DMRyear, sic_list=p_sic_max_error_list, state_list=states, nutrient='P')
            
        if args.Option == 'B':
            generateStateTotal(DMRyear)
        
        if args.Option == 'C':
            
            sic_df = generateDMR(DMRyear)
            sic_df = filter_states(standardize_df(sic_df))# TODO: Skip querying of US territories for optimization

            P_df = generateDMR(DMRyear, nutrient='P')
            N_df = generateDMR(DMRyear, nutrient='N')
            nutrient_agg_df = pd.concat([P_df, N_df])

            # Filter out nitrogen and phosphorus flows before combining with aggregated nutrients
            nutrient_agg_df = filter_states(standardize_df(nutrient_agg_df))# TODO: Skip querying of US territories for optimization
            
            #sic_df.to_csv(output_dir+'DMR_sic_df_standardized.csv')
            #nutrient_agg_df.to_csv(output_dir+'DMR_nut_df_standardized.csv')
            #sic_df = pd.read_csv(output_dir+'DMR_sic_df_standardized.csv')
            #nutrient_agg_df = pd.read_csv(output_dir+'DMR_nut_df_standardized.csv')
            
            nut_drop_list = pd.read_csv(dmr_data_dir + 'DMR_Parameter_List_10302018.csv')
            nut_drop_list.rename(columns={'PARAMETER_DESC': 'ParameterDesc'}, inplace=True)
            nut_drop_list = nut_drop_list[(nut_drop_list['NITROGEN'] == 'Y') | (nut_drop_list['PHOSPHORUS'] == 'Y')]
            nut_drop_list = nut_drop_list[['ParameterDesc']]
            dmr_nut_filtered = filter_inventory(sic_df, nut_drop_list, 'drop')
            dmr_df = pd.concat([dmr_nut_filtered, nutrient_agg_df]).reset_index(drop=True)
            #PermitTypeCode needed for state validation but not maintained
            dmr_df = dmr_df.drop(columns=['PermitTypeCode'])
            
            # generate validation by state sums across species
            filepath = data_dir + 'DMR_' + DMRyear + '_StateTotals.csv'
            if os.path.exists(filepath):
                reference_df = pd.read_csv(filepath)
                reference_df['FlowAmount'] = 0.0
                reference_df = unit_convert(reference_df, 'FlowAmount', 'Unit', 'lb', lb_kg, 'Amount')
                reference_df = reference_df[['FlowName', 'State', 'FlowAmount']]
                dmr_by_state = sic_df[['State', 'FlowAmount','PermitTypeCode']]
                dmr_by_state = dmr_by_state[dmr_by_state['PermitTypeCode']=='NPD']
                dmr_by_state = dmr_by_state[['State', 'FlowAmount']].groupby('State').sum().reset_index()
                dmr_by_state['FlowName'] = 'All'
                validation_df = validate_inventory(dmr_by_state, reference_df)
                write_validation_result('DMR', DMRyear, validation_df)
            else:
                print('State totals for validation not found for ' + DMRyear)
            

            # generate output for facility
            facility_columns = ['FacilityID', 'FacilityName', 'City', 'State', 'Zip', 'Latitude', 'Longitude',
                                'County', 'NAICS', 'SIC'] #'Address' not in DMR
            dmr_facility = dmr_df[facility_columns].drop_duplicates()
            dmr_facility.to_csv(set_dir(output_dir + 'facility/')+'DMR_' + DMRyear + '.csv', index=False)
            
            # generate output for flow
            flow_columns = ['FlowName']
            dmr_flow = dmr_df[flow_columns].drop_duplicates()
            dmr_flow['Compartment'] = 'water'
            dmr_flow['Unit'] = 'kg'
            dmr_flow.to_csv(output_dir + 'flow/DMR_' + DMRyear + '.csv', index=False)
            
            # generate output for flowbyfacility
            fbf_columns = ['FlowName', 'FlowAmount', 'FacilityID', 'ReliabilityScore']
            dmr_fbf = dmr_df[fbf_columns].drop_duplicates()
            dmr_fbf['Compartment'] = 'water'
            dmr_fbf['Unit'] = 'kg'
            dmr_fbf.to_csv(set_dir(output_dir + 'flowbyfacility/')+'DMR_' + DMRyear + '.csv', index=False)
            
            # TODO: write metadata
            # 
            


