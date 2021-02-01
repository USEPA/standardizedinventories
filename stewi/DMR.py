#!/usr/bin/env python 
'''
Queries DMR data by state, temporarily saves them,
Web service documentation found at https://echo.epa.gov/system/files/ECHO%20All%20Data%20Search%20Services_v3.pdf

This file requires paramaters be passed like:
    Option -Y Year
    A -Y 2016
Options:
A - for downloading DMR data by state
B - for downloading and generating state totals file
C - for generating StEWI output files and validation from downloaded data

'''

import os, requests
import pandas as pd
import stewi.globals as globals
from stewi.globals import set_dir, filter_inventory, filter_states,\
    validate_inventory, write_validation_result, unit_convert, modulepath,\
    output_dir, data_dir, lb_kg, get_relpath, write_metadata,\
    weighted_average, log, compile_metadata, config
import argparse

_config = config()['databases']['DMR']
dmr_data_dir = data_dir + 'DMR/'
dmr_filepath = '../DMR Data Files/'
dmr_external_dir = set_dir(dmr_filepath)

# two digit SIC codes from advanced search drop down stripped and formatted as a list
sic2 = list(pd.read_csv(dmr_data_dir + '2_digit_SIC.csv', dtype={'SIC2': str})['SIC2'])

states_df = pd.read_csv(data_dir + 'state_codes.csv')
states = list(states_df['states']) + list(states_df['dc']) + list(states_df['territories'])
states = [x for x in states if str(x) != 'nan']
base_url = _config['base_url']

# Values used for StEWI query
PARAM_GROUP = True
DETECTION = 'HALF'

big_state_list = ['CA','KY','WV']

def generate_url(report_year, base_url=base_url, sic='', region='', state='', 
                 nutrient='', nutrient_agg=False, param_group=False,
                 detection='', estimation=True, responseset='100000',
                 pageno='1', output_type='JSON'):
    
    """
    Generates the url for DMR query, see web service documentation for details        
    """
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


def query_dmr(year, sic_list=[], state_list=states, nutrient='', path=dmr_external_dir):
    """
    Loops through a set of states and sics to download and pickle DMR data
    :param sic_list: Option to break up queries further by list of 2-digit SIC codes
    :param state_list: List of states to include in query
    :param nutrient: Option to query by nutrient category with aggregation. Input 'N' or 'P'
    :param path: Path to save raw data as pickle files. Set to external directory one level above 'standardizedinventories' by default
    :return: max_error_list, no_data_list, success_list
    """
    max_error_list, no_data_list, success_list = [], [], []
    param_list = []
    path = path+str(year)+'/'
    if not os.path.exists(path):
        os.mkdir(path)
    nutrient_agg = False
    if nutrient:
        path = path + nutrient + '_'
        nutrient_agg = True

    if sic_list: param_list = [[sic, state] for sic in sic_list for state in state_list]

    if param_list:
        log.info('Breaking up queries further by SIC')
        for params in param_list:
            sic = params[0]
            state = params[1]    
            filepath = path + 'state_' + state + '_sic_' + sic + '.pickle'
            url = generate_url(report_year=year, sic=sic, state=state, 
                               nutrient=nutrient, nutrient_agg=nutrient_agg,
                               param_group=PARAM_GROUP, detection=DETECTION)
            if os.path.exists(filepath):
                log.debug('file already exists for '+ str(params) +', skipping')
                success_list.append(sic + '_' + state)
            else:
                log.info('executing query for '+ str(params))
                result = execute_query(url)
                if str(type(result)) == "<class 'str'>":
                    log.error('error in state: ' + sic + '_' + state)
                    if result == 'no_data': no_data_list.append(sic + '_' + state)
                    elif result == 'max_error': max_error_list.append(sic + '_' + state)
                else:
                    pd.to_pickle(result, filepath)
                    success_list.append(sic + '_' + state)
    else:
        for state in state_list:
            if not state in big_state_list:
                filepath = path + 'state_' + state + '.pickle'
                url = generate_url(report_year=year, state=state, param_group=PARAM_GROUP,
                                   detection=DETECTION, nutrient = nutrient,
                                   nutrient_agg = nutrient_agg) 
                if os.path.exists(filepath):
                    log.debug('file already exists for '+ state +', skipping')
                    success_list.append(state)
                else:
                    log.info('executing query for '+ state)
                    result = execute_query(url)
                    if str(type(result)) == "<class 'str'>":
                        log.error('error in state: ' + state)
                        if result == 'no_data': no_data_list.append(state)
                        elif result == 'max_error': max_error_list.append(state)
                    else:
                        pd.to_pickle(result, filepath)
                        success_list.append(state)
            else:
                counter = 1
                pages = 1
                while counter <= pages:
                    filepath = path + 'state_' + state + '_' + str(counter) + '.pickle'
                    url = generate_url(report_year=year, state=state, param_group=PARAM_GROUP,
                                       detection=DETECTION, nutrient = nutrient,
                                       nutrient_agg = nutrient_agg, responseset = '3000',
                                       pageno = str(counter))
                    if os.path.exists(filepath):
                        log.debug('file already exists for '+ state +', skipping')
                        if counter == 1:
                            result = pd.read_pickle(filepath)
                            pages = int(result['Results']['PageCount'])
                        success_list.append(state + '_' + str(counter))
                    else:
                        log.info('executing query for '+ state)
                        result = execute_query(url)
                        if str(type(result)) == "<class 'str'>":
                            log.error('error in state: ' + state)
                            if result == 'no_data': no_data_list.append(state)
                            elif result == 'max_error': max_error_list.append(state)
                        else:
                            if counter == 1: pages = int(result['Results']['PageCount'])
                            pd.to_pickle(result, filepath)
                            success_list.append(state + '_' + str(counter))
                    counter += 1

    return max_error_list, no_data_list, success_list


def execute_query(url):
    log.debug(url)
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
    """Modifies DMR data to meet StEWI specifications."""
    dmr_required_fields = pd.read_csv(dmr_data_dir + 'DMR_required_fields.txt', header=None)[0]
    output_df = input_df[dmr_required_fields]
    reliability_table = globals.reliability_table
    dmr_reliability_table = reliability_table[reliability_table['Source'] == 'DMR']
    dmr_reliability_table.drop(['Source', 'Code'], axis=1, inplace=True)
    output_df['ReliabilityScore'] = dmr_reliability_table['DQI Reliability Score'].values[0]

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
        field_dictionary['PollutantDesc']='FlowName'
        field_dictionary['PollutantCode']='FlowID'
    else:
        field_dictionary['ParameterDesc']='FlowName'
        field_dictionary['ParameterCode']='FlowID'
    output_df.rename(columns = field_dictionary, inplace=True)
    # Drop flow amount of '--'
    output_df = output_df[output_df['FlowAmount'] != '--']
    # Already in kg/yr, so no conversion necessary

    # FlowAmount is not a number
    # First remove commas
    output_df['FlowAmount'] = output_df['FlowAmount'].replace({',': ''}, regex=True)
    # Then convert to numeric
    output_df['FlowAmount'] = pd.to_numeric(output_df['FlowAmount'], errors='coerce')
    
    if PARAM_GROUP:
        flows = read_pollutant_parameter_list()
        dmr_flows = flows[['FlowName','FlowID']].drop_duplicates(subset=['FlowName'])
        output_df = output_df.merge(dmr_flows, on = 'FlowName',how = 'left')
        output_df.loc[output_df.FlowID_x.isin(flows.PARAMETER_CODE),['FlowID']] = output_df['FlowID_x']
        output_df.loc[~output_df.FlowID_x.isin(flows.PARAMETER_CODE),['FlowID']] = output_df['FlowID_y']
        output_df.drop(['FlowID_x','FlowID_y'],inplace=True,axis=1)
        
    return output_df

def generateDMR(year, nutrient='', path=dmr_external_dir):
    """Loops through pickled data and combined into a dataframe. """
    path += str(year)+'/'
    output_df = pd.DataFrame()
    if nutrient:
        path += nutrient+'_'
    for state in states:
        log.info('accessing data for ' + state)
        if not state in big_state_list:
            filepath = path + 'state_' + state + '.pickle'
            result = unpickle(filepath)
            output_df = pd.concat([output_df, result])
        else: # multiple files for each state
            counter = 1
            while True:
                try:    
                    filepath = path + 'state_' + state + '_' +str(counter)+ '.pickle'
                    result = unpickle(filepath)
                    if result is None: break
                    output_df = pd.concat([output_df, result])
                    counter+=1
                except: pass

    return output_df

def unpickle(filepath):
    try:
        result = pd.read_pickle(filepath)
    except:
        log.error(filepath.rsplit('/', 1)[-1]+' does not exist')
        return None
    if str(type(result)) == "<class 'NoneType'>":
        raise Exception('Problem with saved dataframe')
    result = pd.DataFrame(result['Results']['Results'])
    return result

def generateStateTotal(year):
    """Generates file of state totals as csv"""
    log.info('generating state totals')
    # https://echo.epa.gov/trends/loading-tool/get-data/state-statistics
    url = _config['state_url'].replace("__year__",year)
    
    state_csv = pd.read_csv(url, header=2)
    state_totals = pd.DataFrame()
    state_totals['state_name']=state_csv['State']
    state_totals['FlowName']='All'
    state_totals['Compartment']='water'
    state_totals['Amount']=state_csv['Total Pollutant Pounds (lb/yr) for Majors'] +\
        state_csv['Total Pollutant Pounds (lb/yr) for Non-Majors']
    state_totals['Unit']='lb'
    state_names = states_df[['states','state_name']]
    state_totals = state_totals.merge(state_names, how='left', on='state_name')
    state_totals.drop(columns=['state_name'], inplace=True)
    state_totals.dropna(subset=['states'], inplace=True)
    state_totals.rename(columns={'states':'State'}, inplace=True)
    state_totals.to_csv(data_dir + 'DMR_' + year + '_StateTotals.csv', index=False)

def validateStateTotals(df, year):
    """ generate validation by state, sums across species. Details on results
    by state can be found in the search results help website"""
    # https://echo.epa.gov/help/loading-tool/water-pollution-search/search-results-help-dmr
    filepath = data_dir + 'DMR_' + year + '_StateTotals.csv'
    if os.path.exists(filepath):
        reference_df = pd.read_csv(filepath)
        reference_df['FlowAmount'] = 0.0
        reference_df = unit_convert(reference_df, 'FlowAmount', 'Unit', 'lb', lb_kg, 'Amount')
        reference_df = reference_df[['FlowName', 'State', 'FlowAmount']]
        
        # to match the state totals, only compare NPD facilities, and remove some flows
        flow_exclude = pd.read_csv(data_dir + 'DMR/DMR_state_filter_list.csv')
        state_flow_exclude_list = flow_exclude['POLLUTANT_DESC'].to_list()

        dmr_by_state = df[~df['FlowName'].isin(state_flow_exclude_list)]
        dmr_by_state = dmr_by_state[dmr_by_state['PermitTypeCode']=='NPD']
        
        dmr_by_state = dmr_by_state[['State', 'FlowAmount']]
        dmr_by_state = dmr_by_state[['State', 'FlowAmount']].groupby('State').sum().reset_index()
        dmr_by_state['FlowName'] = 'All'
        validation_df = validate_inventory(dmr_by_state, reference_df)
        write_validation_result('DMR', year, validation_df)
    else:
        log.error('State totals for validation not found for ' + year)

def generate_metadata(year):
    """
    Gets metadata and writes to .json
    """

    path = dmr_filepath + str(year)
    DMR_meta = compile_metadata(path, _config, year)
    DMR_meta['SourceType'] = 'Web Service'
    #Write metadata to json
    write_metadata('DMR', year, DMR_meta)    

def read_pollutant_parameter_list(parameter_grouping = PARAM_GROUP):
    url = _config['pollutant_list_url']
    flows = pd.read_csv(url, header=1, usecols=['POLLUTANT_CODE','POLLUTANT_DESC',
                                                'PARAMETER_CODE','PARAMETER_DESC',
                                                'SRS_ID', 'NITROGEN','PHOSPHORUS',
                                                'ORGANIC_ENRICHMENT'], dtype=str)
    if parameter_grouping:
        flows.rename(columns={'POLLUTANT_DESC':'FlowName',
                              'POLLUTANT_CODE':'FlowID'}, inplace=True)
    else:
        flows.rename(columns={'PARAMETER_DESC':'FlowName',
                              'PARAMETER_CODE':'FlowID'}, inplace=True)
    return flows

def aggregate_to_facility(df):
    """
    Aggregates DMR dataframe to flow by facility
    """
    # drops rows if flow amount or reliability score is zero
    df = df[(df['FlowAmount'] > 0) & (df['ReliabilityScore'] > 0)]

    grouping_vars = ['FacilityID', 'FlowName']
    dmrbyfacility = df.groupby(grouping_vars).agg({'FlowAmount': ['sum']})
    dmrbyfacility['ReliabilityScore']=weighted_average(
        df, 'ReliabilityScore', 'FlowAmount', grouping_vars)

    dmrbyfacility = dmrbyfacility.reset_index()
    dmrbyfacility.columns = dmrbyfacility.columns.droplevel(level=1)

    return dmrbyfacility

def consolidate_nutrients(df, drop_list, nutrient):
    """
    Renames flows following nutrient aggregation to better handle flow overlaps
    """
    drop_list = drop_list['FlowName'].to_list()
    if nutrient == 'P':
        flow = ['Phosphorus', 'PHOSP']
    elif nutrient == 'N':
        flow = ['Nitrogen', 'N']
    df.loc[(df['PollutantDesc'].isin(drop_list)),['PollutantDesc','PollutantCode']]=flow
    
    return df

def remove_duplicate_organic_enrichment(df):
    """
    Facilities can report multiple forms of organic enrichment, BOD and COD,
    which represent duplicate accounting of oxygen depletion. See Myer et al.
    2020
    """
    flow_preference = 'COD'

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
    df_duplicates = df_org[df_org.duplicated(subset = 'FacilityID', keep = False)]
    if len(df_duplicates) == 0:
        return df

    df = df.loc[~df['FlowName'].isin(org_flow_list)]
    df_org = df_org[~df_org.duplicated(subset = 'FacilityID', keep = False)]
    to_be_concat = []
    to_be_concat.append(df)
    to_be_concat.append(df_org)
    
    df_duplicates['PrefList'] = df_duplicates['FlowName'].apply(lambda x: x in keep_list)
    df_duplicates['NonPrefList'] = df_duplicates['FlowName'].apply(lambda x: x not in keep_list)
    grouped = df_duplicates.groupby(['FacilityID'])
    for name, frame in grouped:
        if not frame['NonPrefList'].all():
            frame = frame[frame['PrefList']]
        to_be_concat.append(frame)       
    df = pd.concat(to_be_concat)
    df.sort_index(inplace = True)
    df.drop(columns=['PrefList','NonPrefList'], inplace = True)
    df.reset_index(inplace = True)
    return df

def remove_nutrient_overlap_TRI(df, preference):
    
    tri_list = ['AMMONIA','NITRATE COMPOUNDS']
    dmr_list = ['Nitrogen']
    combined_list = tri_list + dmr_list
    
    # for facilities where the FRS and compartment match
    if preference == 'DMR':
        keep_list = dmr_list

    df_nutrients = df.loc[((df['FlowName'].isin(combined_list)) & (df['Compartment'] == 'water'))]
    df_duplicates = df_nutrients[df_nutrients.duplicated(subset = 'FRS_ID', keep = False)]
    if len(df_duplicates) == 0:
        return df

    df = df.loc[~((df['FlowName'].isin(combined_list)) & (df['Compartment'] == 'water'))]
    df_nutrients = df_nutrients[~df_nutrients.duplicated(subset = 'FRS_ID', keep = False)]
    to_be_concat = []
    to_be_concat.append(df)
    to_be_concat.append(df_nutrients)
    
    df_duplicates['PrefList'] = df_duplicates['FlowName'].apply(lambda x: x in keep_list)
    df_duplicates['NonPrefList'] = df_duplicates['FlowName'].apply(lambda x: x not in keep_list)
    grouped = df_duplicates.groupby(['FRS_ID'])
    for name, frame in grouped:
        if not frame['NonPrefList'].all():
            frame = frame[frame['PrefList']]
        to_be_concat.append(frame)       
    df = pd.concat(to_be_concat)
    df.sort_index(inplace = True)
    df.drop(columns=['PrefList','NonPrefList'], inplace = True)
    df.reset_index(inplace = True)
        
    return df
    
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
            log.info("Querying for "+DMRyear)
   
            # Query by state, then by SIC-state where necessary
            state_max_error_list, state_no_data_list, state_success_list = query_dmr(year = DMRyear, state_list=states)
            if (len(state_max_error_list) == 0) & (len(state_no_data_list) == 0):
                log.info('all states succesfully downloaded')
            else:
                log.error('Max error: ')
                log.error(state_max_error_list)
                log.error(state_no_data_list)
                sic_state_max_error_list, sic_state_no_data_list, sic_state_success_list = query_dmr(year = DMRyear, sic_list = sic2, state_list=state_max_error_list)
            
            log.info("Querying nutrients for "+DMRyear)
            # Query aggregated nutrients data
            n_state_max_error_list, n_state_no_data_list, n_state_success_list = query_dmr(year = DMRyear, nutrient='N', state_list=states)
            n_sic_state_max_error_list, n_sic_state_no_data_list, n_sic_state_success_list = query_dmr(year = DMRyear, sic_list = sic2, state_list=n_state_max_error_list, nutrient='N')
            p_state_max_error_list, p_state_no_data_list, p_state_success_list = query_dmr(year = DMRyear, nutrient='P', state_list=states)
            p_sic_state_max_error_list, p_sic_state_no_data_list, p_sic_state_success_list = query_dmr(year = DMRyear, sic_list = sic2, state_list=p_state_max_error_list, nutrient='P')
            
        if args.Option == 'B':
            generateStateTotal(DMRyear)
        
        if args.Option == 'C':

            state_df = generateDMR(DMRyear)
            state_df = filter_states(standardize_df(state_df))

            # Validation against state totals is done prior to combinine with aggregated nutrients
            validateStateTotals(state_df, DMRyear)

            P_df = generateDMR(DMRyear, nutrient='P')
            N_df = generateDMR(DMRyear, nutrient='N')

            nut_drop_list = read_pollutant_parameter_list()
            nut_drop_list = nut_drop_list[(nut_drop_list['NITROGEN'] == 'Y') | (nut_drop_list['PHOSPHORUS'] == 'Y')]
            nut_drop_list = nut_drop_list[['FlowName']].drop_duplicates()
            
            # Consolidate N and P based flows to reflect nutrient aggregation
            P_df = consolidate_nutrients(P_df, nut_drop_list, 'P')
            N_df = consolidate_nutrients(N_df, nut_drop_list, 'N')

            nutrient_agg_df = pd.concat([P_df, N_df])
            nutrient_agg_df = filter_states(standardize_df(nutrient_agg_df))

            # Filter out nitrogen and phosphorus flows before combining with aggregated nutrients            
            dmr_nut_filtered = filter_inventory(state_df, nut_drop_list, 'drop')
            dmr_df = pd.concat([dmr_nut_filtered, nutrient_agg_df]).reset_index(drop=True)

            #PermitTypeCode needed for state validation but not maintained
            dmr_df = dmr_df.drop(columns=['PermitTypeCode'])

            # generate output for facility
            facility_columns = ['FacilityID', 'FacilityName', 'City', 'State', 'Zip', 'Latitude', 'Longitude',
                                'County', 'NAICS', 'SIC'] #'Address' not in DMR
            dmr_facility = dmr_df[facility_columns].drop_duplicates()
            dmr_facility.to_csv(set_dir(output_dir + 'facility/')+'DMR_' + DMRyear + '.csv', index=False)
            
            # generate output for flow
            flow_columns = ['FlowID','FlowName']
            dmr_flow = dmr_df[flow_columns].drop_duplicates()
            dmr_flow.sort_values(by=['FlowName'],inplace=True)
            dmr_flow['Compartment'] = 'water'
            dmr_flow['Unit'] = 'kg'
            dmr_flow.to_csv(output_dir + 'flow/DMR_' + DMRyear + '.csv', index=False)
            
            # generate output for flowbyfacility
            fbf_columns = ['FlowName', 'FlowAmount', 'FacilityID', 'ReliabilityScore']
            dmr_fbf = aggregate_to_facility(dmr_df[fbf_columns])
            dmr_fbf['Compartment'] = 'water'
            dmr_fbf['Unit'] = 'kg'
            dmr_fbf.to_csv(set_dir(output_dir + 'flowbyfacility/')+'DMR_' + DMRyear + '.csv', index=False)

            # write metadata
            generate_metadata(DMRyear)


