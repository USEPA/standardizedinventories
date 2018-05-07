#!/usr/bin/env python
# Queries DMR data by SIC or by SIC and Region (for large sets), temporarily saves them,
# Web service documentation found at https://echo.epa.gov/system/files/ECHO%20All%20Data%20Search%20Services_v3.pdf


import requests
import pandas as pd
from stewi import globals


# appends form object to be used in query to sic code
def app_param(form_obj, param_list):
    result = [form_obj + s for s in param_list]
    return result


# creates urls from various search parameters and outputs as a list
def create_urls(main_api, service_parameter, year, l, output_type, region=[]):
    urls = []
    for s in l:
        if region:
            for r in region:
                url = main_api + service_parameter + year + s + r + '&output=' + output_type
                urls.append(url)
        else:
            url = main_api + service_parameter + year + s + '&output=' + output_type
            urls.append(url)
    return urls


def query_dmr(urls, sic_list=[], reg_list=[], path=''):
    output_df = pd.DataFrame()
    max_error_list = []
    no_data_list = []
    success_list = []
    if len(urls) != len(sic_list): sic_list = [[s, r] for s in sic_list for r in reg_list]
    for i in range(len(urls)):
        # final_path = path + 'sic_' + sic[i] + '.json'
        # final_path = path + 'sic_' + sic[i]  + '.pickle'
        if len(sic_list) == len(urls): print(sic_list[i])
        while True:
            try:
                json_data = requests.get(urls[i]).json()
                df = pd.DataFrame(json_data)
                break
            except: pass
        if 'Error' in df.index:
            if df['Results'].astype(str).str.contains('Maximum').any():
                # print("iterate through by region code" + url)
                # split url by & and append region code, import function debugging
                if len(sic_list) == len(urls): max_error_list.append(sic_list[i])
            else: print("Error: " + urls[i])
        elif 'NoDataMsg' in df.index:
            if len(sic_list) == len(urls):
                print('No data found for'': ' + urls[i])
                no_data_list.append(sic_list[i])
        else:
            # with open(final_path, 'w') as fp:
            # json.dump(json_data, fp, indent = 2)
            # pd.to_pickle(df,final_path)
            df = pd.DataFrame(df['Results']['Results'])
            output_df = pd.concat([output_df, df])
            if len(sic_list) == len(urls): success_list.append(sic_list[i])
    return output_df, max_error_list, no_data_list, success_list


# creates file path for json output.
# iterates through url list, requests data, and writes to json file in output directory.
def main():
    # two digit SIC codes from advanced search drop down stripped and formatted as a list
    data_source = 'dmr'
    output_dir = globals.output_dir
    data_dir = globals.data_dir
    report_year = '2015'  # year of data requested
    sic = ['01', '02', '07', '08', '09', '10', '12', '13', '14', '15',
           '16', '17', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29',
           '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41',
           '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53',
           '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65',
           '67', '70', '72', '73', '75', '76', '78', '79', '80', '81', '82', '83',
           '84', '86', '87', '88', '89', '91', '92', '93', '95', '96', '97', '99']
    epa_region = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']
    main_api = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_'  # base url
    service_parameter = 'annual?'  # define which parameter is primary search criterion
    year = 'p_year=' + report_year  # define year
    form_obj_sic = '&p_sic2='# define any secondary search criteria
    form_obj_reg = '&p_reg='
    output_type = 'JSON'  # define output type

    # dmr_df = pd.DataFrame()

    sic_code_query = app_param(form_obj_sic, sic)
    # output_dir = set_output_dir('./output/DMRquerybySIC/')
    urls = create_urls(main_api, service_parameter, year, sic_code_query,
                       output_type)  # creates a list oof urls based on sic
    # json_output_file = get_write_json_file(urls, output_dir, 'DMR_data') #saves json file to LCI-Prime_Output
    print(report_year)
    dmr_df, sic_maximum_record_error_list, sic_no_data_list, sic_successful_df_list = \
        query_dmr(urls, sic, path=output_dir)
    if sic_successful_df_list: print('Successfully obtained data for the following SIC:\n' +
                                           str(sic_successful_df_list))
    if sic_no_data_list: print('No data for the following SIC:\n' + str(sic_no_data_list))
    if sic_maximum_record_error_list: print('Maximum record limit reached for the following SIC:\n' +
                                            str(sic_maximum_record_error_list) +
                                            '\nBreaking queries up by EPA Region...\n')
    max_error_sic_query = app_param(form_obj_sic, sic_maximum_record_error_list)
    region_query = app_param(form_obj_reg, epa_region)
    region_urls = create_urls(main_api, service_parameter, year, max_error_sic_query, output_type, region_query)
    reg_df, reg_max_error, reg_no_data, reg_success = \
        query_dmr(region_urls, sic_maximum_record_error_list, epa_region, path=output_dir)
    if reg_max_error:
        print('Maximum record limit still reached for the following [SIC, EPA Region]:\n' + str(reg_max_error))
    if reg_success: print('Successfully obtained data for [SIC, EPA Region]:\n' + str(reg_success))
    if reg_no_data: print(('No data for [SIC, EPA Region]:\n' + str(reg_no_data)))
    dmr_df = pd.concat([dmr_df, reg_df])
    # Quit here if the resulting DataFrame is empty
    if len(dmr_df) == 0:
        print('No data found for this year.')
        exit()

    dmr_required_fields = pd.read_csv(data_dir + 'DMR_required_fields.txt', header=None)[0]
    dmr_df = dmr_df[dmr_required_fields]
    reliability_table = globals.reliability_table
    dmr_reliability_table = reliability_table[reliability_table['Source'] == 'DMR']
    dmr_reliability_table.drop(['Source', 'Code'], axis=1, inplace=True)
    dmr_df['DQI Reliability Score'] = dmr_reliability_table['DQI Reliability Score']

    # Rename with standard column names
    dmr_df.rename(columns={'ExternalPermitNmbr': 'FacilityID'}, inplace=True)
    dmr_df.rename(columns={'Siccode': 'SIC'}, inplace=True)
    dmr_df.rename(columns={'NaicsCode': 'NAICS'}, inplace=True)
    dmr_df.rename(columns={'StateCode': 'State'}, inplace=True)
    dmr_df.rename(columns={'ParameterDesc': 'FlowName'}, inplace=True)
    dmr_df.rename(columns={'DQI Reliability Score': 'ReliabilityScore'}, inplace=True)
    dmr_df.rename(columns={'PollutantLoad': 'Amount'}, inplace=True)
    dmr_df = dmr_df[dmr_df['Amount'] != '--']
    # Already in kg/yr, so no conversion necessary

    file_name = data_source + '_' + report_year + '.csv'
    dmr_df.to_csv(path_or_buf=output_dir + file_name, index=False)


if __name__ == '__main__':
    main()



