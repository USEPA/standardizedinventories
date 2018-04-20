#Queries DMR data by SIC or by SIC and Region (for large sets), temporarily saves them,
# Web service documentation can be found at https://echo.epa.gov/system/files/ECHO%20All%20Data%20Search%20Services_v3.pdf


import requests
import pandas as pd
from StandardizedReleaseAndWasteInventories import globals

# two digit SIC codes from advanced search drop down stripped and formatted as a list
sic = ['01', '02', '07', '08', '09', '10', '12', '13', '14', '15',
       '16', '17', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29'
    , '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41',
       '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53',
       '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65'
    , '67', '70', '72', '73', '75', '76', '78', '79', '80', '81', '82'
    , '83', '84', '86', '87', '88', '89', '91', '92', '93', '95', '96', '97', '99']
epa_region = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']

sic_maximum_record_error_list = []
sic_no_data_list = []
sic_successful_df_list = []


# appends form object to be used in query to sic code
def app_sic(form_obj, param_list):
    result = [form_obj + s for s in param_list]
    return result


# creates urls from various search parameters and outputs as a list
def create_urls(main_api, service_parameter, year, l, output_type, region=False):
    urls = []
    for s in l:
        if region:
            for r in epa_region:
                url = main_api + service_parameter + year + s + '&p_reg=' + r + '&output=' + output_type
                urls.append(url)
        else:
            url = main_api + service_parameter + year + s + '&output=' + output_type
            urls.append(url)
    return urls


def queryDMR(urls, path):
    output_df = pd.DataFrame()
    i = 0
    for url in urls:
        # final_path = path + 'sic_' + sic[i] + '.json'
        # final_path = path + 'sic_' + sic[i]  + '.pickle'
        json_data = requests.get(url).json()
        df = pd.DataFrame(json_data)
        if 'Error' in df.index:
            if df['Results'].astype(str).str.contains('Maximum').any():
                # print("iterate through by region code" + url)
                # split url by & and append region code, import function debugging
                sic_maximum_record_error_list.append(sic[i])
            else:
                print("Error:" + url)
        elif 'NoDataMsg' in df.index:
            print('No data found for:' + url)
            sic_no_data_list.append(sic[i])
        else:
            # with open(final_path, 'w') as fp:
            # json.dump(json_data, fp, indent = 2)
            # pd.to_pickle(df,final_path)
            df = pd.DataFrame(df['Results']['Results'])
            output_df = pd.concat([output_df, df])
            sic_successful_df_list.append(sic[i])
        i += 1
    return output_df

# results on 3/22 4pm
# sic_maximum_record_error_list = ['12', '49']
# sic_no_data_list = ['81', '93']
# sic_successful_df_list = ['01', '02', '07', '08', '09', '10', '13', '14', '15', '16', '17', '20', '21', '22', '23', '24', '25', '26','27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '67', '70', '72', '73', '75', '76', '78', '79', '80', '82', '83', '84', '86', '87', '88', '89', '91', '92', '95', '96', '97', '99']

# write function to query for SIC codes that ran into maximum record errors
# iterate through sic_maximum_record_error_list

# creates file path for json output. irterates through url list, requests data, and writes to json file in output directory.
def main():
    data_source = 'dmr'
    output_dir = globals.output_dir
    data_dir = globals.data_dir
    DMR_year = '2015'  # year of data requested
    main_api = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_'  # base url
    service_parameter = 'annual?'  # define which parameter is primary search criterion
    year = 'p_year=' + DMR_year  # define year
    form_obj = '&p_sic2='  # define any secondary search criteria
    output_type = 'JSON'  # define output type

    #dmr_df = pd.DataFrame()

    sic_code_query = app_sic(form_obj, sic)
    #output_dir = set_output_dir('./output/DMRquerybySIC/')
    urls = create_urls(main_api, service_parameter, year, sic_code_query,
                       output_type)  # creates a list oof urls based on sic
    # json_output_file = get_write_json_file(urls, output_dir, 'DMR_data') #saves json file to LCI-Prime_Output
    dmr_df = queryDMR(urls, output_dir)
    max_error_list_query = app_sic(form_obj, sic_maximum_record_error_list)
    region_urls = create_urls(main_api, service_parameter, year, max_error_list_query, output_type, region=True)
    dmr_df = pd.concat([dmr_df, queryDMR(region_urls, output_dir)])


    dmr_required_fields = pd.read_csv(data_dir + 'DMR_required_fields.txt',header=None)[0]
    dmr_df = dmr_df[dmr_required_fields]
    reliability_table = globals.reliability_table
    dmr_reliability_table = reliability_table[reliability_table['Source'] == 'DMR']
    dmr_reliability_table.drop('Source', axis=1, inplace=True)
    dmr_df['DQI Reliability Score'] = dmr_reliability_table['DQI Reliability Score']

    # Rename with standard column names
    dmr_df.rename(columns={'ExternalPermitNmbr': 'FacilityID'}, inplace=True)
    dmr_df.rename(columns={'Siccode': 'SIC'}, inplace=True)
    dmr_df.rename(columns={'StateCode': 'State'}, inplace=True)
    dmr_df.rename(columns={'ParameterDesc': 'FlowName'}, inplace=True)
    dmr_df.rename(columns={'DQI Reliability Score': 'ReliabilityScore'}, inplace=True)
    dmr_df.rename(columns={'PollutantLoad': 'Amount'}, inplace=True)
    dmr_df = dmr_df[dmr_df['Amount'] != '--']
    # Already in kg/yr, so no conversion necessary

    file_name = 'dmr_' + DMR_year + '.csv'
    dmr_df.to_csv(path_or_buf=output_dir + file_name, index=False)


if __name__ == '__main__':
    main()



