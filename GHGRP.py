#GHGRP import and processing

import pandas as pd
import numpy as np
import requests
from xml.dom import minidom
import os

#Define URLs to obtain data using EnviroFacts RESTful data service API
enviro_url = 'https://iaspub.epa.gov/enviro/efservice/'
subparts_url = enviro_url + 'PUB_DIM_SUBPART/JSON'
ghgs_url = enviro_url + 'PUB_DIM_GHG/JSON'

def followURL(input_url):
    for i in range(0,3):
        try: 
            if input_url[-3:].lower() == 'csv': output_data = pd.read_csv(input_url)
            elif input_url[-4:].lower() == 'json': output_data = pd.read_json(input_url)
            elif input_url[-5:]=='excel' or ('xls' in input_url[-4:]): output_data = pd.read_excel(input_url, sheetname=None)
            break
        except ValueError: pass
        except: 
            if i == 2: raise
    return output_data

#Create subparts and GHGs tables from JSON output of EF RESTful data service API
subparts = followURL(subparts_url)#[['SUBPART_NAME','SUBPART_CATEGORY']]
ghgs = followURL(ghgs_url)
                
#Clean up misencoded subscripts
for table in [subparts,ghgs]:
    for column in table.select_dtypes([np.object]): 
        table[column] = table[column].str.replace('%3Csub%3E','').str.replace('%3C/sub%3E','')

name_cols = ['GAS_NAME','GHG_GAS_NAME','GHG_NAME','FGHG_GROUP_NAME'             'PROCESS_NAME','PROCESS_TYPE','OTHER_GAS_GHG_GROUP','OTHER_GHG_NAME',             'OTHER_GREENHOUSE_GAS_NAME'            ]
quantity_cols = ['ANN_FGHG_EMISSIONS_BY_PROCTYPE','GHG_QUANTITY','PRO_VENT_EM_BASED_ON_MISS_DATA',                 'PRO_VENT_MISSING_DATA_ESTIMATE','EQUIPEAK_MISSING_DATA_ESTIMATE',                 'EQUIP_LEAK_EM_BASED_MISS_DATA'                ]
ch4_cols = ['T4CH4COMBUSTIONEMISSIONS','TIER1_CH4_COMBUSTION_EMISSIONS','TIER2_CH4_COMBUSTION_EMISSIONS',            'TIER3_CH4_COMBUSTION_EMISSIONS','TIER_1_CH4_EMISSIONS','TIER_2_CH4_EMISSIONS','TIER_3_CH4_EMISSIONS'           ]
n2o_cols = ['T4N2OCOMBUSTIONEMISSIONS','TIER1_N2O_COMBUSTION_EMISSIONS','TIER2_N2O_COMBUSTION_EMISSIONS',            'TIER3_N2O_COMBUSTION_EMISSIONS','TIER_1_N2O_EMISSIONS','TIER_2_N2O_EMISSIONS','TIER_3_N2O_EMISSIONS',            'TOTAL_ANN_N2O_CHEM_VAP_DEP_EMI','TOT_ANN_N2O_OTHR_ELE_MANU_PROC'           ]
co2_cols = ['CO2_QUANTITY','TIER1_CO2_COMBUSTION_EMISSIONS','TIER2_CO2_COMBUSTION_EMISSIONS','TIER3_CO2_COMBUSTION_EMISSIONS',            'TIER_1_CO2_EMISSIONS','TIER_2_CO2_EMISSIONS','TIER_3_CO2_EMISSIONS'           ]
co2e_cols = ['PART_75_CH4_EMISSIONS_CO2E','N2O_EMISSIONS_CO2E','CH4_EMISSIONS_CO2E','PART_75_N2O_EMISSIONS_CO2E',             'EQUIPMENT_LEAK_CO2E','PROCESS_VENT_CO2E'            ]
method_cols = ['AVERAGE_EF_APPROACH','EF_METHOD_USED','EMISSIONS_METHOD',               'EQUIP_LEAK_MISSING_DATAMETHOD','TIER_1_METHOD_EQUATION','TIER_2_METHOD_EQUATION',               'TIER_3_METHOD_EQUATION','PRO_VENT_MISSING_DATA_METHOD'               'ECF_METHOD_USED'              ]
base_cols =  ['SUBPART_NAME'] + ['FACILITY_ID']# + ['GHG_QUANTITY_UNIT_OF_MEASURE']
info_cols = name_cols + quantity_cols + method_cols
group_cols =  ch4_cols + n2o_cols + co2_cols + co2e_cols
ghg_cols = base_cols + info_cols + group_cols
ghg_tables = ['_SUBPART_LEVEL_INFORMATION','_FUEL_LEVEL_INFORMATION','_FOSSIL_FUEL_INFORMATION',              '_SUBPART_GHG_INFO','_SUBPART_LEVEL_INFO','MV_EF_I_ANN_FGHG_PVMEMSLCD','MV_EF_I_FAB_N2O_EMISSIONS',              'EF_L_PROCESS_EF_ECF'             ]

ghgrp = pd.DataFrame(columns = ghg_cols)

#Define EnviroFacts URL and query criteria
report_year = '2015'
row_start = '0'
row_end = '5'

def generateURL(subpart_name,report_year,row_start,row_end):
    request_url1, request_url2 = enviro_url, ''
    #TODO: Subpart W has zero results--figure out which table to use instead.
    if subpart_name in ('F','G','H','K','N','Q','R','S','T','U','V','W','X','Y','Z',                        'DD','FF','EE','GG','HH','II','MM','NN','PP','SS'): 
        request_url1 += subpart_name + '_SUBPART_LEVEL_INFORMATION'
    elif subpart_name in ('C','D'): request_url1 += subpart_name + '_FUEL_LEVEL_INFORMATION'
    elif subpart_name == 'AA': request_url1 += subpart_name + '_FOSSIL_FUEL_INFORMATION'
    elif subpart_name == 'TT': request_url1 += subpart_name + '_SUBPART_GHG_INFO'
    elif subpart_name == 'P': request_url1 += subpart_name + '_SUBPART_LEVEL_INFO'
    elif subpart_name == 'L': request_url1 += 'EF_L_PROCESS_EF_ECF'
    elif subpart_name == 'I': 
        request_url1 += 'MV_EF_I_ANN_FGHG_PVMEMSLCD'
        request_url2 += enviro_url + 'MV_EF_I_FAB_N2O_EMISSIONS'
        if report_year != '': request_url2 += '/REPORTING_YEAR/=/' + report_year
        if row_start != '': request_url2 += '/ROWS/' + row_start + ':' + row_end
        request_url2 += '/JSON'
    else: return False, ''
    if report_year != '': request_url1 += '/REPORTING_YEAR/=/' + report_year
    if row_start != '': request_url1 += '/ROWS/' + row_start + ':' + row_end
    request_url1 += '/JSON'
    return request_url1, request_url2

for index, row in subparts.iterrows():
    print('index: ',index,'\n')
    #generate a URL for the tables to try
    request_url1, request_url2 = generateURL(row['SUBPART_NAME'],report_year,row_start,row_end)
    print(request_url1, request_url2,'\n')
    if request_url1 == False: continue
    subpart_df = followURL(request_url1)
    subpart_df['SUBPART_NAME'] = row['SUBPART_NAME']
    ghgrp = pd.concat([ghgrp,subpart_df])
    if row['SUBPART_NAME'] == 'I':
        subpart_df = followURL(request_url2)
        subpart_df['SUBPART_NAME'] = row['SUBPART_NAME']
        ghgrp = pd.concat([ghgrp,subpart_df])
    request_url1, request_url2 = '', ''

#Combine equivalent columns from different tables into one, delete old columns
ghgrp2 = ghgrp[ghg_cols]
ghgrp2['Amount']=ghgrp2[quantity_cols].fillna(0).sum(axis=1)
ghgrp2['OriginalFlowID']=ghgrp2[name_cols].fillna('').sum(axis=1)
ghgrp2['METHOD']=ghgrp2[method_cols].fillna('').sum(axis=1)

ghgrp2.drop(info_cols, axis=1, inplace=True)
ghgrp2.drop(group_cols, axis=1, inplace=True)
ghgrp2 = ghgrp2[ghgrp2['OriginalFlowID'] != '']

ghgrp3 = pd.DataFrame()
group_list = [ch4_cols,n2o_cols,co2_cols,co2e_cols]
for group in group_list: 
    for i in range(0,len(group)): 
        ghg_cols2 = base_cols + [group[i]] + method_cols
        temp_df = ghgrp[ghg_cols2]
        temp_df = temp_df[pd.notnull(temp_df[group[i]])]
        temp_df['OriginalFlowID']=group[i]
        temp_df['METHOD']=temp_df[method_cols].fillna('').sum(axis=1)
        temp_df.drop(method_cols, axis=1, inplace=True)
        temp_df.rename(columns={group[i]:'Amount'}, inplace=True)
        ghgrp3 = pd.concat([ghgrp3,temp_df])
ghgrp = pd.concat([ghgrp2,ghgrp3])

#Download link comes from 'https://www.epa.gov/ghgreporting/ghg-reporting-program-data-sets' -- May need to update before running
excel_subparts_url = 'https://www.epa.gov/sites/production/files/2017-09/e_o_s_cems_bb_cc_ll_rr_full_data_set_8_5_17_final_0.xlsx'
excel_dfs = followURL(excel_subparts_url)

def getColumns(subpart_name):
    excel_base_cols = ['GHGRP ID','Year']
    if subpart_name == 'E': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  E\n(metric tons CO2e)',                     'Rounded N2O Emissions from Adipic Acid Production'                          ]
        excel_method_cols = [                             'Type of abatement technologies',                             'Are N2O emissions estimated for this production unit using an Adminstrator-Approved Alternate Method or the Site Specific Emission Factor',                             'Name of Alternate Method (98.56(k)(1)):','Description of the Alternate Method (98.56(k)(2)):',                             'Method Used for the Performance Test'                            ]
    elif subpart_name == 'O': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  O\n(metric tons CO2e)',                           'Rounded HFC-23 emissions (metric tons, output of equation O-4)',                           'Rounded HFC-23 emissions (metric tons, output of equation O-5)',                           'Annual mass of HFC-23 emitted from equipment leaks in metric tons',                           'Annual mass of HFC-23 emitted from all process vents at the facility (metric tons)',                           'Rounded HFC-23 emissions (from the destruction process/device)'                          ]
        excel_method_cols = [                             'Method for tracking startups, shutdowns, and malfuctions and HFC-23 generation/emissions during these events',                             'If any change was made that affects the HFC-23 destruction efficiency or if any change was made to the method used to record the volume destroyed, methods used to determine destruction efficiency.',                             'If any change was made that affects the HFC-23 destruction efficiency or if any change was made to the method used to record the volume destroyed, methods used to record the mass of HFC-23 destroyed.'                            ]
    elif subpart_name == 'S': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  S\n(metric tons CO2e)']
        excel_method_cols = [                             'Method Used to Determine the Quantity of Lime Product Produced and Sold ',                             'Method Used to Determine the Quantity of Calcined Lime ByProduct/Waste Sold '                            ]
    elif subpart_name == 'BB': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  BB\n(metric tons CO2e)']
        excel_method_cols = [                             'Indicate whether carbon content of the petroleum coke is based on reports from the supplier or through self measurement using applicable ASTM standard method'                            ]
    elif subpart_name == 'CC': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  CC\n(metric tons CO2e)',                           'Annual process CO2 emissions from each manufacturing line'                          ]
        excel_method_cols = [                             'Indicate whether CO2 emissions were calculated using a trona input method, a soda ash output method, a site-specific emission factor method, or CEMS'                            ]
    elif subpart_name == 'LL': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  LL\n(metric tons CO2e)',                           'Annual CO2 emissions that would result from the complete combustion or oxidation of all products '                          ]
        excel_method_cols = []
    elif subpart_name == 'RR':
        excel_quant_cols = ['Annual Mass of Carbon Dioxide Sequestered (metric tons)',                            'Total Mass of Carbon Dioxide Sequestered (metric tons)',                            'Equation RR-6 Injection Flow Meter Summation (Metric Tons)',                            'Equation RR-10 Surface Leakage Summation (Metric Tons)',                            'Mass of CO2 emitted from equipment leaks and vented emissions of CO2 from equipment located on the surface between theflow meter used to measure injection quantity and the injection wellhead (metric tons)',                            'Mass of CO2 emitted annually from equipment leaks and vented emissions of CO2 from equipment located on the surface between the production wellhead and the flow meter used to measure production quantity (metric tons)',                            'The entrained CO2 in produced oil or other fluid divided by the CO2 separated through all separators in the reporting year',                            'Injection Flow Meter:  Mass of CO2 Injected (Metric tons)',                            'Leakage Pathway:  Mass of CO2 Emitted  (Metric tons)'                           ]
        excel_method_cols = [                             'Equation used to calculate the Mass of Carbond Dioxide Sequestered',                             'Source(s) of CO2 received',                             'CO2 Received: Unit Name',                             'CO2 Received:Description',                             'CO2 Received Unit:  Flow Meter or Container',                             'CO2 Received Unit:  Mass or Volumetric Basis',                             'Injection Flow Meter:  Name or ID',                             'Injection Flow Meter:  Description',                             'Injection Flow meter:  Mass or Volumetric Basis',                             'Injection Flow meter:  Location',                             'Separator Flow Meter:  Description',                             'Separator Flow meter:  Mass or Volumetric Basis',                             'Leakage Pathway:  Name or ID',                             ]
    return excel_base_cols, excel_quant_cols, excel_method_cols

ghgrp4 = pd.DataFrame()
excel_list = ['E','O','S','BB','CC','LL','RR']
excel_keys = list(excel_dfs.keys())[1:]
for i in range(0,len(excel_keys)):
    excel_base_cols, excel_quant_cols, excel_method_cols = getColumns(excel_list[i])
    temp_cols = excel_base_cols + excel_quant_cols + excel_method_cols
    temp_df=excel_dfs[excel_keys[i]][temp_cols]
#Ignore method info for now
#    temp_df['METHOD']=temp_df[excel_method_cols].fillna('').sum(axis=1)
    for col in excel_quant_cols:
        col_df = temp_df.dropna(subset=[col])
        col_df = col_df[col_df['Year']==int(report_year)]
        col_df['SUBPART_NAME'] = excel_list[i]
        col_df['OriginalFlowID'] = col
        col_df['Amount'] = col_df[col]
        col_df.drop(excel_method_cols, axis=1, inplace=True)
        col_df.drop(excel_quant_cols, axis=1, inplace=True)
        ghgrp4 = pd.concat([ghgrp4,col_df])
ghgrp4 = ghgrp4.rename(columns={'GHGRP ID':'FACILITY_ID'})
ghgrp4.drop('Year', axis=1, inplace=True)
ghgrp = pd.concat([ghgrp,ghgrp4]).reset_index(drop=True)

#Obtain State and NAICS from facilities table using facilityID
#Read facilities file from pickle if it exists
fac_path = 'data/facilities.pkl'
if os.path.exists(fac_path): facilities = pd.read_pickle(fac_path)
else:
    #Create facilities table from scratch. RESTful data service API can only return 10,000 row at time.
    #CSV is used because JSON output includes inordinate amounts of HTML.
    #Check if fac_count variable is defined. If not, count facilities and start from row 0, otherwise pick up where left off after previous 404 error.
    try: fac_count
    except NameError: 
        fac_cols = ['PUB_DIM_FACILITY.FACILITY_ID','PUB_DIM_FACILITY.STATE','PUB_DIM_FACILITY.NAICS_CODE']
        row_start = 0
        facilities = pd.DataFrame()
        #Obtain number of rows in EF facilities table, returned as XML
        fcount_url = enviro_url + 'PUB_DIM_FACILITY/COUNT'
        fcount_request = requests.get(fcount_url)
        fcount_xml = minidom.parseString(fcount_request.text)#TODO: Exception handling
        fac_count = fcount_xml.getElementsByTagName('RequestRecordCount')
        fac_count = int(fac_count[0].firstChild.nodeValue)
    #Generate URL for each 10,000 row grouping and add to dataframe
    while row_start <= fac_count:
        row_end = row_start + 9999
        fac_url = enviro_url + 'PUB_DIM_FACILITY/ROWS/' + str(row_start) + ':' + str(row_end) + '/CSV'
        #Try URL, and exit if it fails 3 times
        facilities_temp = followURL(fac_url)[fac_cols]
        facilities = pd.concat([facilities,facilities_temp])
        row_start += 10000
    #Remove duplicates and remove table name from column names
    facilities.drop_duplicates(inplace=True)
    facilities.rename(columns={'PUB_DIM_FACILITY.FACILITY_ID':'FACILITY_ID'}, inplace=True)
    facilities.rename(columns={'PUB_DIM_FACILITY.STATE':'State'}, inplace=True)
    facilities.rename(columns={'PUB_DIM_FACILITY.NAICS_CODE':'NAICS'}, inplace=True)
    #Save as pickle for future use
    facilities.to_pickle('data/facilities.pkl')

facilities.drop_duplicates(inplace=True)
facilities.rename(columns={'STATE':'State'}, inplace=True)
ghgrp = ghgrp.merge(facilities,on='FACILITY_ID')

reliabilitytable = pd.read_csv('data/DQ_Reliability_Scores_Table3-3fromERGreport.csv', usecols=['Source','Code','DQI Reliability Score'])
ghgrp_reliabilitytable = reliabilitytable[reliabilitytable['Source']=='GHGRPa']
ghgrp_reliabilitytable.drop('Source', axis=1, inplace=True)
ghgrp = pd.merge(ghgrp,ghgrp_reliabilitytable, left_on='METHOD', right_on='Code', how='left')

#Fill NAs with 5 for DQI reliability score
ghgrp.replace('',np.nan)
ghgrp['DQI Reliability Score'] = ghgrp['DQI Reliability Score'].fillna(value=5)
ghgrp.drop('Code', axis=1, inplace=True)
ghgrp['Context'] = 'air'
ghgrp['Amount'] = 1000*ghgrp['Amount']
ghgrp.drop('METHOD', axis=1, inplace=True)
ghgrp.rename(columns={'FACILITY_ID':'FacilityID'}, inplace=True)
ghgrp.rename(columns={'DQI Reliability Score':'ReliabilityScore'}, inplace=True)
ghgrp.rename(columns={'NAICS_CODE':'NAICS'}, inplace=True)

#Standardize output
reflist = pd.read_csv('data/Standarized_Output_Format_EPA _Data_Sources.csv')
reflist = reflist[reflist['required?']==1]
refnames = list(reflist['Name'])
ghgrp = ghgrp[refnames]

#Output to CSV
outputdir='data/'
#ghgrp.to_csv(outputdir + 'ghgrp.csv', index=False)



