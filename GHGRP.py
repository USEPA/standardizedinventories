#GHGRP import and processing

import pandas as pd

subpart_list = 'C:/Users/Mbergman/Downloads/LCI-Primer-master/LCI-Primer-master/data/ghg_subpart_codes.csv'
subparts = pd.read_csv(subpart_list, usecols=['subpart_code','subpart_name'])

#Create unique columns to populate with query outputs
ghg_cols = ['FACILITY_ID','GAS_NAME','GHG_GAS_NAME','GHG_NAME','GHG_QUANTITY','GHG_QUANTITY_UNIT_OF_MEASURE','REPORTING_YEAR',\
            'TIER_1_METHOD_EQUATION','TIER1_CO2_COMBUSTION_EMISSIONS','TIER1_CH4_COMBUSTION_EMISSIONS',\
            'TIER1_N2O_COMBUSTION_EMISSIONS','TIER_2_METHOD_EQUATION','TIER2_CO2_COMBUSTION_EMISSIONS',\
            'TIER2_CH4_COMBUSTION_EMISSIONS','TIER2_N2O_COMBUSTION_EMISSIONS','T4CH4COMBUSTIONEMISSIONS',\
            'T4N2OCOMBUSTIONEMISSIONS'\
            ]
ghgrp = pd.DataFrame(columns = ghg_cols)

#Making lists of categories for which appropriate tables to query are still being determined
fuel_present_cat = []
fuel_missing_cat = []
subpart_present_cat = []
subpart_missing_cat = []

#Define EnviroFacts URL and query criteria
enviro_url = 'https://iaspub.epa.gov/enviro/efservice/'
report_year = ''
format_out = 'json'
row_start = '0'
row_end = '5'

#For each row of the subparts table
for index, row in spinfo.iterrows():
    #generate a URL for the tables to try
    subpart_url = enviro_url + row['subpart_code'] + '_SUBPART_LEVEL_INFORMATION'
    fuel_url = enviro_url + row['subpart_code'] + '_FUEL_LEVEL_INFORMATION'
    if report_year != '': 
        subpart_url += '/REPORTING_YEAR/=/' + report_year
        fuel_url += '/REPORTING_YEAR/=/' + report_year
    if row_start != '': 
        subpart_url += '/ROWS/' + row_start + ':' + row_end
        fuel_url += '/ROWS/' + row_start + ':' + row_end
    subpart_url += '/' + format_out
    fuel_url += '/' + format_out

    #Try to query fuel_level_information table, then subpart_level_information
    #This could probably be done better once appropriate tables are known for each subpart
    try: 
        if format_out == 'json': subpart_df = pd.read_json(fuel_url)
        ghgrp = pd.concat([ghgrp,subpart_df])
        fuel_present_cat += [[row['subpart_code'],row['subpart_name']]]
    except: 
        fuel_missing_cat += [[row['subpart_code'],row['subpart_name']]]
        try: 
            if format_out == 'json': subpart_df = pd.read_json(subpart_url)
            ghgrp = pd.concat([ghgrp,subpart_df])
            subpart_present_cat += [[row['subpart_code'],row['subpart_name']]]
        except: subpart_missing_cat += [[row['subpart_code'],row['subpart_name']]]

#Simply printing missing categories while appropriate tables to query are determined
print('fuel_present:\n',fuel_present,'\nsubpart_present:\n',subpart_present,\
      '\nfuel_missing:\n',fuel_missing,'\nsubpart_missing:\n',subpart_missing,'\n')
                        
#Combine equivalent columns from different tables into one, delete old columns
ghgrp['EMISSION_NAME'] = ghgrp[['GAS_NAME','GHG_GAS_NAME','GHG_NAME']].fillna('').sum(axis=1)
ghgrp = ghgrp.drop(['GAS_NAME','GHG_GAS_NAME','GHG_NAME'], axis=1)



#TODO: Merge reliability table using equations referenced here in 'Code' column
reliabilitytable = pd.read_csv('data/DQ_Reliability_Scores_Table3-3fromERGreport.csv', usecols=['Source','Code','DQI Reliability Score'])
ghgrp_reliabilitytable = reliabilitytable[reliabilitytable['Source']=='GHGRPa']
ghgrp_reliabilitytable.drop('Source', axis=1, inplace=True)
ghgrp_reliabilitytable



