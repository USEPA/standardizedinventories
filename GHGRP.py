#GHGRP import and processing

import pandas as pd
import numpy as np
import requests
import urllib
from xml.dom import minidom
import os

#Set reporting year to be used in API requests
report_year = '2015'

#Define URLs to obtain data using EnviroFacts RESTful data service API
enviro_url = 'https://iaspub.epa.gov/enviro/efservice/'
subparts_url = enviro_url + 'PUB_DIM_SUBPART/JSON'
ghgs_url = enviro_url + 'PUB_DIM_GHG/JSON'
#Download link comes from 'https://www.epa.gov/ghgreporting/ghg-reporting-program-data-sets' -- May need to update before running
excel_subparts_url = 'https://www.epa.gov/sites/production/files/2017-09/e_o_s_cems_bb_cc_ll_rr_full_data_set_8_5_17_final_0.xlsx'

#Column groupings handled based on table structure, which varies by subpart
name_cols = ['GAS_NAME','GHG_GAS_NAME','GHG_NAME','FGHG_GROUP_NAME''PROCESS_NAME','PROCESS_TYPE','OTHER_GAS_GHG_GROUP','OTHER_GHG_NAME','OTHER_GREENHOUSE_GAS_NAME']
quantity_cols = ['ANN_FGHG_EMISSIONS_BY_PROCTYPE','GHG_QUANTITY','PRO_VENT_EM_BASED_ON_MISS_DATA','PRO_VENT_MISSING_DATA_ESTIMATE','EQUIPEAK_MISSING_DATA_ESTIMATE','EQUIP_LEAK_EM_BASED_MISS_DATA']
ch4_cols = ['T4CH4COMBUSTIONEMISSIONS','TIER1_CH4_COMBUSTION_EMISSIONS','TIER2_CH4_COMBUSTION_EMISSIONS','TIER3_CH4_COMBUSTION_EMISSIONS','TIER_1_CH4_EMISSIONS','TIER_2_CH4_EMISSIONS','TIER_3_CH4_EMISSIONS']
n2o_cols = ['T4N2OCOMBUSTIONEMISSIONS','TIER1_N2O_COMBUSTION_EMISSIONS','TIER2_N2O_COMBUSTION_EMISSIONS','TIER3_N2O_COMBUSTION_EMISSIONS','TIER_1_N2O_EMISSIONS','TIER_2_N2O_EMISSIONS','TIER_3_N2O_EMISSIONS','TOTAL_ANN_N2O_CHEM_VAP_DEP_EMI','TOT_ANN_N2O_OTHR_ELE_MANU_PROC']
co2_cols = ['CO2_QUANTITY','TIER1_CO2_COMBUSTION_EMISSIONS','TIER2_CO2_COMBUSTION_EMISSIONS','TIER3_CO2_COMBUSTION_EMISSIONS','TIER_1_CO2_EMISSIONS','TIER_2_CO2_EMISSIONS','TIER_3_CO2_EMISSIONS']
co2e_cols = ['PART_75_CH4_EMISSIONS_CO2E','N2O_EMISSIONS_CO2E','CH4_EMISSIONS_CO2E','PART_75_N2O_EMISSIONS_CO2E','EQUIPMENT_LEAK_CO2E','PROCESS_VENT_CO2E']
method_cols = ['AVERAGE_EF_APPROACH','EF_METHOD_USED','EMISSIONS_METHOD','EQUIP_LEAK_MISSING_DATAMETHOD','TIER_1_METHOD_EQUATION','TIER_2_METHOD_EQUATION','TIER_3_METHOD_EQUATION','PRO_VENT_MISSING_DATA_METHOD''ECF_METHOD_USED']
base_cols =  ['SUBPART_NAME'] + ['FACILITY_ID']# + ['GHG_QUANTITY_UNIT_OF_MEASURE']
info_cols = name_cols + quantity_cols + method_cols
group_cols =  ch4_cols + n2o_cols + co2_cols + co2e_cols
ghg_cols = base_cols + info_cols + group_cols
ghg_tables = ['_SUBPART_LEVEL_INFORMATION','_FUEL_LEVEL_INFORMATION','_FOSSIL_FUEL_INFORMATION','_SUBPART_GHG_INFO','_SUBPART_LEVEL_INFO','MV_EF_I_ANN_FGHG_PVMEMSLCD','MV_EF_I_FAB_N2O_EMISSIONS','EF_L_PROCESS_EF_ECF']

#Input subpart_name from table of subparts, return name(s) of table to query for that subpart. 
#Returns false for subparts covered in Excel file.
def getSubpartTable(subpart_name):
    subpart_table2 = ''
    if subpart_name in ('F','G','H','K','N','Q','R','S','T','U','V','X','Y','Z','DD','FF','EE','GG','HH','II','MM','NN','PP','SS'): 
        subpart_table1 = subpart_name + '_SUBPART_LEVEL_INFORMATION'
    elif subpart_name in ('C','D'): subpart_table1 = subpart_name + '_FUEL_LEVEL_INFORMATION'
    elif subpart_name == 'AA': subpart_table1 = subpart_name + '_FOSSIL_FUEL_INFORMATION'
    elif subpart_name == 'TT': subpart_table1 = subpart_name + '_SUBPART_GHG_INFO'
    elif subpart_name == 'P': subpart_table1 = subpart_name + '_SUBPART_LEVEL_INFO'
    elif subpart_name == 'L': subpart_table1 = 'EF_L_PROCESS_EF_ECF'
    elif subpart_name == 'W': subpart_table1 = 'EF_W_EMISSIONS_SOURCE_GHG'
    elif subpart_name == 'I': 
        subpart_table1 = 'MV_EF_I_ANN_FGHG_PVMEMSLCD'
        subpart_table2 = 'MV_EF_I_FAB_N2O_EMISSIONS'
    else: return False, ''
    return subpart_table1, subpart_table2

#Input a specific table name to generate the query URL to submit
def generateURL(table,report_year='',row_start=0,row_end=9999,output_ext='JSON'):
    request_url = enviro_url + table
    if report_year != '': request_url += '/REPORTING_YEAR/=/' + report_year
    if row_start != '': request_url += '/ROWS/' + str(row_start) + ':' + str(row_end)
    request_url += '/'+output_ext
    return request_url

#Try a URL 3 times before giving up
def followURL(input_url,filepath=''):
    for i in range(0,3):
        try: 
            if input_url[-3:].lower() == 'csv': output_data = pd.read_csv(input_url)
            elif input_url[-4:].lower() == 'json': output_data = pd.read_json(input_url)
            elif 'xls' in input_url[-4:]:
                urllib.request.urlretrieve(input_url,filepath)#Downloads file before reading into Python
                return#output_data = pd.read_excel(filepath, sheet_name=None)
            break
        except ValueError: pass
        except: 
            if i == 2: raise
    return output_data

#Input specific table name, returns number of rows from API as XML then converts to integer
def getRowCount(table):
    count_url = enviro_url + table + '/COUNT'
    count_request = requests.get(count_url)
    count_xml = minidom.parseString(count_request.text)
    table_count = count_xml.getElementsByTagName('RequestRecordCount')
    table_count = int(table_count[0].firstChild.nodeValue)
    return table_count

#Download in chunks of 10,000 (API limit)
def downloadChunks(table,table_count,row_start=0,report_year='',output_ext='JSON'):
    #Generate URL for each 10,000 row grouping and add to dataframe
    output_table = pd.DataFrame()
    while row_start <= table_count:
        row_end = row_start + 9999
        table_url = generateURL(table=table,report_year=report_year,row_start=row_start,row_end=row_end,output_ext=output_ext)
        table_temp = followURL(table_url)
        output_table = pd.concat([output_table,table_temp])
        row_start += 10000
    output_table.drop_duplicates(inplace=True)
    return output_table

#Input subpart_name from table of subparts, return column names to use in Excel file 
def getColumns(subpart_name):
    excel_base_cols = ['GHGRP ID','Year']
    if subpart_name == 'E': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  E\n(metric tons CO2e)','Rounded N2O Emissions from Adipic Acid Production']
        excel_method_cols = ['Type of abatement technologies','Are N2O emissions estimated for this production unit using an Adminstrator-Approved Alternate Method or the Site Specific Emission Factor','Name of Alternate Method (98.56(k)(1)):','Description of the Alternate Method (98.56(k)(2)):','Method Used for the Performance Test']
    elif subpart_name == 'O': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  O\n(metric tons CO2e)','Rounded HFC-23 emissions (metric tons, output of equation O-4)','Rounded HFC-23 emissions (metric tons, output of equation O-5)','Annual mass of HFC-23 emitted from equipment leaks in metric tons','Annual mass of HFC-23 emitted from all process vents at the facility (metric tons)','Rounded HFC-23 emissions (from the destruction process/device)']
        excel_method_cols = ['Method for tracking startups, shutdowns, and malfuctions and HFC-23 generation/emissions during these events','If any change was made that affects the HFC-23 destruction efficiency or if any change was made to the method used to record the volume destroyed, methods used to determine destruction efficiency.','If any change was made that affects the HFC-23 destruction efficiency or if any change was made to the method used to record the volume destroyed, methods used to record the mass of HFC-23 destroyed.']
    elif subpart_name == 'S': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  S\n(metric tons CO2e)']
        excel_method_cols = ['Method Used to Determine the Quantity of Lime Product Produced and Sold ','Method Used to Determine the Quantity of Calcined Lime ByProduct/Waste Sold ']
    elif subpart_name == 'BB': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  BB\n(metric tons CO2e)']
        excel_method_cols = ['Indicate whether carbon content of the petroleum coke is based on reports from the supplier or through self measurement using applicable ASTM standard method']
    elif subpart_name == 'CC': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  CC\n(metric tons CO2e)','Annual process CO2 emissions from each manufacturing line']
        excel_method_cols = ['Indicate whether CO2 emissions were calculated using a trona input method, a soda ash output method, a site-specific emission factor method, or CEMS']
    elif subpart_name == 'LL': 
        excel_quant_cols = ['Total Reported Emissions Under Subpart  LL\n(metric tons CO2e)','Annual CO2 emissions that would result from the complete combustion or oxidation of all products ']
        excel_method_cols = []
    elif subpart_name == 'RR':
        excel_quant_cols = ['Annual Mass of Carbon Dioxide Sequestered (metric tons)','Total Mass of Carbon Dioxide Sequestered (metric tons)','Equation RR-6 Injection Flow Meter Summation (Metric Tons)','Equation RR-10 Surface Leakage Summation (Metric Tons)','Mass of CO2 emitted from equipment leaks and vented emissions of CO2 from equipment located on the surface between theflow meter used to measure injection quantity and the injection wellhead (metric tons)','Mass of CO2 emitted annually from equipment leaks and vented emissions of CO2 from equipment located on the surface between the production wellhead and the flow meter used to measure production quantity (metric tons)','The entrained CO2 in produced oil or other fluid divided by the CO2 separated through all separators in the reporting year','Injection Flow Meter:  Mass of CO2 Injected (Metric tons)','Leakage Pathway:  Mass of CO2 Emitted  (Metric tons)']
        excel_method_cols = ['Equation used to calculate the Mass of Carbond Dioxide Sequestered','Source(s) of CO2 received','CO2 Received: Unit Name','CO2 Received:Description','CO2 Received Unit:  Flow Meter or Container','CO2 Received Unit:  Mass or Volumetric Basis','Injection Flow Meter:  Name or ID','Injection Flow Meter:  Description','Injection Flow meter:  Mass or Volumetric Basis','Injection Flow meter:  Location','Separator Flow Meter:  Description','Separator Flow meter:  Mass or Volumetric Basis','Leakage Pathway:  Name or ID',]
    return excel_base_cols, excel_quant_cols, excel_method_cols

#Read in tables as CSV if available, otherwise create tables from JSON output of EF RESTful data service API
required_tables = ['facilities','subparts','ghgs','excel_subparts']
for table in required_tables:
    if table == 'excel_subparts':
        file_ext = 'xlsx'
        read_type = 'pd.ExcelFile'
    else:
        file_ext = 'csv'
        read_type = 'pd.read_'+file_ext
    filepath = 'data/ghgrp/'+table+'.'+file_ext
    write_type = table +'.to_'+file_ext+'("'+filepath+'",index=False)'
    if os.path.exists(filepath): 
        try: exec(table +'='+ read_type +'("'+filepath+'")')
        except UnicodeDecodeError: exec(table +'='+ read_type +'("'+filepath+'",encoding="latin1")')
        write_type=''
    elif table=='facilities':
        #Create facilities table from scratch. RESTful data service API can only return 10,000 row at time.
        #CSV is used because JSON output includes inordinate amounts of HTML.
        #Check if fac_count variable is defined. If not, count facilities and start from row 0, otherwise pick up where left off after previous 404 error.
        try: fac_table
        except NameError: 
            fac_table = 'PUB_DIM_FACILITY'
            fac_cols = ['PUB_DIM_FACILITY.FACILITY_ID','PUB_DIM_FACILITY.STATE','PUB_DIM_FACILITY.NAICS_CODE']
            fac_count = getRowCount(fac_table)
        facilities = downloadChunks(table=fac_table,table_count=fac_count,output_ext='CSV')[fac_cols]
        #Remove duplicates and remove table name from column names
        facilities.rename(columns={'PUB_DIM_FACILITY.FACILITY_ID':'FACILITY_ID'}, inplace=True)
        facilities.rename(columns={'PUB_DIM_FACILITY.STATE':'State'}, inplace=True)
        facilities.rename(columns={'PUB_DIM_FACILITY.NAICS_CODE':'NAICS'}, inplace=True)
        facilities['NAICS'] = facilities['NAICS'].fillna(0).astype(int).astype(str)
        facilities['NAICS'][facilities['NAICS']=='0']=''
    else:
        exec(table +'= followURL('+table+'_url, filepath="'+filepath+'")')
        if table == 'excel_subparts': exec(table +'= pd.ExcelFile("'+filepath+'")')
    if table == 'excel_subparts':
        exec(table +'={sheet: '+table+'.parse(sheet) for sheet in '+table+'.sheet_names}')
        exec(table +'.pop("READ ME", None)')
        write_type = ''
    exec(write_type)

#Clean up misencoded subscripts
for table in [subparts,ghgs]:
    for column in table.select_dtypes([np.object]): 
        table[column] = table[column].str.replace('%3Csub%3E','').str.replace('%3C/sub%3E','')

ghgrp0 = pd.DataFrame(columns = ghg_cols)
used_tables=[]
for index, row in subparts.iterrows():
    #generate a URL for the tables to try
    subpart = row['SUBPART_NAME']
    subpart_table1,subpart_table2 = getSubpartTable(subpart)
    if subpart_table1 == False: continue
    filepath='./data/ghgrp/tables/2015_'+subpart_table1
    used_tables+=[subpart_table1]
    subpart_df=pd.read_csv(filepath)
#    subpart_count=getRowCount(subpart_table1)
#    subpart_df = downloadChunks(table=subpart_table1,table_count=subpart_count,report_year=report_year)
    subpart_df['SUBPART_NAME']=subpart
    for col in subpart_df: exec("subpart_df = subpart_df.rename(columns={'"+col+"':'"+col[len(subpart_table1)+1:]+"'})")
    ghgrp0 = pd.concat([ghgrp0,subpart_df])
    if subpart_table2!='':
        filepath='./data/ghgrp/tables/2015_'+subpart_table2
        used_tables+=[subpart_table2]
        subpart_df=pd.read_csv(filepath)
#        subpart_count=getRowCount(subpart_table2)
#        subpart_df = downloadChunks(table=subpart_table2,table_count=subpart_count,report_year=report_year)
        subpart_df['SUBPART_NAME']=subpart
        for col in subpart_df: exec("subpart_df = subpart_df.rename(columns={'"+col+"':'"+col[len(subpart_table2)+1:]+"'})")
        ghgrp0 = pd.concat([ghgrp0,subpart_df])

#Fixes issue with SUBPART_NAME heading and data are in separate columns
ghgrp0.drop('SUBPART_NAME', axis=1, inplace=True)
ghgrp0 = ghgrp0.rename(columns={'':'SUBPART_NAME'})

#Combine equivalent columns from different tables into one, delete old columns
ghgrp1 = ghgrp0[ghg_cols]
ghgrp1['Amount']=ghgrp1[quantity_cols].fillna(0).sum(axis=1)
ghgrp1['Flow Description']=ghgrp1[name_cols].fillna('').sum(axis=1)
ghgrp1['METHOD']=ghgrp1[method_cols].fillna('').sum(axis=1)

ghgrp1.drop(info_cols, axis=1, inplace=True)
ghgrp1.drop(group_cols, axis=1, inplace=True)
ghgrp1 = ghgrp1[ghgrp1['Flow Description'] != '']

ghgrp2 = pd.DataFrame()
group_list = [ch4_cols,n2o_cols,co2_cols,co2e_cols]
for group in group_list: 
    for i in range(0,len(group)): 
        ghg_cols2 = base_cols + [group[i]] + method_cols
        temp_df = ghgrp0[ghg_cols2]
        temp_df = temp_df[pd.notnull(temp_df[group[i]])]
        temp_df['Flow Description']=group[i]
        temp_df['METHOD']=temp_df[method_cols].fillna('').sum(axis=1)
        temp_df.drop(method_cols, axis=1, inplace=True)
        temp_df.rename(columns={group[i]:'Amount'}, inplace=True)
        ghgrp2 = pd.concat([ghgrp2,temp_df])

ghgrp3 = pd.DataFrame()
excel_dict = {'Adipic Acid':'E', 'HCFC-22 Prod. HFC-23 Dest.':'O', 'Lime':'S', 'Silicon Carbide':'BB', 'Soda Ash':'CC', 'CoalBased Liquid Fuel Suppliers':'LL', 'Geologic Sequestration of CO2':'RR'}
excel_keys = list(excel_subparts.keys())

for key in excel_keys:
    excel_base_cols, excel_quant_cols, excel_method_cols = getColumns(excel_dict[key])
    temp_cols = excel_base_cols + excel_quant_cols + excel_method_cols
    temp_df=excel_subparts[key][temp_cols]#.to_frame()
    temp_df['METHOD']=temp_df[excel_method_cols].fillna('').sum(axis=1)
    for col in excel_quant_cols:
        col_df = temp_df.dropna(subset=[col])
        col_df = col_df[col_df['Year']==int(report_year)]
        col_df['SUBPART_NAME'] = excel_dict[key]
        col_df['Flow Description'] = col
        col_df['Amount'] = col_df[col]
        col_df.drop(excel_method_cols, axis=1, inplace=True)
        col_df.drop(excel_quant_cols, axis=1, inplace=True)
        ghgrp3 = pd.concat([ghgrp3,col_df])
ghgrp3 = ghgrp3.rename(columns={'GHGRP ID':'FACILITY_ID'})
ghgrp3.drop('Year', axis=1, inplace=True)

reliabilitytable = pd.read_csv('data/DQ_Reliability_Scores_Table3-3fromERGreport.csv', usecols=['Source','Code','DQI Reliability Score'])
ghgrp_reliabilitytable = reliabilitytable[reliabilitytable['Source']=='GHGRPa']
ghgrp_reliabilitytable.drop('Source', axis=1, inplace=True)

#Map flow descriptions to standard gas names from GHGRP
ghg_mapping = pd.read_csv('data/ghgrp/ghg_mapping.csv', usecols=['Flow Description','OriginalFlowID'])


#Merge tables
ghgrp = pd.concat([ghgrp1,ghgrp2,ghgrp3]).reset_index(drop=True)
ghgrp = ghgrp.merge(facilities,on='FACILITY_ID',how='left')
ghgrp = pd.merge(ghgrp,ghgrp_reliabilitytable,left_on='METHOD',right_on='Code',how='left')
ghgrp = pd.merge(ghgrp,ghg_mapping,on='Flow Description',how='left')

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
refnames = list(reflist['Name'])+['SUBPART_NAME']
ghgrp = ghgrp[refnames]

