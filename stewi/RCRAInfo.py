# List of tables:
        ### BR_GM_WASTE_CODE
        ### BR_LU_DENSITY_UOM
        ### BR_LU_FORM_CODE
        ### BR_LU_MANAGEMENT_METHOD
        ### BR_LU_SOURCE_CODE
        ### BR_LU_UOM
        ### BR_LU_WASTE_MINIMIZATION
        ### BR_REPORTING
        ### BR_REPORTING_2001
        ### BR_WR_WASTE_CODE
        ### CE_CITATION
        ### CE_LU_CITATION
        ### CE_REPORTING
        ### CA_AREA
        ### CA_AREA_EVENT
        ### CA_AREA_UNIT
        ### CA_AUTHORITY
        ### CA_AUTHORITY_CITATION
        ### CA_EVENT
        ### CA_EVENT_AUTHORITY
        ### CA_LU_AUTHORITY
        ### CA_LU_EVENT_CODE
        ### CA_LU_STATUTORY_CITATION
        ### EM_REPORTING
        ### FA_COST_ESTIMATE
        ### FA_COST_MECHANISM_DETAIL
        ### FA_LU_MECHANISM_TYPE
        ### FA_MECHANISM
        ### FA_MECHANISM_DETAIL
        ### GS_GIS
        ### GS_GIS_LAT_LONG
        ### GS_LU_AREA_SOURCE
        ### GS_LU_COORDINATE
        ### GS_LU_GEOGRAPHIC_REFERENCE
        ### GS_LU_GEOMETRIC
        ### GS_LU_HORIZONTAL_COLLECTION
        ### GS_LU_HORIZONTAL_REFERENCE
        ### GS_LU_VERIFICATION
        ### HD_BASIC
        ### HD_CERTIFICATION
        ### HD_EPISODIC_EVENT
        ### HD_EPISODIC_WASTE
        ### HD_EPISODIC_WASTE_CODE
        ### HD_HANDLER
        ### HD_HSM_ACTIVITY
        ### HD_HSM_BASIC
        ### HD_HSM_RECYCLER
        ### HD_HSM_WASTE_CODE
        ### HD_LQG_CLOSURE
        ### HD_LQG_CONSOLIDATION
        ### HD_LU_COUNTRY
        ### HD_LU_COUNTY
        ### HD_LU_EPISODIC_EVENT
        ### HD_LU_FOREIGN_STATE
        ### HD_LU_GENERATOR_STATUS
        ### HD_LU_HSM_FACILITY_CODE
        ### HD_LU_NAICS
        ### HD_LU_OTHER_PERMIT
        ### HD_LU_RELATIONSHIP
        ### HD_LU_STATE
        ### HD_LU_STATE_ACTIVITY
        ### HD_LU_STATE_DISTRICT
        ### HD_LU_UNIVERSAL_WASTE
        ### HD_LU_WASTE_CODE
        ### HD_NAICS
        ### HD_OTHER_ID
        ### HD_OTHER_PERMIT
        ### HD_OWNER_OPERATOR
        ### HD_PART_A
        ### HD_REPORTING
        ### HD_STATE_ACTIVITY
        ### HD_UNIVERSAL_WASTE
        ### HD_WASTE_CODE
        ### PM_EVENT
        ### PM_EVENT_UNIT_DETAIL
        ### PM_LU_LEGAL_OPERATING_STATUS
        ### PM_LU_PERMIT_EVENT_CODE
        ### PM_LU_PROCESS_CODE
        ### PM_LU_UNIT_OF_MEASURE
        ### PM_SERIES
        ### PM_UNIT
        ### PM_UNIT_DETAIL
        ### PM_UNIT_DETAIL_WASTE

import pandas as pd
import stewi.globals as globals
from stewi.globals import write_metadata,unit_convert,validate_inventory,write_validation_result,set_dir,output_dir,data_dir
from stewi.globals import checkforFile
import zipfile
import shutil
import numpy as np
import argparse
from selenium import webdriver
import re
import os
import time, datetime
from stewi.globals import USton_kg

sys.path.insert(0, '/../')
from common import config

def waste_description_cleaner(x):
    if (x == 'from br conversion') or (x =='From 1989 BR data'):
        x = None
    return x


def extracting_files(path_unzip, name):
    with zipfile.ZipFile(path_unzip + name + '.zip') as z:
        z.extractall(path_unzip)
    os.remove(path_unzip + name + '.zip')


def download_zip(url, dir_path, Tables, query):
    regex = re.compile(r'(.+).zip\s?\(\d+.?\d*\s?[a-zA-Z]{2,}\)')
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-notifications')
    options.add_argument('--no-sandbox')
    options.add_argument('--verbose')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument("--log-level=3")
    options.add_argument('--hide-scrollbars')
    prefs = {'download.default_directory' : dir_path,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing_for_trusted_sources_enabled': False,
            'safebrowsing.enabled': False}
    options.add_experimental_option('prefs', prefs)
    browser = webdriver.Chrome(executable_path = os.path.dirname(os.path.realpath(__file__)) + \
                            '/chromedriver.exe', options = options)
    browser.maximize_window()
    browser.set_page_load_timeout(180)
    browser.get(url)
    time.sleep(30)
    Table_of_tables = browser.find_element_by_xpath(query)
    rows = Table_of_tables.find_elements_by_css_selector('tr')[1:] # Excluding header
    # Extracting zip files for Biennial Report Tables
    Links = {}
    for row in rows:
        loop = 'YES'
        while loop == 'YES':
            try:
                Table_name = re.search(regex, row.find_elements_by_css_selector('td')[3].text).group(1)
                Link = row.find_elements_by_css_selector('td')[3].find_elements_by_css_selector('a')[0].get_attribute('href')
                Links.update({Table_name:Link})
                loop = 'NO'
            except AttributeError:
                loop = 'YES'
                now = datetime.datetime.now()
                print('AttributeError occurred with selenium due to not appropriate charging of website.\nHour: {}:{}:{}'.format(now.hour,now.minute,now.second))
    # Download the desired zip
    for name in Tables:
        browser.get(Links[name])
        condition = checkforFile(dir_path + name + '.zip')
        while condition is False:
            #download file
            #get timestamp
            condition = checkforFile(dir_path + name + '.zip')
        time.sleep(5)
        extracting_files(dir_path, name)
    browser.quit()


def organizing_files_by_year(Tables, Path, Years_saved):
    for Table  in Tables:
        # Get file columns widths
        dir_RCRA_by_year = set_dir(Path + 'RCRAInfo_by_year/')
        linewidthsdf = pd.read_csv(data_dir + 'RCRA_FlatFile_LineComponents_2019.csv')
        BRwidths = linewidthsdf['Size'].astype(int)
        BRnames = linewidthsdf['Data Element Name']
        Files = [file for file in os.listdir(Path) if ((file.startswith(Table)) & file.endswith('.txt'))]
        Files.sort()
        for File in Files:
            print('Processing file {}'.format(File))
            df = pd.read_fwf(Path + File, widths = BRwidths, \
                     header = None, names = BRnames)
            df.sort_values(by=['Report Cycle'])
            df = df[df['Report Cycle'].apply(lambda x: str(x).isnumeric())]
            df['Report Cycle'] = df['Report Cycle'].astype(int)
            df = df[~df['Report Cycle'].isin(Years_saved)]
            Years = list(df['Report Cycle'].unique())
            for Year in Years:
                if re.match(r'\d{4}', str(int(Year))):
                    df_year = df[df['Report Cycle'] == Year]
                    Path_directory = dir_RCRA_by_year + 'br_reporting_' + str(int(Year)) + '.txt'
                    condition = True
                    while condition:
                        try:
                            if os.path.exists(Path_directory):
                                with open(Path_directory, 'a') as f:
                                    df_year.to_csv(f, header = False, sep = '\t', index = False)
                            else:
                                df_year.to_csv(Path_directory, sep = '\t', index = False)
                            condition = False
                        except UnicodeEncodeError:
                            for column in df_year:
                                if df_year[column].dtype == object:
                                    df_year[column] = df_year[column].map(lambda x: x.replace(u'\uFFFD', '?') \
                                                    if type(x) == str else x)
                            condition = True
                else:
                    continue


if __name__ == '__main__':

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\n[E]xtract information from RCRAInfo.\n[O]rganize files for each year.\n[C]reate RCRAInfo for StEWI',
                        type = str)

    parser.add_argument('Year',
                        help = 'What RCRA Biennial Report year you want to retrieve or generate for StEWI',
                        nargs= '?',
                        type = str,
                        default = None)

    parser.add_argument('-T', '--Tables', nargs = '+',
                        help = 'What RCRAInfo tables you want.\nCheck:\nhttps://rcrainfopreprod.epa.gov/rcrainfo-help/application/publicHelp/index.htm',
                        required = False)

    args = parser.parse_args()

    #Metadata
    BR_meta = globals.inventory_metadata

    #RCRAInfo url
    config = config()['web_sites']['RCRAInfo']
    RCRAfInfoflatfileURL = config['url']

    RCRAInfopath = set_dir(data_dir + "../../../RCRAInfo/")

    if args.Option == 'E':

        query = config['queries']['Table_of_tables']
        download_zip(RCRAfInfoflatfileURL, RCRAInfopath, args.Tables, query)

    elif args.Option == 'O':

        regex =  re.compile(r'RCRAInfo_(\d{4})')
        PathWithSavingData = output_dir + 'flowbyfacility'
        files = os.listdir(PathWithSavingData)
        RCRAInfo_years_saved = list()
        for file in files:
            if re.match(regex, file):
                RCRAInfo_years_saved.append(int(re.search(regex, file).group(1)))
        organizing_files_by_year(args.Tables, RCRAInfopath, RCRAInfo_years_saved)

    elif args.Option == 'G':

        report_year = args.Year
        RCRAInfoBRtextfile = RCRAInfopath + 'RCRAInfo_by_year/br_reporting_' + report_year + '.txt'

        #Get file columns widths
        linewidthsdf = pd.read_csv(data_dir + 'RCRA_FlatFile_LineComponents_2019.csv')
        BRwidths = linewidthsdf['Size']

        #Metadata
        BR_meta = globals.inventory_metadata

        #Get columns to keep
        RCRAfieldstokeepdf = pd.read_csv(data_dir + 'RCRA_required_fields.txt', header = None)
        RCRAfieldstokeep = list(RCRAfieldstokeepdf[0])

        #Get total row count of the file
        with open(RCRAInfoBRtextfile, 'rb') as rcrafile:
            row_count = sum([1 for row in rcrafile]) - 1

        BR = pd.read_csv(RCRAInfoBRtextfile, header = 0, usecols = RCRAfieldstokeep, sep = '\t',
                        low_memory = False, error_bad_lines = False, encoding = 'ISO-8859-1')
        # Checking the Waste Generation Data Health
        BR = BR[pd.to_numeric(BR['Generation Tons'], errors = 'coerce').notnull()]
        BR['Generation Tons'] = BR['Generation Tons'].astype(float)


        #Pickle as a backup
        # BR.to_pickle('work/BR_'+ report_year + '.pk')
        #Read in to start from a pickle
        # BR = pd.read_pickle('work/BR_'+report_year+'.pk')
        print(len(BR))
        #2001:838497
        #2003:770727
        #2005:697706
        #2007:765764
        #2009:919906
        #2011:1590067
        #2013:1581899
        #2015:2053108
        #2017:1446613

        #Validate correct import - number of states should be 50+ (includes PR and territories)
        states = BR['State'].unique()
        print(len(states))
        #2001: 46
        #2003: 46
        #2005: 46
        #2007: 46
        #2009: 46
        #2011: 56
        #2013: 56
        #2015: 57
        #2017: 45

        #Filtering to remove double counting and non BR waste records
        #Do not double count generation from sources that receive it only
        #Check sum of tons and number of records after each filter step
        #See EPA 2013. Biennial Report Analytical Methodologies: Data Selection
        #Logic and Assumptions used to Analyze the Biennial Report. Office of Resource Conservation and Recovery

        #Drop lines with source code G61
        BR = BR[BR['Source Code'] != 'G61']
        print(len(BR))
        #2001:798905
        #2003:722958
        #2005:650413
        #2007:722383
        #2009:879845
        #2011:1496275
        #2013:1492245
        #2015:1959883
        #2017:1375562

        #Only include wastes that are included in the National Biennial Report
        BR = BR[BR['Generator ID Included in NBR'] == 'Y']
        print(len(BR))
        #2001:734349
        #2003:629802
        #2005:482345
        #2007:598748
        #2009:704233
        #2011:1284796
        #2013:1283457
        #2015:1759711
        #2017:1269987

        BR = BR[BR['Generator Waste Stream Included in NBR'] == 'Y']
        print(len(BR))
        #2001:172539
        #2003:167488
        #2005:152036
        #2007:151729
        #2009:142918
        #2011:209342
        #2013:256978
        #2015:288980
        #2017:202842

        #Remove imported wastes, source codes G63-G75
        ImportSourceCodes = pd.read_csv(data_dir + 'RCRAImportSourceCodes.txt', header=None)
        ImportSourceCodes = ImportSourceCodes[0].tolist()
        SourceCodesPresent = BR['Source Code'].unique().tolist()

        SourceCodestoKeep = []
        for item in SourceCodesPresent:
            if item not in ImportSourceCodes:
                #print(item)
                SourceCodestoKeep.append(item)

        BR = BR[BR['Source Code'].isin(SourceCodestoKeep)]
        print(len(BR))
        #2001:172539
        #2003:167264
        #2005:151638
        #2007:151695
        #2009:142825
        #2011:209306
        #2013:256844
        #2015:286813
        #2017:202513

        #Reassign the NAICS to a string
        BR['NAICS'] = BR['Primary NAICS'].astype('str')
        BR.drop('Primary NAICS', axis=1, inplace=True)

        #Create field for DQI Reliability Score with fixed value from CSV
        #Currently generating a warning
        reliability_table = globals.reliability_table
        rcrainfo_reliability_table = reliability_table[reliability_table['Source']=='RCRAInfo']
        rcrainfo_reliability_table.drop('Source', axis=1, inplace=True)
        BR['ReliabilityScore'] = float(rcrainfo_reliability_table['DQI Reliability Score'])

        #Create a new field to put converted amount in
        BR['Amount_kg'] = 0.0

        #Convert amounts from tons. Note this could be replaced with a conversion utility
        BR['Amount_kg'] = USton_kg*BR['Generation Tons']

        ##Read in waste descriptions
        linewidthsdf = pd.read_csv(data_dir + 'RCRAInfo_LU_WasteCode_LineComponents.csv')
        widths = linewidthsdf['Size']
        names = linewidthsdf['Data Element Name']

        File_lu = [file for file in os.listdir(RCRAInfopath) if 'lu_waste_code' in file.lower()][0]

        os.listdir(RCRAInfopath)
        wastecodesfile = RCRAInfopath + File_lu

        WasteCodesTest = pd.read_fwf(wastecodesfile,widths=widths,header=None,names=names,nrows=10)
        WasteCodes = pd.read_fwf(wastecodesfile,widths=widths,header=None,names=names)
        WasteCodes = WasteCodes[['Waste Code', 'Code Type', 'Waste Code Description']]
        #Remove rows where any fields are na description is missing
        WasteCodes.dropna(inplace=True)

        #Bring in form codes
        #Replace form code with the code name
        form_code_name_file = data_dir + 'RCRA_LU_FORM_CODE.csv'
        form_code_table_cols_needed = ['FORM_CODE','FORM_CODE_NAME']
        form_code_name_df = pd.read_csv(form_code_name_file,header=0,usecols=form_code_table_cols_needed)

        #Merge waste codes with BR records
        BR = pd.merge(BR,WasteCodes,left_on='Waste Code Group',right_on='Waste Code',how='left')
        #Rename code type to make it clear
        BR.rename(columns={'Code Type':'Waste Code Type'},inplace=True)
        #Merge form codes with BR
        BR = pd.merge(BR,form_code_name_df,left_on='Form Code',right_on='FORM_CODE',how='left')
        #Drop duplicates from merge
        BR.drop(columns=['FORM_CODE','Waste Code Group'], inplace=True)

        #Set flow name to Waste Code Description
        BR['FlowName'] = BR['Waste Code Description']
        #BR['FlowNameSource'] = 'Waste Code Description'
        #If a useful Waste Code Description is present, use it


        BR['FlowName'] = BR['FlowName'].apply(waste_description_cleaner)
        #Check unique flow names
        pd.unique(BR['FlowName'])

        #If there is not useful waste code, fill it with the Form Code Name
        #Find the NAs in FlowName and then give that source of Form Code
        BR.loc[BR['FlowName'].isnull(),'FlowNameSource'] = 'Form Code'
        #Now for those source name rows that are blank, tell it its a waste code
        BR.loc[BR['FlowNameSource'].isnull(),'FlowNameSource'] = 'Waste Code'

        #Set FlowIDs to the appropriate code
        BR.loc[BR['FlowName'].isnull(),'FlowID'] = BR['Form Code']
        BR.loc[BR['FlowID'].isnull(),'FlowID'] = BR['Waste Code']

        #Now finally fill names that are blank with the form code name
        BR['FlowName'].fillna(BR['FORM_CODE_NAME'], inplace=True)

        #Drop unneeded fields
        BR.drop('Generation Tons', axis=1, inplace=True)
        BR.drop('Generator ID Included in NBR', axis=1, inplace=True)
        BR.drop('Generator Waste Stream Included in NBR', axis=1, inplace=True)
        BR.drop('Source Code', axis=1, inplace=True)
        BR.drop('Management Method', axis=1, inplace=True)
        BR.drop('Waste Description', axis=1, inplace=True)
        BR.drop('Waste Code Description', axis=1, inplace=True)
        BR.drop('FORM_CODE_NAME', axis=1, inplace=True)

        #Rename cols used by multiple tables
        BR.rename(columns={'Handler ID':'FacilityID'}, inplace=True)
        #rename new name
        BR.rename(columns={'Amount_kg':'FlowAmount'}, inplace=True)

        #Prepare flows file
        flows = BR[['FlowName','FlowID','FlowNameSource']]
        #Drop duplicates
        flows = flows.drop_duplicates()
        flows['Compartment']='Waste'
        flows['Unit']='kg'
        #Sort them by the flow names
        flows.sort_values(by='FlowName',axis=0,inplace=True)
        #Export them
        flows.to_csv(output_dir + 'flow/RCRAInfo_' + report_year + '.csv',index=False)

        #Prepare facilities file
        facilities = BR[['FacilityID', 'Handler Name','Location Street Number',
           'Location Street 1', 'Location Street 2', 'Location City',
           'Location State', 'Location Zip', 'County Name','NAICS']]

        #Drop duplicates
        facilities.drop_duplicates(inplace=True)

        facilities['Location Street Number'] = facilities['Location Street Number'].apply(str)
        facilities['Location Street Number'].fillna('',inplace=True)

        facilities['Address'] = facilities['Location Street Number'] + ' ' + facilities['Location Street 1'] + ' ' + facilities['Location Street 2']
        facilities.drop(columns=['Location Street Number','Location Street 1','Location Street 2'],inplace=True)

        facilities.rename(columns={'Primary NAICS':'NAICS',
                            'Handler Name':'FacilityName',
                            'Location City':'City',
                            'Location State':'State',
                            'Location Zip':'Zip',
                            'County Name':'County'}, inplace=True)
        facilities.to_csv(output_dir + 'facility/RCRAInfo_' + report_year + '.csv',index=False)

        #Prepare flow by facility

        flowbyfacility = BR.groupby(['FacilityID','ReliabilityScore','FlowName'])['FlowAmount'].sum().reset_index()

        ##VALIDATION
        BR_national_total = pd.read_csv(data_dir + 'RCRAInfo_' + report_year + '_NationalTotals.csv', header=0, dtype={"FlowAmount":np.float})
        BR_national_total['FlowAmount_kg'] = 0
        BR_national_total = unit_convert(BR_national_total, 'FlowAmount_kg', 'Unit', 'Tons', 907.18474, 'FlowAmount')
        BR_national_total.drop('FlowAmount',axis=1,inplace=True)
        BR_national_total.drop('Unit',axis=1,inplace=True)
        # Rename cols to match reference format
        BR_national_total.rename(columns={'FlowAmount_kg':'FlowAmount'},inplace=True)

        #Validate total waste generated against national totals
        sum_of_flowbyfacility = flowbyfacility['FlowAmount'].sum()
        sum_of_flowbyfacility_df = pd.DataFrame({'FlowAmount':[sum_of_flowbyfacility],'FlowName':'ALL','Compartment':'waste'})
        validation_df = validate_inventory(sum_of_flowbyfacility_df,BR_national_total,group_by='flow')
        write_validation_result('RCRAInfo', report_year, validation_df)

        #Export to csv
        flowbyfacility.to_csv(output_dir + 'flowbyfacility/RCRAInfo_' + report_year + '.csv',index=False)

        #Record metadata
        try: retrieval_time = os.path.getctime(RCRAInfoBRtextfile)
        except: retrieval_time = time.time()
        BR_meta['SourceAquisitionTime'] = time.ctime(retrieval_time)
        BR_meta['SourceFileName'] = RCRAInfoBRtextfile
        BR_meta['SourceURL'] = RCRAfInfoflatfileURL

        write_metadata('RCRAInfo', report_year, BR_meta)
