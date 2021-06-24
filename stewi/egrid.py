#!/usr/bin/env python
"""
Imports eGRID data and processes to Standardized EPA output format.
Uses the eGRID data files from EPA.
This file requires parameters be passed like:

    Option -y Year 

Options:
    A - Download eGRID data
    B - Process and parse eGRID data and validation against national totals
    C - Download and process data for validation 

Year: 
    2019
    2018
    2016
    2014
"""

import pandas as pd
import numpy as np
import argparse
import os
from stewi.globals import data_dir,write_metadata,\
    unit_convert,log,MMBtu_MJ,MWh_MJ,config,\
    validate_inventory,write_validation_result,USton_kg,lb_kg,\
    compile_source_metadata, remove_line_breaks, paths, storeInventory,\
    read_source_metadata, readInventory, update_validationsets_sources
import requests
import zipfile
import io


_config = config()['databases']['eGRID']

# set filepath
ext_folder = '/eGRID Data Files/'
eGRIDfilepath = paths.local_path + ext_folder
eGRID_data_dir = data_dir + 'eGRID/'

# Import list of fields from egrid that are desired for LCI
def imp_fields(fields_txt, year):
    egrid_req_fields_df = pd.read_csv(fields_txt, header=0)
    egrid_req_fields_df = remove_line_breaks(egrid_req_fields_df,
                                             headers_only=False)
    egrid_req_fields = list(egrid_req_fields_df[year])
    col_dict = egrid_req_fields_df.set_index(year).to_dict()
    return egrid_req_fields, col_dict

def egrid_unit_convert(value,factor):
    new_val = value*factor;
    return new_val;

def download_eGRID(year):
    '''
    Downloads eGRID files from EPA website
    '''
    log.info('downloading eGRID data for ' + year)
    
    ## make http request
    r = []
    download_url = _config[year]['download_url']
    egrid_file_name = _config[year]['file_name']

    try:
        r = requests.Session().get(download_url)
    except requests.exceptions.ConnectionError:
        log.error("URL Connection Error for " + download_url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        log.error('Error in URL request!')
        
    ## extract .xlsx workbook
    if year == '2016' or year == '2014':
        z = zipfile.ZipFile(io.BytesIO(r.content))
        workbook = z.read(egrid_file_name)
    else:
        workbook = r.content
        
    ## save .xlsx workbook to destination directory
    destination = eGRIDfilepath + egrid_file_name
    # if destination folder does not already exist, create it
    if not(os.path.exists(eGRIDfilepath)):
        os.makedirs(eGRIDfilepath)
    with open(destination, 'wb') as output:
        output.write(workbook)
    log.info('%s saved to %s', egrid_file_name, eGRIDfilepath)

def generate_metadata(year, datatype = 'inventory'):
    """
    Gets metadata and writes to .json
    """
    if datatype == 'source':
        source_path = eGRIDfilepath + _config[year]['file_name']
        source_meta = compile_source_metadata(source_path, _config, year)
        write_metadata('eGRID_'+year, source_meta, category=ext_folder,
                       datatype='source')
    else:
        source_meta = read_source_metadata(
            eGRIDfilepath + 'eGRID_'+ year)['tool_meta']
        write_metadata('eGRID_'+year, source_meta, datatype=datatype)    

def extract_eGRID_excel(year, sheetname, index='field'):
    eGRIDfile = eGRIDfilepath + _config[year]['file_name']
    if index != 'field': header = 1
    else: header = 0
    df = pd.read_excel(eGRIDfile, sheet_name=sheetname+year[2:], header=header)
    df = remove_line_breaks(df)
    if index == 'field':
        #drop first row which are column name abbreviations
        df = df.drop([0])
    return df
        
def generate_eGRID_files(year):
    '''
    Parses a locally downloaded eGRID file to generate output files for 'flow',
    'facility', and 'flowbyfacility'

    :param year: str, Year of eGRID dataset  
    '''
    log.info('generating eGRID files for '+ year)
    
    egrid = extract_eGRID_excel(year, 'PLNT')
    #get list of columns not in the required fields and drop them
    required_fields, col_dict = imp_fields(eGRID_data_dir+\
                                                 'eGRID_required_fields.csv',
                                                 year)
    colstodrop = list(set(list(egrid.columns)) - set(required_fields))
    egrid2 = egrid.drop(colstodrop,axis=1)
    egrid2.rename(columns = col_dict['StEWI'], inplace=True)

    #Read in unit sheet to get comment fields related to source of heat,NOx,
    #SO2, and CO2 emission estimates
    unit_egrid = extract_eGRID_excel(year, 'UNT')
    #get list of columns not in the required fields and drop them
    unit_rqd_fields, unit_col_dict = imp_fields(eGRID_data_dir+\
                                                'eGRID_unit_level_required_fields.csv',
                                                year)
    colstodrop = list(set(list(unit_egrid.columns)) - \
                      set(unit_rqd_fields))
    unit_egrid = unit_egrid.drop(colstodrop,axis=1)
    unit_egrid.rename(columns = unit_col_dict['StEWI'], inplace=True)
    
    #Import reliability mapping. Merge one by one.
    rel_scores_heat_SO2_CO2_NOx = pd.read_csv(
        eGRID_data_dir+'eGRID_unit_level_reliability_scores.csv')

    rel_score_dict = {'ReliabilityScore_heat':'Unit unadjusted annual '
                          'heat input source',
                      'ReliabilityScore_NOx':'Unit unadjusted annual '
                          'NOx emissions source',
                      'ReliabilityScore_SO2':'Unit unadjusted annual '
                          'SO2 emissions source',
                      'ReliabilityScore_CO2':'Unit unadjusted annual '
                          'CO2 emissions source'}
    rel_score_cols = list(rel_score_dict.keys())
   
    for k, v in rel_score_dict.items():
        unit_egrid = unit_egrid.merge(rel_scores_heat_SO2_CO2_NOx,
                                        left_on =[v], right_on =['Source'], how = 'left')
        unit_egrid = unit_egrid.rename(columns= {'ReliabilityScore':k})
        del unit_egrid['Source']
    
    #Calculate reliability scores at plant level using flow-weighted average.
    flows_used_for_weighting = ['Unit unadjusted annual heat input (MMBtu)',
                                'Unit unadjusted annual NOx emissions (tons)',
                                'Unit unadjusted annual SO2 emissions (tons)',
                                'Unit unadjusted annual CO2 emissions (tons)']
    #First multiply by flows
    unit_egrid[rel_score_cols] = np.multiply(unit_egrid[rel_score_cols],
                                                     unit_egrid[flows_used_for_weighting])

    #Aggregate the multiplied scores at the plant level
    unit_egrid_rel = unit_egrid.groupby(
        ['FacilityID'])[rel_score_cols].sum().reset_index()
    unit_egrid_flows = unit_egrid.groupby(
        ['FacilityID'])[flows_used_for_weighting].sum().reset_index()
    unit_egrid_final = unit_egrid_rel.merge(unit_egrid_flows,
                                            on = ['FacilityID'],
                                            how = 'inner')

    # To avoid the RuntimeWarning:
    np.seterr(divide='ignore',invalid='ignore')
    unit_egrid_final[rel_score_cols] = np.divide(unit_egrid_final[rel_score_cols],
                                                 unit_egrid_final[flows_used_for_weighting])
    np.seterr(divide='warn',invalid='warn')

    unit_emissions_with_rel_scores = ['Heat','Nitrogen oxides',
                                      'Sulfur dioxide','Carbon dioxide']    
    unit_egrid_final[unit_emissions_with_rel_scores] = unit_egrid_final[rel_score_cols]
    rel_scores_by_facility = pd.melt(unit_egrid_final,
                                     id_vars=['FacilityID'],
                                     value_vars=unit_emissions_with_rel_scores,
                                     var_name='FlowName',
                                     value_name='DataReliability')
    
    ##Create FLOWBYFACILITY output
    flowbyfac_fields = {'FacilityID':'FacilityID',
                        'Plant primary fuel':'Plant primary fuel',
                        'Plant total annual heat input (MMBtu)':'Heat',
                        'Plant annual net generation (MWh)':'Electricity',
                        'Plant annual NOx emissions (tons)':'Nitrogen oxides',
                        'Plant annual SO2 emissions (tons)':'Sulfur dioxide',
                        'Plant annual CO2 emissions (tons)':'Carbon dioxide',
                        'Plant annual CH4 emissions (lbs)':'Methane',
                        'Plant annual N2O emissions (lbs)':'Nitrous oxide',
                        'CHP plant useful thermal output (MMBtu)':'Steam'
                        }
    
    flowbyfac_prelim = egrid2[list(flowbyfac_fields.keys())]
    flowbyfac_prelim = flowbyfac_prelim.rename(columns=flowbyfac_fields)
    nox_so2_co2 = egrid_unit_convert(flowbyfac_prelim[['Nitrogen oxides',
                                                       'Sulfur dioxide',
                                                       'Carbon dioxide']],USton_kg)
    ch4_n2o = egrid_unit_convert(flowbyfac_prelim[['Methane',
                                                   'Nitrous oxide']],lb_kg)
    heat_steam = egrid_unit_convert(flowbyfac_prelim[['Heat',
                                                      'Steam']],MMBtu_MJ)
    electricity = egrid_unit_convert(flowbyfac_prelim[['Electricity']],MWh_MJ)
    facilityid = flowbyfac_prelim[['FacilityID','Plant primary fuel']]
    frames = [facilityid,nox_so2_co2,ch4_n2o,heat_steam,electricity]
    flowbyfac_stacked = pd.concat(frames,axis = 1)
    #Create flowbyfac
    flowbyfac = pd.melt(flowbyfac_stacked,
                        id_vars=['FacilityID','Plant primary fuel'],
                        value_vars=list(flowbyfac_stacked.columns[2:]),
                        var_name='FlowName', value_name='FlowAmount')
    
    #Dropping na emissions
    flowbyfac = flowbyfac.dropna(subset=['FlowAmount'])
    flowbyfac = flowbyfac.sort_values(by = ['FacilityID'], axis=0, 
                                      ascending=True, inplace=False, 
                                      kind='quicksort', na_position='last')
    
    #Merge in heat_SO2_CO2_NOx reliability scores calculated from unit sheet
    flowbyfac = flowbyfac.merge(rel_scores_by_facility,
                                on = ['FacilityID','FlowName'], how = 'left')
    #Assign electricity to a reliabilty score of 1
    flowbyfac['DataReliability'].loc[flowbyfac['FlowName']=='Electricity'] = 1
    #Replace NaNs with 5
    flowbyfac['DataReliability']=flowbyfac['DataReliability'].replace({None:5})
    
    #Methane and nitrous oxide reliability scores
    #Assign 3 to all facilities except for certain fuel types where 
    #measurements are taken
    flowbyfac.loc[(flowbyfac['FlowName']=='Methane') |
                  (flowbyfac['FlowName']=='Nitrous oxide')
                    ,'DataReliability'] = 3
    #For all but selected fuel types, change it to 2
    flowbyfac.loc[((flowbyfac['FlowName']=='Methane') |
                   (flowbyfac['FlowName']=='Nitrous oxide')) &
                   ((flowbyfac['Plant primary fuel'] != 'PG') |
                    (flowbyfac['Plant primary fuel'] != 'RC') |
                    (flowbyfac['Plant primary fuel'] != 'WC') |
                    (flowbyfac['Plant primary fuel'] != 'SLW'))
                    ,'DataReliability'] = 2
    
    #Now the plant primary fuel is no longer needed
    flowbyfac = flowbyfac.drop(columns = ['Plant primary fuel'])
    
    #Import flow compartments
    flow_compartments = pd.read_csv(eGRID_data_dir+'eGRID_flow_compartments.csv',
                                    header=0)
    
    #Merge in with flowbyfacility
    flowbyfac = pd.merge(flowbyfac,flow_compartments,on='FlowName',how='left')
    
    #Drop original name
    flowbyfac = flowbyfac.drop(columns='OriginalName')
    
    #Write flowbyfacility file to output
    storeInventory(flowbyfac, 'eGRID_' + year, 'flowbyfacility')
    
    ##Creation of the facility file
    #Need to change column names manually
    facility=egrid2[['FacilityName','Plant operator name','FacilityID',
                     'State','eGRID subregion acronym','Plant county name',
                     'Plant latitude', 'Plant longitude','Plant primary fuel',
                     'Plant primary coal/oil/gas/ other fossil fuel category',
                     'NERC region acronym',
                     'Balancing Authority Name','Balancing Authority Code',
                     'Plant coal generation percent (resource mix)',
                     'Plant oil generation percent (resource mix)',
                     'Plant gas generation percent (resource mix)',
                     'Plant nuclear generation percent (resource mix)',
                     'Plant hydro generation percent (resource mix)',
                     'Plant biomass generation percent (resource mix)',
                     'Plant wind generation percent (resource mix)',
                     'Plant solar generation percent (resource mix)',
                     'Plant geothermal generation percent (resource mix)',
                     'Plant other fossil generation percent (resource mix)',
                     'Plant other unknown / purchased fuel generation '
                         'percent (resource mix)']]
    
    # Data starting in 2018 for resource mix is listed as percentage.
    # For consistency multiply by 100
    if int(year) >= 2018:
        facility.loc[:,facility.columns.str.contains('resource mix')] *=100
    
    log.debug(len(facility))
    #2019: 11865
    #2018: 10964
    #2016: 9709
    #2014: 8503
    storeInventory(facility, 'eGRID_' + year, 'facility')
    
    ##Write flows file
    flows = flowbyfac[['FlowName','Compartment','Unit']]
    flows = flows.drop_duplicates()
    flows = flows.sort_values(by='FlowName',axis=0)
    storeInventory(flows, 'eGRID_' + year, 'flow')


def validate_eGRID(year):
    #VALIDATE
    log.info('validating data against national totals')
    validation_file = data_dir + 'eGRID_'+ year + '_NationalTotals.csv'
    if (os.path.exists(validation_file)):
        egrid_national_totals = pd.read_csv(validation_file,header=0,
                                            dtype={"FlowAmount":float})
        egrid_national_totals = unit_convert(
            egrid_national_totals,'FlowAmount', 'Unit', 'lbs',
            lb_kg, 'FlowAmount')
        egrid_national_totals = unit_convert(
            egrid_national_totals,'FlowAmount', 'Unit', 'tons',
            USton_kg, 'FlowAmount')
        egrid_national_totals = unit_convert(
            egrid_national_totals,'FlowAmount', 'Unit', 'MMBtu',
            MMBtu_MJ, 'FlowAmount')
        egrid_national_totals = unit_convert(
            egrid_national_totals,'FlowAmount', 'Unit', 'MWh',
            MWh_MJ, 'FlowAmount')
        # drop old unit
        egrid_national_totals.drop('Unit',axis=1,inplace=True)
        flowbyfac = readInventory('eGRID_'+ year, 'flowbyfacility')
        validation_result = validate_inventory(flowbyfac, egrid_national_totals,
                                               group_by='flow', tolerance=5.0)
        write_validation_result('eGRID',year,validation_result)
    else:
        log.warning('validation file for eGRID_%s does not exist. Please '
                    'run option C', year)

def generate_national_totals(year):
    #Download and process eGRID national totals
    log.info('Processing eGRID national totals for %s', year)
    totals_dict = {'USHTIANT':'Heat',
                   'USNGENAN':'Electricity',
                   #'USETHRMO':'Steam', #PLNTYR sheet
                   'USNOXAN':'Nitrogen oxides',
                   'USSO2AN':'Sulfur dioxide',
                   'USCO2AN':'Carbon dioxide',
                   'USCH4AN':'Methane',
                   'USN2OAN':'Nitrous oxide',
                   }
    
    us_totals = extract_eGRID_excel(year, 'US', index='code')
    us_totals = us_totals[list(totals_dict.keys())]
    us_totals.rename(columns=totals_dict, inplace=True)
    us_totals = us_totals.transpose().reset_index()
    us_totals = us_totals.rename(columns={'index':'FlowName',
                                         0:'FlowAmount'})
    
    steam_df = extract_eGRID_excel(year, 'PLNT', index='code')
    steam_total = steam_df['USETHRMO'].sum()
    us_totals = us_totals.append({'FlowName':'Steam', 'FlowAmount':steam_total},
                                 ignore_index=True)
    
    flow_compartments = pd.read_csv(eGRID_data_dir+'eGRID_flow_compartments.csv',
                                    usecols=['FlowName','Compartment'])
    us_totals = us_totals.merge(flow_compartments, how = 'left', on = 'FlowName')
    
    us_totals.loc[(us_totals['FlowName']=='Carbon dioxide') |
                  (us_totals['FlowName']=='Sulfur dioxide') |
                  (us_totals['FlowName']=='Nitrogen oxides'),
                  'Unit'] = 'tons'
    us_totals.loc[(us_totals['FlowName']=='Methane') |
                  (us_totals['FlowName']=='Nitrous oxide'),
                  'Unit'] = 'lbs'
    us_totals.loc[(us_totals['FlowName']=='Heat') |
                  (us_totals['FlowName']=='Steam'),
                  'Unit'] = 'MMBtu'
    us_totals.loc[(us_totals['FlowName']=='Electricity'),
                  'Unit'] = 'MWh'
    log.info('saving eGRID_%s_NationalTotals.csv to %s', year, data_dir)
    us_totals = us_totals[['FlowName','Compartment','FlowAmount','Unit']]
    us_totals.to_csv(data_dir+'eGRID_'+year+'_NationalTotals.csv',index=False)
    
    #Update validationSets_Sources.csv
    validation_dict = {'Inventory':'eGRID',
                       'Version':_config[year]['file_version'],
                       'Year':year,
                       'Name':'eGRID Data Files',
                       'URL':_config[year]['download_url'],
                       'Criteria':'Extracted from US Total tab, or for '
                       'steam, summed from PLNT tab',
                       }
    update_validationsets_sources(validation_dict)
    

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Download eGRID data\
                        [B] Process and parse eGRID data\
                        [C] National Totals',
                        type = str)

    parser.add_argument('-y', '--Year', nargs = '+',
                        help = 'What eGRID year you want to retrieve',
                        type = str)
    
    args = parser.parse_args()
    
    for year in args.Year:
        if args.Option == 'A':
            #download data
            download_eGRID(year)
            generate_metadata(year, datatype='source')
            
        if args.Option == 'B':
            #process data
            generate_eGRID_files(year)
            generate_metadata(year, datatype='inventory')
            validate_eGRID(year)
            
        if args.Option == 'C':
            #download and store national totals
            generate_national_totals(year)
