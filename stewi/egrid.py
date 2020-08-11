#!/usr/bin/env python
"""
Imports eGRID data and processes to Standardized EPA output format.
Uses the eGRID data files from EPA.
This file requires parameters be passed like:

    Option -y Year 

Options:
    A - 
    B - 
    C - 

Year: 
    2018
    2016
    2014
"""

import pandas as pd
import numpy as np
import argparse
import time
import os
from stewi.globals import output_dir,data_dir,write_metadata,\
    inventory_metadata,unit_convert,log,MMBtu_MJ,MWh_MJ,config,\
    validate_inventory,write_validation_result,USton_kg,lb_kg

#filepath
_config = config()['databases']['eGRID']
url = _config['url']

eGRIDfilepath = '../eGRID/'
egrid_file_begin = {"2014":"eGRID2014", "2016":"egrid2016", "2018":"eGRID2018"}
egrid_file_version = {"2014":"_v2","2016":"","2018":"_v2"}

# Import list of fields from egrid that are desired for LCI
def imp_fields(fields_txt, year):
    egrid_req_fields_df = pd.read_csv(fields_txt, header=0)
    egrid_req_fields = list(egrid_req_fields_df[year])
    col_dict = egrid_req_fields_df.set_index(year).to_dict()
    return egrid_req_fields, col_dict

def egrid_unit_convert(value,factor):
    new_val = value*factor;
    return new_val;

def download_eGRID(year):
    log.info('downloading eGRID data')
    #TODO add data download

def generate_eGRID_files(year):
    log.info('generating eGRID files for '+ year)
    year_last2 = year[2:]
    eGRIDfile = eGRIDfilepath + egrid_file_begin[year] + '_Data' + egrid_file_version[year] + '.xlsx'
    pltsheetname = 'PLNT'+ year_last2
    untsheetname = 'UNT' + year_last2
    
    # Import egrid file
    egrid = pd.read_excel(eGRIDfile, sheet_name=pltsheetname, skipinitialspace = True)
    #drop first row which are column name abbreviations
    egrid = egrid.drop([0])
    
    #egrid.to_csv(eGRIDfilepath+'egrid_temp.csv', index=False)
    #egrid = pd.read_csv(eGRIDfilepath+'egrid_temp.csv')
    
    #use_cols not working so drop them after import
    #get list of columns not in the required fields and drop them
    egrid_required_fields, egrid_col_dict = imp_fields(data_dir+'eGRID_required_fields.csv',year)
    colstodrop = list(set(list(egrid.columns)) - set(egrid_required_fields))
    egrid2 = egrid.drop(colstodrop,axis=1)
    egrid2.rename(columns = egrid_col_dict['StEWI'], inplace=True)
    
    #Read in unit sheet to get comment fields related to source of heat,NOx,SO2, and CO2 emission estimates
    unit_egrid_required_fields, unit_egrid_col_dict = imp_fields(data_dir+'eGRID_unit_level_required_fields.csv',year) #@author: Wes
    unit_egrid = pd.read_excel(eGRIDfile, sheet_name=untsheetname, skipinitialspace = True)
    #drop first row which are column name abbreviations
    unit_egrid = unit_egrid.drop([0])
    
    #get list of columns not in the required fields and drop them
    colstodrop = list(set(list(unit_egrid.columns)) - set(unit_egrid_required_fields))
    unit_egrid2 = unit_egrid.drop(colstodrop,axis=1)
    unit_egrid2.rename(columns = unit_egrid_col_dict['StEWI'], inplace=True)
    
    #Import mapping between heat,NOx,SO2, and CO2 emissions source comments and reliability scores. Merge one by one.
    rel_scores_heat_SO2_CO2_NOx = pd.read_csv(data_dir+'eGRID_unit_level_reliability_scores.csv')
    unit_egrid2 = unit_egrid2.merge(rel_scores_heat_SO2_CO2_NOx, left_on =['Unit unadjusted annual heat input source'], right_on =['Source'], how = 'left')
    unit_egrid2 = unit_egrid2.rename(columns= {'ReliabilityScore':'ReliabilityScore_heat'})
    del unit_egrid2['Source']
    unit_egrid2 = unit_egrid2.merge(rel_scores_heat_SO2_CO2_NOx, left_on =['Unit unadjusted annual NOx emissions source'], right_on =['Source'], how = 'left')
    unit_egrid2 = unit_egrid2.rename(columns= {'ReliabilityScore':'ReliabilityScore_NOx'})
    del unit_egrid2['Source']
    unit_egrid2 = unit_egrid2.merge(rel_scores_heat_SO2_CO2_NOx, left_on =['Unit unadjusted annual SO2 emissions source'], right_on =['Source'], how = 'left')
    unit_egrid2 = unit_egrid2.rename(columns= {'ReliabilityScore':'ReliabilityScore_SO2'})
    del unit_egrid2['Source']
    unit_egrid2 = unit_egrid2.merge(rel_scores_heat_SO2_CO2_NOx, left_on =['Unit unadjusted annual CO2 emissions source'], right_on =['Source'], how = 'left')
    unit_egrid2 = unit_egrid2.rename(columns= {'ReliabilityScore':'ReliabilityScore_CO2'})
    del unit_egrid2['Source']
    
    unit_emissions_with_rel_scores = ['Heat','Nitrogen oxides','Sulfur dioxide','Carbon dioxide']
    
    #Calculate reliability scores at plant level using flow-weighted average.
    rel_score_cols = ['ReliabilityScore_heat','ReliabilityScore_NOx','ReliabilityScore_SO2','ReliabilityScore_CO2']
    flows_used_for_weighting = ['Unit unadjusted annual heat input (MMBtu)',
                                'Unit unadjusted annual NOx emissions (tons)',
                                'Unit unadjusted annual SO2 emissions (tons)',
                                'Unit unadjusted annual CO2 emissions (tons)']
    #First multiply by flows
    unit_egrid2[rel_score_cols] = np.multiply(unit_egrid2[rel_score_cols],unit_egrid2[flows_used_for_weighting])
    #Aggregate the multiplied scores at the plant level
    unit_egrid3 = unit_egrid2.groupby(['DOE/EIA ORIS plant or facility code'])['ReliabilityScore_heat','ReliabilityScore_NOx','ReliabilityScore_SO2','ReliabilityScore_CO2'].sum().reset_index()
    unit_egrid4 = unit_egrid2.groupby(['DOE/EIA ORIS plant or facility code'])['Unit unadjusted annual heat input (MMBtu)','Unit unadjusted annual NOx emissions (tons)','Unit unadjusted annual SO2 emissions (tons)','Unit unadjusted annual CO2 emissions (tons)'].sum().reset_index()
    unit_egrid5 = unit_egrid3.merge(unit_egrid4, left_on = ['DOE/EIA ORIS plant or facility code'],right_on = ['DOE/EIA ORIS plant or facility code'], how = 'inner')
    unit_egrid5[rel_score_cols] = np.divide(unit_egrid5[rel_score_cols],unit_egrid5[flows_used_for_weighting])
    #Throws a RuntimeWarning about true_divide
    
    unit_egrid5[unit_emissions_with_rel_scores] = unit_egrid5[rel_score_cols]
    unit_egrid5['FacilityID'] = unit_egrid5['DOE/EIA ORIS plant or facility code']
    rel_scores_heat_SO2_CO2_NOx_by_facility = pd.melt(unit_egrid5, id_vars=['FacilityID'], value_vars=unit_emissions_with_rel_scores, var_name='FlowName', value_name='ReliabilityScore')
    
    ##Create FLOWBYFACILITY output
    flowbyfac_prelim = egrid2[['DOE/EIA ORIS plant or facility code',
                               'Plant primary fuel',
                               'Plant total annual heat input (MMBtu)',
                               'Plant annual net generation (MWh)',
                               'Plant annual NOx emissions (tons)',
                               'Plant annual SO2 emissions (tons)',
                               'Plant annual CO2 emissions (tons)',
                               'Plant annual CH4 emissions (lbs)',
                               'Plant annual N2O emissions (lbs)',
                               'CHP plant useful thermal output (MMBtu)']]
    flowbyfac_prelim = flowbyfac_prelim.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID',
                         'Plant total annual heat input (MMBtu)':'Heat',
                         'Plant annual net generation (MWh)':'Electricity',
                         'Plant annual NOx emissions (tons)':'Nitrogen oxides',
                         'Plant annual SO2 emissions (tons)':'Sulfur dioxide',
                         'Plant annual CO2 emissions (tons)':'Carbon dioxide',
                         'Plant annual CH4 emissions (lbs)':'Methane',
                         'Plant annual N2O emissions (lbs)':'Nitrous oxide',
                         'CHP plant useful thermal output (MMBtu)':'Steam'})
    nox_so2_co2 = egrid_unit_convert(flowbyfac_prelim[['Nitrogen oxides','Sulfur dioxide','Carbon dioxide']],USton_kg)
    ch4_n2o = egrid_unit_convert(flowbyfac_prelim[['Methane','Nitrous oxide']],lb_kg)
    heat_steam = egrid_unit_convert(flowbyfac_prelim[['Heat','Steam']],MMBtu_MJ)
    electricity = egrid_unit_convert(flowbyfac_prelim[['Electricity']],MWh_MJ)
    facilityid = flowbyfac_prelim[['FacilityID','Plant primary fuel']]
    frames = [facilityid,nox_so2_co2,ch4_n2o,heat_steam,electricity]
    flowbyfac_stacked = pd.concat(frames,axis = 1)
    #Create flowbyfac
    flowbyfac = pd.melt(flowbyfac_stacked, id_vars=['FacilityID','Plant primary fuel'], value_vars=list(flowbyfac_stacked.columns[2:]),
                        var_name='FlowName', value_name='FlowAmount')
    
    #Dropping zero emissions by changing name to NA
    #Do not drop zeroes - WI 1/16/2019
    #flowbyfac['FlowAmount'] = flowbyfac['FlowAmount'].replace({0:None})
    
    #Dropping na emissions
    flowbyfac = flowbyfac.dropna(subset=['FlowAmount'])
    flowbyfac = flowbyfac.sort_values(by = ['FacilityID'], axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last')
    
    #Merge in heat_SO2_CO2_NOx reliability scores calculated from unit sheet
    flowbyfac = flowbyfac.merge(rel_scores_heat_SO2_CO2_NOx_by_facility,left_on = ['FacilityID','FlowName'],right_on = ['FacilityID','FlowName'], how = 'left')
    #Assign electricity to a reliabilty score of 1
    flowbyfac['ReliabilityScore'].loc[flowbyfac['FlowName']=='Electricity'] = 1
    #Replace NaNs with 5
    flowbyfac['ReliabilityScore']=flowbyfac['ReliabilityScore'].replace({None:5})
    
    #Methane and nitrous oxide reliability scores
    #Assign 3 to all facilities except for certain fuel types where measurements are taken
    flowbyfac.loc[(flowbyfac['FlowName']=='Methane') | (flowbyfac['FlowName']=='Nitrous oxide')
                    ,'ReliabilityScore'] = 3
    #For all but selected fuel types, change it to 2
    flowbyfac.loc[((flowbyfac['FlowName']=='Methane') | (flowbyfac['FlowName']=='Nitrous oxide')) &
                   ((flowbyfac['Plant primary fuel'] != 'PG') |  (flowbyfac['Plant primary fuel'] != 'RC') |
                    (flowbyfac['Plant primary fuel'] != 'WC') |  (flowbyfac['Plant primary fuel'] != 'SLW'))
                    ,'ReliabilityScore'] = 2
    
    #Now the plant primary fuel is no longer needed
    flowbyfac = flowbyfac.drop(columns = ['Plant primary fuel'])
    
    #Import flow compartments
    flow_compartments = pd.read_csv(data_dir+'eGRID_flow_compartments.csv',header=0)
    
    #Merge in with flowbyfacility
    flowbyfac = pd.merge(flowbyfac,flow_compartments,on='FlowName',how='left')
    
    #Drop original name
    flowbyfac = flowbyfac.drop(columns='OriginalName')
    
    #Write flowbyfacility file to output
    flowbyfac.to_csv(output_dir + 'flowbyfacility/eGRID_'+ year +'.csv', index=False)
    
    ##Creation of the facility file
    #Need to change column names manually
    facility=egrid2[['Plant name','Plant operator name','DOE/EIA ORIS plant or facility code',
                     'Plant state abbreviation','eGRID subregion acronym','Plant county name',
                     'Plant latitude', 'Plant longitude','Plant primary fuel',
                     'Plant primary coal/oil/gas/ other fossil fuel category','NERC region acronym',
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
                     'Plant other unknown / purchased fuel generation percent (resource mix)']]
    facility = facility.rename(columns={'Plant name':'FacilityName',
                                        'DOE/EIA ORIS plant or facility code':'FacilityID',
                                        'Plant state abbreviation':'State'})
    
    # Data starting in 2018 for resource mix is listed as percentage. For consistency
    # multiply by 100
    if int(year) >= 2018:
        facility.loc[:,facility.columns.str.contains('(resource mix)')] *=100
    
    log.info(len(facility))
    #2016: 9709
    #2014: 8503
    facility.to_csv(output_dir + '/facility/eGRID_' + year + '.csv', index=False)
    
    ##Write flows file
    flows = flowbyfac[['FlowName','Compartment','Unit']]
    flows = flows.drop_duplicates()
    flows.to_csv(output_dir + '/flow/eGRID_' + year + '.csv', index=False)
    
    #Write metadata
    eGRID_meta = inventory_metadata
    
    eGRID_retrieval_time = time.ctime(os.path.getctime(eGRIDfile))
    eGRID_meta['SourceAquisitionTime'] = eGRID_retrieval_time
    eGRID_meta['SourceType'] = 'Static File'
    eGRID_meta['SourceFileName'] = eGRIDfile
    eGRID_meta['SourceURL'] = url
    eGRID_meta['SourceVersion'] = egrid_file_version[year]
    write_metadata('eGRID',year, eGRID_meta)

def validate_eGRID(year):
    #Download and process eGRID national totals
    
    
    #VALIDATE
    egrid_national_totals = pd.read_csv(data_dir + 'eGRID_'+ year + '_NationalTotals.csv',header=0,dtype={"FlowAmount":np.float})
    egrid_national_totals = unit_convert(egrid_national_totals, 'FlowAmount', 'Unit', 'lbs', lb_kg, 'FlowAmount')
    egrid_national_totals = unit_convert(egrid_national_totals, 'FlowAmount', 'Unit', 'tons', USton_kg, 'FlowAmount')
    egrid_national_totals = unit_convert(egrid_national_totals, 'FlowAmount', 'Unit', 'MMBtu', MMBtu_MJ, 'FlowAmount')
    egrid_national_totals = unit_convert(egrid_national_totals, 'FlowAmount', 'Unit', 'MWh', MWh_MJ, 'FlowAmount')
    # drop old unit
    egrid_national_totals.drop('Unit',axis=1,inplace=True)
    flowbyfac = pd.read_csv(output_dir + 'flowbyfacility/eGRID_'+ year +'.csv')
    validation_result = validate_inventory(flowbyfac, egrid_national_totals, group_by='flow', tolerance=5.0)
    write_validation_result('eGRID',year,validation_result)

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
            
        if args.Option == 'B':
            #process data
            generate_eGRID_files(year)
            
        if args.Option == 'C':
            #national totals
            validate_eGRID(year)