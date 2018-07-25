#Data source:
url = 'https://www.epa.gov/sites/production/files/2018-02/egrid2016_all_files_since_1996.zip'

import pandas as pd
from stewi import globals #@author: Wes
import os
import numpy as np
import sys
from stewi.globals import write_metadata

# Set the year
eGRIDyear = '2016'

year_last2 = eGRIDyear[2:]
output_dir = globals.output_dir
data_dir = globals.data_dir

#filepath
eGRIDfilepath = '../eGRID/'
egrid_file_begin = {"2014":"eGRID2014", "2016":"egrid2016"}
egrid_file_version = {"2014":"_v2","2016":""}

#filename for 2014
eGRIDfile = eGRIDfilepath + egrid_file_begin[eGRIDyear] + '_Data' + egrid_file_version[eGRIDyear] + '.xlsx'
pltsheetname = 'PLNT'+ year_last2
untsheetname = 'UNT' + year_last2

# Import list of fields from egrid that are desired for LCI
def imp_fields(fields_txt):
    egrid_req_fields_df = pd.read_csv(fields_txt, header=None)
    egrid_req_fields = list(egrid_req_fields_df[0])
    return egrid_req_fields

egrid_required_fields = (imp_fields(data_dir+'egrid_required_fields.txt')) #@author: Wes

# Import egrid file
egrid = pd.read_excel(eGRIDfile, sheet_name=pltsheetname, skipinitialspace = True)
#drop first row which are column name abbreviations
egrid = egrid.drop([0])

#use_cols not working so drop them after import
#get list of columns not in the required fields and drop them
colstodrop = list(set(list(egrid.columns)) - set(egrid_required_fields))
egrid2 = egrid.drop(colstodrop,axis=1)
egrid2.columns


def unit_convert(value,factor):
    new_val = value*factor;
    return new_val;


#Reliability Scores for EGRID
    
unit_egrid_required_fields = (imp_fields(data_dir+'egrid_unit_level_required_fields.txt')) #@author: Wes

unit_egrid = pd.read_excel(eGRIDfile, sheet_name=untsheetname, skipinitialspace = True)
#drop first row which are column name abbreviations
unit_egrid = unit_egrid.drop([0])

#use_cols not working so drop them after import
#get list of columns not in the required fields and drop them
colstodrop = list(set(list(unit_egrid.columns)) - set(unit_egrid_required_fields))
unit_egrid2 = unit_egrid.drop(colstodrop,axis=1)
rel_scores = pd.read_csv(data_dir+'eGRID_unit_level_reliability_scores.csv')




unit_egrid2 = unit_egrid2.merge(rel_scores, left_on =['Unit unadjusted annual heat input source'], right_on =['Source'], how = 'left')
unit_egrid2 = unit_egrid2.rename(columns= {'ReliabilityScore':'ReliabilityScore_heat'})
del unit_egrid2['Source']

unit_egrid2 = unit_egrid2.merge(rel_scores, left_on =['Unit unadjusted annual NOx emissions source'], right_on =['Source'], how = 'left')
unit_egrid2 = unit_egrid2.rename(columns= {'ReliabilityScore':'ReliabilityScore_NOx'})
del unit_egrid2['Source']

unit_egrid2 = unit_egrid2.merge(rel_scores, left_on =['Unit unadjusted annual SO2 emissions source'], right_on =['Source'], how = 'left')
unit_egrid2 = unit_egrid2.rename(columns= {'ReliabilityScore':'ReliabilityScore_SO2'})
del unit_egrid2['Source']

unit_egrid2 = unit_egrid2.merge(rel_scores, left_on =['Unit unadjusted annual CO2 emissions source'], right_on =['Source'], how = 'left')
unit_egrid2 = unit_egrid2.rename(columns= {'ReliabilityScore':'ReliabilityScore_CO2'})
del unit_egrid2['Source']

#unit_egrid2 = unit_egrid2.replace({0:None})

emissions = ['Heat','Nitrogen oxides','Sulfur oxide','Carbon dioxide']


cols = ['ReliabilityScore_heat','ReliabilityScore_NOx','ReliabilityScore_SO2','ReliabilityScore_CO2']
flow = ['Unit unadjusted annual heat input (MMBtu)','Unit unadjusted annual NOx emissions (tons)','Unit unadjusted annual SO2 emissions (tons)','Unit unadjusted annual CO2 emissions (tons)']
unit_egrid2[cols] = np.multiply(unit_egrid2[cols],unit_egrid2[flow])

unit_egrid3 = unit_egrid2.groupby(['DOE/EIA ORIS plant or facility code'])['ReliabilityScore_heat','ReliabilityScore_NOx','ReliabilityScore_SO2','ReliabilityScore_CO2'].sum()
unit_egrid4 = unit_egrid2.groupby(['DOE/EIA ORIS plant or facility code'])['Unit unadjusted annual heat input (MMBtu)','Unit unadjusted annual NOx emissions (tons)','Unit unadjusted annual SO2 emissions (tons)','Unit unadjusted annual CO2 emissions (tons)'].sum()


unit_egrid3 = unit_egrid3.reset_index()
unit_egrid4 = unit_egrid4.reset_index()

unit_egrid5 = unit_egrid3.merge(unit_egrid4, left_on = ['DOE/EIA ORIS plant or facility code'],right_on = ['DOE/EIA ORIS plant or facility code'], how = 'inner')

unit_egrid5[cols] = np.divide(unit_egrid5[cols],unit_egrid5[flow])

unit_egrid5[emissions] = unit_egrid5[cols]

unit_egrid5['FacilityID'] = unit_egrid5['DOE/EIA ORIS plant or facility code']

unit_egrid6 = pd.melt(unit_egrid5, id_vars=['FacilityID'], value_vars=emissions, var_name='FlowName', value_name='ReliabilityScore') 



#Creation of the facility file
#Need to change column names manually
def createfacilityfile(): 
    facility=egrid2[['Plant name','Plant operator name','DOE/EIA ORIS plant or facility code',
                     'Plant state abbreviation','eGRID subregion acronym','Plant county name',
                     'Plant latitude', 'Plant longitude','Plant primary fuel',
                     'Plant primary coal/oil/gas/ other fossil fuel category','NERC region acronym',
                     'Plant coal generation percent (resource mix)',
                     'Plant oil generation percent (resource mix)',
                     'Plant gas generation percent (resource mix)',
                     'Plant nuclear generation percent (resource mix)',
                     'Plant  hydro generation percent (resource mix)',
                     'Plant biomass generation percent (resource mix)',
                     'Plant wind generation percent (resource mix)',
                     'Plant solar generation percent (resource mix)',
                     'Plant geothermal generation percent (resource mix)',
                     'Plant other fossil generation percent (resource mix)',
                     'Plant other unknown / purchased fuel generation percent (resource mix)']]
    facility.rename(columns={'Plant name':'FacilityName','DOE/EIA ORIS plant or facility code':'FacilityID','Plant state abbreviation':'State'},inplace=True)
    return facility

#Use this line for printing the column headers. Already done.
#names = egrid.columns.values
#print(names)








#Need to change column names manually
def createflowbyfacility():
    flow = egrid2[['DOE/EIA ORIS plant or facility code','Plant total annual heat input (MMBtu)','Plant annual net generation (MWh)', 'Plant annual NOx emissions (tons)','Plant annual SO2 emissions (tons)','Plant annual CO2 emissions (tons)','Plant annual CH4 emissions (lbs)','Plant annual N2O emissions (lbs)','CHP plant useful thermal output (MMBtu)']]
    flow.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID',
                         'Plant total annual heat input (MMBtu)':'Heat',
                         'Plant annual net generation (MWh)':'Electricity',
                         'Plant annual NOx emissions (tons)':'Nitrogen oxides',
                         'Plant annual SO2 emissions (tons)':'Sulfur dioxide',
                         'Plant annual CO2 emissions (tons)':'Carbon dioxide',
                         'Plant annual CH4 emissions (lbs)':'Methane',
                         'Plant annual N2O emissions (lbs)':'Nitrous oxide',
                         'CHP plant useful thermal output (MMBtu)':'Steam'},inplace=True)
    flow1 = unit_convert(flow[['Nitrogen oxides','Sulfur dioxide','Carbon dioxide']],907.1874)
    flow1_1 = unit_convert(flow[['Methane','Nitrous oxide']],0.4535924)
    flow2 = unit_convert(flow[['Heat']],1055.056)
    flow3 = unit_convert(flow[['Electricity']],3600)
    flow4 = flow[['FacilityID']]
    flow4_4 = unit_convert(flow['Steam'],1055.056)
    frames = [flow4,flow2,flow4_4,flow3,flow1,flow1_1]
    flow5 = pd.concat(frames,axis = 1)
    
    flow6 = pd.melt(flow5, id_vars=['FacilityID'], value_vars=list(flow5.columns[1:]), var_name='FlowName', value_name='FlowAmount')    
    return flow6;


flowbyfac_1 = createflowbyfacility();

#flow6.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID', 'Plant annual NOx total output emission rate (lb/MWh)':'Plant annual NOx total output emission rate (kg/MWh)','Plant annual SO2 total output emission rate (lb/MWh)':'Plant annual SO2 total output emission rate (kg/MWh)','Plant annual CO2 total output emission rate (lb/MWh)':'Plant annual CO2 total output emission rate (kg/MWh)','Plant annual CH4 total output emission rate (lb/GWh)':'Plant annual CH4 total output emission rate (kg/GWh)','Plant annual N2O total output emission rate (lb/GWh)':'Plant annual N2O total output emission rate (kg/GWh)'},inplace=True)
flowbyfac = flowbyfac_1.merge(unit_egrid6,left_on = ['FacilityID','FlowName'],right_on = ['FacilityID','FlowName'], how = 'inner')


#Dropping na emissions
flowbyfac = flowbyfac.dropna(subset=['FlowAmount'])
flowbyfac = flowbyfac.sort_values(by = ['FacilityID'], axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last')

#Import flow compartments
flow_compartments = pd.read_csv(data_dir+'eGRID_flow_compartments.csv',header=0)
#Merge in with flowbyfacility
flowbyfac = pd.merge(flowbyfac,flow_compartments,on='FlowName',how='left')
#Drop original name
flowbyfac.drop(columns='OriginalName', inplace=True)


#os.chdir(output_dir)
#Write flowbyfacility file to output
flowbyfac.to_csv(output_dir + 'eGRID_'+ eGRIDyear +'.csv', index=False)

facility = createfacilityfile()
len(facility)
facility.head()
facility.to_csv(output_dir + '/facility/eGRID_' + eGRIDyear + '.csv', index=False)

##Write flows file
flows = flowbyfac[['FlowName','Compartment','Unit']]
flows.drop_duplicates(inplace=True)
flows.to_csv(output_dir + '/flow/eGRID_' + eGRIDyear + '.csv', index=False)

#Write metadata
eGRID_meta = globals.inventory_metadata

#Set time manually for now
eGRID_meta['SourceAquisitionTime'] = 'Wed May 10 10:00:01 2018'
eGRID_meta['SourceType'] = 'Static File'
eGRID_meta['SourceFileName'] = eGRIDfile
eGRID_meta['SourceURL'] = url
eGRID_meta['SourceVersion'] = egrid_file_version[eGRIDyear]
write_metadata('eGRID',eGRIDyear, eGRID_meta)
