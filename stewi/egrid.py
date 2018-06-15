#Data source:
url = 'https://www.epa.gov/sites/production/files/2018-02/egrid2016_all_files_since_1996.zip'

import pandas as pd
from stewi import globals #@author: Wes
import os
from stewi.globals import write_metadata

# Set the year
eGRIDyear = '2014'

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

# Import list of fields from egrid that are desired for LCI
def imp_fields(fields_txt):
    egrid_req_fields_df = pd.read_csv(fields_txt, header=None)
    egrid_req_fields = list(egrid_req_fields_df.loc[0,:])
    return egrid_req_fields

egrid_required_fields = (imp_fields(data_dir+'eGRID_required_fields.txt')) #@author: Wes
#egrid_required_fields = (imp_fields(data_dir+'\\data\\eGRID_required_fields.txt'))

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

#Creation of the facility file
#Need to change column names manually
def createfacilityfile(): 
    facility=egrid2[['Plant name','Plant operator name','DOE/EIA ORIS plant or facility code','Plant state abbreviation','eGRID subregion acronym','Plant county name','Plant latitude', 'Plant longitude','Plant primary fuel','Plant primary coal/oil/gas/ other fossil fuel category','NERC region acronym']]
    facility.rename(columns={'Plant name':'FacilityName','DOE/EIA ORIS plant or facility code':'FacilityID','Plant state abbreviation':'State'},inplace=True)
    return facility

#Use this line for printing the column headers. Already done.
#names = egrid.columns.values
#print(names)

#Need to change column names manually
def createflowbyfacility():
    flow = egrid2[['DOE/EIA ORIS plant or facility code','Plant total annual heat input (MMBtu)','Plant annual net generation (MWh)', 'Plant annual NOx emissions (tons)','Plant annual SO2 emissions (tons)','Plant annual CO2 emissions (tons)','Plant annual CH4 emissions (lbs)','Plant annual N2O emissions (lbs)']]
    flow.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID',
                         'Plant total annual heat input (MMBtu)':'Heat input',
                         'Plant annual net generation (MWh)':'Net generation',
                         'Plant annual NOx emissions (tons)':'Nitrogen oxides',
                         'Plant annual SO2 emissions (tons)':'Sulfur dioxide',
                         'Plant annual CO2 emissions (tons)':'Carbon dioxide',
                         'Plant annual CH4 emissions (lbs)':'Methane','Plant annual N2O emissions (lbs)':'Nitrous oxide'},inplace=True)
    flow1 = unit_convert(flow[flow.columns[3:6]],1000)
    flow1_1 = unit_convert(flow[flow.columns[6:8]],0.4535924)
    flow2 = unit_convert(flow[flow.columns[1]],1055.056)
    flow3 = unit_convert(flow[flow.columns[2]],3600)
    flow4 = flow[['FacilityID']]
    frames = [flow4,flow2,flow3,flow1,flow1_1]
    flow5 = pd.concat(frames,axis = 1)
    flow6 = pd.melt(flow5, id_vars=['FacilityID'], value_vars=list(flow5.columns[1:]), var_name='FlowName', value_name='FlowAmount')    
    return flow6;


flowbyfac = createflowbyfacility();
#flow6.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID', 'Plant annual NOx total output emission rate (lb/MWh)':'Plant annual NOx total output emission rate (kg/MWh)','Plant annual SO2 total output emission rate (lb/MWh)':'Plant annual SO2 total output emission rate (kg/MWh)','Plant annual CO2 total output emission rate (lb/MWh)':'Plant annual CO2 total output emission rate (kg/MWh)','Plant annual CH4 total output emission rate (lb/GWh)':'Plant annual CH4 total output emission rate (kg/GWh)','Plant annual N2O total output emission rate (lb/GWh)':'Plant annual N2O total output emission rate (kg/GWh)'},inplace=True)
flowbyfac['ReliabilityScore'] = 0;
#Dropping na emissions
flowbyfac = flowbyfac.dropna(subset=['FlowAmount'])
flowbyfac = flowbyfac.sort_values(by = ['FacilityID'], axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last')

flowbyfac.head()

os.chdir(output_dir)
flowbyfac.to_csv('eGRID_'+ eGRIDyear+'.csv', index=False)
#flowbyfac.to_csv('eGRID_2016.csv', index=False)

facility = createfacilityfile()
len(facility)
facility.head()
facility.to_csv(output_dir + '/facility/eGRID_' + eGRIDyear + '.csv', index=False)

#Write metadata
eGRID_meta = globals.inventory_metadata

#Set time manually for now
eGRID_meta['SourceAquisitionTime'] = 'Wed May 10 10:00:01 2018'
eGRID_meta['SourceType'] = 'Static File'
eGRID_meta['SourceFileName'] = eGRIDfile
eGRID_meta['SourceURL'] = url
eGRID_meta['SourceVersion'] = egrid_file_version[eGRIDyear]
write_metadata('eGRID',eGRIDyear, eGRID_meta)
