# egrid import and processing
# This script uses the egrid excel files found here:
# https://www.epa.gov/sites/production/files/2018-02/egrid2016_all_files_since_1996.zip
# The format may change from yr to yr requiring code updates.
# This code has been tested for 2014.

import pandas as pd
from stewi import globals
from stewi.globals import unit_convert

# Set some metadata
eGRIDyear = '2014'
output_dir = globals.output_dir
data_dir = globals.data_dir

#filepath
eGRIDfilepath = '../eGRID/'

#filename for 2014
eGRIDfile = eGRIDfilepath + 'eGRID2014_Data_v2.xlsx'

pltsheetname = 'PLNT14'

# Import list of fields from egrid that are desired for LCI
def imp_fields(fields_txt):
    egrid_req_fields_df = pd.read_csv(fields_txt, header=None)
    egrid_req_fields = list(egrid_req_fields_df[0])
    return egrid_req_fields

egrid_required_fields = (imp_fields(data_dir+'eGRID_required_fields.txt'))

# Import egrid file
egrid = pd.read_excel(eGRIDfile, sheet_name=pltsheetname)
#drop first row which are column name abbreviations
egrid = egrid.drop([0])

#use_cols not working so drop them after import
#get list of columns not in the required fields and drop them
colstodrop = list(set(list(egrid.columns)) - set(egrid_required_fields))
egrid2 = egrid.drop(colstodrop,axis=1)


#Need to change column names manually
egrid = egrid.drop(['eGRID2014 Plant file sequence number','NERC region acronym','Plant name'],axis = 1)
egrid = egrid.dropna(subset=['Plant primary fuel'])
egrid = egrid[egrid['Plant primary fuel'] != 0]
egrid = egrid.dropna(subset=['eGRID subregion acronym'])
egrid = egrid[egrid['eGRID subregion acronym'] != 0]                 
egrid = egrid.dropna(subset=['Efficiency'])
egrid = egrid[egrid['Efficiency'] != 0]



def unit_convert(value,factor):
    new_val = value*factor;
    return new_val;



#Creation of the facility file
#Need to change column names manually
def createfacilityfile(): 
   
   
    facility1=egrid[['DOE/EIA ORIS plant or facility code','Plant state abbreviation']]
   
    facility2=egrid[['DOE/EIA ORIS plant or facility code','Plant state abbreviation','eGRID subregion acronym','Plant primary fuel','Plant primary coal/oil/gas/ other fossil fuel category','Efficiency']]
  
    os.chdir(dir_path)
    
    facility1.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID','Plant state abbreviation':'State'},inplace=True)
    facility1.to_csv('egrid_2014.csv', index=False)
    facility2.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID','Plant state abbreviation':'State'},inplace=True)
    facility2.to_csv('egrid_2014(with_options).csv', index=False)

#Use this line for printing the column headers. Already done. 
#names = egrid.columns.values
#print(names)

"""
['Plant state abbreviation' 'DOE/EIA ORIS plant or facility code'
 'eGRID subregion acronym' 'Plant primary fuel'
 'Plant primary coal/oil/gas/ other fossil fuel category'
 'Plant annual NOx total output emission rate (lb/MWh)'
 'Plant annual SO2 total output emission rate (lb/MWh)'
 'Plant annual CO2 total output emission rate (lb/MWh)'
 'Plant annual CH4 total output emission rate (lb/GWh)'
 'Plant annual N2O total output emission rate (lb/GWh)' 'Prime Mover'
 'Plant annual net generation (MWh)' 'Efficiency'
 'Plant annual NOx total output emission rate (kg/MWh)'
 'Plant annual SO2 total output emission rate (kg/MWh)']
"""


#Need to change column names manually
def createflowbyfacility():
    flow = egrid[['DOE/EIA ORIS plant or facility code', 'Plant annual NOx total output emission rate (lb/MWh)','Plant annual SO2 total output emission rate (lb/MWh)','Plant annual CO2 total output emission rate (lb/MWh)','Plant annual CH4 total output emission rate (lb/GWh)','Plant annual N2O total output emission rate (lb/GWh)']]

    flow1  = unit_convert(flow[flow.columns[1:6]],0.4535924)
    flow2 = egrid[['DOE/EIA ORIS plant or facility code']] 
    frames = [flow2,flow1]
    flow3 = pd.concat(frames,axis = 1)
    flow5 = pd.melt(flow3, id_vars=['DOE/EIA ORIS plant or facility code'], value_vars=list(flow3.columns[2:]), var_name='FlowName', value_name='FlowAmount')    

    return flow5;


newpath = dir_path+'\\facility_subfolder'
#os.mkdir(newpath)

flow6 = createflowbyfacility();
flow6.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID', 'Plant annual NOx total output emission rate (lb/MWh)':'Plant annual NOx total output emission rate (kg/MWh)','Plant annual SO2 total output emission rate (lb/MWh)':'Plant annual SO2 total output emission rate (kg/MWh)','Plant annual CO2 total output emission rate (lb/MWh)':'Plant annual CO2 total output emission rate (kg/MWh)','Plant annual CH4 total output emission rate (lb/GWh)':'Plant annual CH4 total output emission rate (kg/GWh)','Plant annual N2O total output emission rate (lb/GWh)':'Plant annual N2O total output emission rate (kg/GWh)'},inplace=True)
flow6['ReliabilityScore'] = 0;
os.chdir(newpath)
flow6.to_csv('egrid_2014.csv', index=False)

createfacilityfile()




