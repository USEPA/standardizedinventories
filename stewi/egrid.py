# -*- coding: utf-8 -*-
"""
Created on Thu May 10 15:33:39 2018

@author: TGhosh
"""

import pandas as pd 
#from stewi import globals #@author: Wes
#from stewi.globals import unit_convert #@author: Wes
import os





# Set some metadata
eGRIDyear = '2014'
#output_dir = globals.output_dir #@author: Wes
#data_dir = globals.data_dir #@author: Wes
data_dir = os.path.dirname(os.path.realpath(__file__))

#filepath
#eGRIDfilepath = '../eGRID/' #@author: Wes
eGRIDfilepath = os.path.dirname(os.path.realpath(__file__))


#filename for 2014
#eGRIDfile = eGRIDfilepath + 'eGRID2014_Data_v2.xlsx' #@author: Wes
eGRIDfile = eGRIDfilepath + '\\eGRID\\eGRID2014_Data_v2.xlsx'

pltsheetname = 'PLNT14'

# Import list of fields from egrid that are desired for LCI
def imp_fields(fields_txt):
    egrid_req_fields_df = pd.read_csv(fields_txt, header=None)
    egrid_req_fields = list(egrid_req_fields_df.values)
    return egrid_req_fields[0]

#egrid_required_fields = (imp_fields(data_dir+'eGRID_required_fields.txt')) #@author: Wes
egrid_required_fields = (imp_fields(data_dir+'\\eGRID_required_fields.txt'))

# Import egrid file
egrid = pd.read_excel(eGRIDfile, sheet_name=pltsheetname, skipinitialspace = True)
#drop first row which are column name abbreviations
egrid = egrid.drop([0])

#use_cols not working so drop them after import
#get list of columns not in the required fields and drop them
colstodrop = list(set(list(egrid.columns)) - set(egrid_required_fields))
egrid2 = egrid.drop(colstodrop,axis=1)



def unit_convert(value,factor):
    new_val = value*factor;
    return new_val;



#Creation of the facility file
#Need to change column names manually
def createfacilityfile(): 
   
   
    facility1=egrid2[['DOE/EIA ORIS plant or facility code','Plant state abbreviation']]
   
    facility2=egrid2[['DOE/EIA ORIS plant or facility code','Plant state abbreviation','eGRID subregion acronym','Plant county name','Plant latitude', 'Plant longitude','Plant primary fuel','Plant primary coal/oil/gas/ other fossil fuel category']]
  
    os.chdir(data_dir)
    
    facility1.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID','Plant state abbreviation':'State'},inplace=True)
    #facility1.to_csv('eGRID_2014.csv', index=False)
    facility2.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID','Plant state abbreviation':'State'},inplace=True)
    facility2.to_csv('eGRID_2014.csv', index=False)

#Use this line for printing the column headers. Already done. 
#names = egrid.columns.values
#print(names)




#Need to change column names manually
def createflowbyfacility():
    flow = egrid2[['DOE/EIA ORIS plant or facility code','Plant total annual heat input (MMBtu)','Plant annual net generation (MWh)','Plant annual NOx total output emission rate (lb/MWh)','Plant annual SO2 total output emission rate (lb/MWh)','Plant annual CO2 total output emission rate (lb/MWh)','Plant annual CH4 total output emission rate (lb/GWh)','Plant annual N2O total output emission rate (lb/GWh)']]
    flow.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID','Plant total annual heat input (MMBtu)':'Plant total annual heat input (MJ)','Plant annual net generation (MWh)':'Plant annual net generation (MJ)', 'Plant annual NOx total output emission rate (lb/MWh)':'NOX (kg/MWh)','Plant annual SO2 total output emission rate (lb/MWh)':'SO2(kg/MWh)','Plant annual CO2 total output emission rate (lb/MWh)':'CO2(kg/MWh)','Plant annual CH4 total output emission rate (lb/GWh)':'CH4(kg/GWh)','Plant annual N2O total output emission rate (lb/GWh)':'N2O(kg/GWh)'},inplace=True)
    
    flow1 = unit_convert(flow[flow.columns[3:8]],0.4535924)
    flow2 = unit_convert(flow[flow.columns[1]],1055.056)
    flow3 = unit_convert(flow[flow.columns[2]],3600)
    flow4 = flow[['FacilityID']] 
    frames = [flow4,flow2,flow3,flow1]
    flow5 = pd.concat(frames,axis = 1)
    flow6 = pd.melt(flow5, id_vars=['FacilityID','Plant total annual heat input (MJ)','Plant annual net generation (MJ)'], value_vars=list(flow5.columns[3:]), var_name='FlowName', value_name='FlowAmount')    
    return flow6;


newpath = data_dir+'\\output'
#os.mkdir(newpath)

flow7 = createflowbyfacility();
#flow6.rename(columns={'DOE/EIA ORIS plant or facility code':'FacilityID', 'Plant annual NOx total output emission rate (lb/MWh)':'Plant annual NOx total output emission rate (kg/MWh)','Plant annual SO2 total output emission rate (lb/MWh)':'Plant annual SO2 total output emission rate (kg/MWh)','Plant annual CO2 total output emission rate (lb/MWh)':'Plant annual CO2 total output emission rate (kg/MWh)','Plant annual CH4 total output emission rate (lb/GWh)':'Plant annual CH4 total output emission rate (kg/GWh)','Plant annual N2O total output emission rate (lb/GWh)':'Plant annual N2O total output emission rate (kg/GWh)'},inplace=True)
flow7['ReliabilityScore'] = 0;
#Dropping na emissions
flowbyfac = flow7.dropna(subset=['FlowAmount'])
os.chdir(newpath)
flowbyfac.to_csv('eGRID_2014.csv', index=False)

createfacilityfile()


