# -*- coding: utf-8 -*-
"""
Created on Wed May  9 15:24:31 2018
@author: TGhosh
"""

#!/usr/bin/env python

# egrid import and processing
# This script uses the egrid Basic Plus National Data File.
# Data files:https://www.epa.gov/toxics-release-inventory-egrid-program/egrid-basic-plus-data-files-calendar-years-1987-2016
# Documentation on file format: https://www.epa.gov/toxics-release-inventory-egrid-program/egrid-basic-plus-data-files-guides
# The format may change from yr to yr requiring code updates.
# This code has been tested for 2014.

import pandas as pd
import os



dir_path = os.path.dirname(os.path.realpath(__file__))


# Import list of fields from egrid that are desired for LCI
def imp_fields(egrid_fields_txt):
    egrid_required_fields_csv = egrid_fields_txt
    egrid_req_fields = pd.read_csv(egrid_required_fields_csv, header=None)
    #egrid_req_fields = list(egrid_req_fields[2])
    return egrid_req_fields

egrid_required_fields = (imp_fields('egrid_required_fields.txt'))

# Import in pieces grabbing main fields plus unique amount and basis of estimate fields
# assigns fields to variables

# Import egrid file


fieldnames = list(egrid_required_fields.values);





#Read egrid Database
#Need to change the file name over here 
def import_egrid_by_release_type():
    
     egrid1 = pd.read_excel('egrid.xlsx', header=0,error_bad_lines=False,skiplinespaces = True)
     return egrid1

egrid = import_egrid_by_release_type()
egrid = egrid[fieldnames[0]]
#Need to change column names manually
#egrid = egrid.drop(['eGRID2014 Plant file sequence number','NERC region acronym','Plant name'],axis = 1)
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

