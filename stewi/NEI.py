# NEI.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Imports NEI data and processes to Standardized EPA output format.
Uses the NEI data exports from EIS. Must contain locally downloaded data for
options A:C.
This file requires parameters be passed like:
    Option -y Year 

Options:
    A - for processing downloaded NEI Point from EIS
    B - for generating inventory files for StEWI: 
        flowbyfacility
        flowbyprocess
        flows
        facilities
    C - for downloading national totals for validation

Year: 
    2017
    2016
    2014
    2011
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
import requests
import requests_ftp
import zipfile
import io

from stewi.globals import data_dir,write_metadata,\
    validate_inventory,write_validation_result,USton_kg,lb_kg,\
    log, storeInventory, config, compile_source_metadata, read_source_metadata,\
    paths, update_validationsets_sources, aggregate,\
    get_reliability_table_for_source, set_stewi_meta


_config = config()['databases']['NEI']
ext_folder = '/NEI Data Files/'
nei_external_dir = paths.local_path + ext_folder
nei_data_dir = data_dir + 'NEI/'
    
def read_data(year,file):
    """
    Reads the NEI data in the named file and returns a dataframe based on
    identified columns

    :param year : str, Year of NEI dataset for identifying field names
    :param file : str, File name (csv) containing NEI data.
    :returns file_result : DataFrame of NEI data from a single file
        with standardized column names.
    """
    file_result = pd.DataFrame(columns=list(nei_required_fields['StandardizedEPA']))
    # read nei file by chunks
    usecols = list(nei_required_fields[year].dropna())
    for file_chunk in pd.read_csv(
            nei_external_dir + file,
            usecols=usecols,
            dtype={'sppd_facility_identifier':'str'},
            chunksize=100000,
            low_memory=False):
        # change column names to Standardized EPA names
        file_chunk = file_chunk.rename(
            columns=pd.Series(list(nei_required_fields['StandardizedEPA']),
                              index=list(nei_required_fields[year])).to_dict())
        # concatenate all chunks
        file_result = pd.concat([file_result,file_chunk])
    return file_result


def standardize_output(year, source='Point'):
    """
    Reads and parses NEI data

    :param year : str, Year of NEI dataset  
    :returns nei: DataFrame of parsed NEI data.
    """
    # extract file paths
    file_paths = nei_file_path
    log.info('identified %s files: '.join(file_paths), str(len(file_paths)))
    nei = pd.DataFrame()
    # read in nei files and concatenate all nei files into one dataframe
    for file in file_paths[0:]:
        # concatenate all other files
        log.info('reading NEI data from '+ file)
        nei = pd.concat([nei,read_data(year,file)])
        log.debug(str(len(nei))+' records')
    # convert TON to KG
    nei['FlowAmount'] = nei['FlowAmount']*USton_kg

    log.info('adding Data Quality information')
    if source == 'Point':
        nei_reliability_table = get_reliability_table_for_source('NEI')
        nei_reliability_table['Code'] = nei_reliability_table['Code'].astype(float)
        nei['ReliabilityScore'] = nei['ReliabilityScore'].astype(float)
        nei = nei.merge(nei_reliability_table, left_on='ReliabilityScore',
                        right_on='Code', how='left')
        nei['DataReliability'] = nei['DQI Reliability Score']
        # drop Code and DQI Reliability Score columns
        nei = nei.drop(['Code', 'DQI Reliability Score',
                        'ReliabilityScore'], 1)
    
        nei['Compartment']='air'
        '''
        # Modify compartment based on stack height (ft)
        nei.loc[nei['StackHeight'] < 32, 'Compartment'] = 'air/ground'
        nei.loc[(nei['StackHeight'] >= 32) & (nei['StackHeight'] < 164),
                'Compartment'] = 'air/low'
        nei.loc[(nei['StackHeight'] >= 164) & (nei['StackHeight'] < 492),
                'Compartment'] = 'air/high'
        nei.loc[nei['StackHeight'] >= 492, 'Compartment'] = 'air/very high'
        '''
    else:
        nei['DataReliability'] = 3
    # add Source column
    nei['Source'] = source
    return nei


def generate_national_totals(year):
    """
    Downloads and parses pollutant national totals from 'Facility-level by
    Pollutant' data downloaded from EPA website. Used for validation.
    Creates NationalTotals.csv files.

    :param year : str, Year of NEI data for comparison.
    """
    log.info('Downloading national totals')
    
    ## generate url based on data year
    build_url = _config['national_url']
    version = _config['national_version'][year]
    url = build_url.replace('__year__', year)
    url = url.replace('__version__', version)
    print(url)
    
    ## make http request
    r = []
    requests_ftp.monkeypatch_session()
    try:
        r = requests.Session().get(url)
        #r = requests.Session().get(url, verify=False)
    except requests.exceptions.ConnectionError:
        log.error("URL Connection Error for " + url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        log.error('Error in URL request!')
    
    ## extract data from zip archive
    z = zipfile.ZipFile(io.BytesIO(r.content))
    # create a list of files contained in the zip archive
    znames = z.namelist()
    # retain only those files that are in .csv format
    znames = [s for s in znames if '.csv' in s]
    # initialize the dataframe
    df = pd.DataFrame()
    # for all of the .csv data files in the .zip archive,
    # read the .csv files into a dataframe
    # and concatenate with the master dataframe
    # captures various column headings across years
    usecols = ['pollutant code','pollutant_cd',
               'pollutant desc','pollutant_desc', 'description',
               'total emissions','total_emissions',
               'emissions uom', 'uom'
               ]
    
    for i in range(len(znames)):
        headers = pd.read_csv(z.open(znames[i]),nrows=0)
        cols = [x for x in headers.columns if x in usecols]
        df = pd.concat([df, pd.read_csv(z.open(znames[i]), 
                                        usecols = cols)])    
    
    ## parse data
    # rename columns to match standard format
    df.columns = ['FlowID', 'FlowName', 'FlowAmount', 'UOM']
    # convert LB/TON to KG
    df['FlowAmount'] = np.where(df['UOM']=='LB',
                                df['FlowAmount']*lb_kg,df['FlowAmount']*USton_kg)
    df = df.drop(['UOM'],1)
    # sum across all facilities to create national totals
    df = df.groupby(['FlowID','FlowName'])['FlowAmount'].sum().reset_index()
    # save national totals to .csv
    df.rename(columns={'FlowAmount':'FlowAmount[kg]'}, inplace=True)
    log.info('saving NEI_%s_NationalTotals.csv to %s', year, data_dir)
    df.to_csv(data_dir+'NEI_'+year+'_NationalTotals.csv',index=False)
    
    # Update validationSets_Sources.csv
    validation_dict = {'Inventory':'NEI',
                       'Version':version,
                       'Year':year,
                       'Name':'NEI Data',
                       'URL':url,
                       'Criteria':'Data Summaries tab, Facility-level by '
                       'Pollutant zip file download, summed to national level',
                       }
    update_validationsets_sources(validation_dict)


def validate_national_totals(nei_flowbyfacility, year):
    """downloads 
    """    
    log.info('validating flow by facility against national totals')
    if not(os.path.exists(data_dir + 'NEI_'+ year + '_NationalTotals.csv')):
        generate_national_totals(year)
    else:
        log.info('using already processed national totals validation file')
    nei_national_totals = pd.read_csv(data_dir + 'NEI_'+ year + \
                                      '_NationalTotals.csv',
                                      header=0,dtype={"FlowAmount[kg]":float})
    nei_national_totals.rename(columns={'FlowAmount[kg]':'FlowAmount'},
                               inplace=True)
    validation_result = validate_inventory(nei_flowbyfacility,
                                           nei_national_totals,
                                           group_by='flow', tolerance=5.0)
    write_validation_result('NEI',year,validation_result)


def generate_metadata(year, datatype = 'inventory'):
    """
    Gets metadata and writes to .json
    """
    if datatype == 'source':
        source_path = [nei_external_dir + p for p in nei_file_path]
        source_path = [os.path.realpath(p) for p in source_path]
        source_meta = compile_source_metadata(source_path, _config, year)
        write_metadata('NEI_'+year, source_meta, category=ext_folder,
                       datatype='source')
    else:
        meta = set_stewi_meta('NEI_'+year, category=ext_folder)
        file = meta.name_data + "_v" + meta.tool_version + "_" + meta.git_hash
        source_meta = read_source_metadata(
            nei_external_dir + file)['tool_meta']
        write_metadata('NEI_'+year, source_meta, datatype=datatype)
    

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Process and pickle NEI data\
                        [B] Generate StEWI inventory outputs and validate \
                            to national totals\
                        [C] Download national totals',
                        type = str)

    parser.add_argument('-y', '--Year', nargs = '+',
                        help = 'What NEI year(s) you want to retrieve',
                        type = str)
    
    args = parser.parse_args()
    
    NEIyears = args.Year
    
    for year in NEIyears:

        pickle_file = nei_external_dir + 'NEI_' + year + '.pk'
        if args.Option == 'A':

            nei_required_fields = pd.read_table(
                nei_data_dir + 'NEI_required_fields.csv',sep=',')
            nei_required_fields = nei_required_fields[[year,'StandardizedEPA']]
            nei_file_path = _config[year]['file_name']

            nei_point = standardize_output(year)
            log.info('saving processed NEI to ' + pickle_file)
            nei_point.to_pickle(pickle_file)
            generate_metadata(year, datatype='source')

        elif args.Option == 'B':
            log.info('extracting data from NEI pickle')
            try:            
                nei_point = pd.read_pickle(pickle_file)
            except FileNotFoundError:
                log.error('pickle file not found. Please run option A '
                          'before proceeding')
                sys.exit(0)
            # for backwards compatability if ReliabilityScore was used to
            # generate pickle
            if 'ReliabilityScore' in nei_point:
                nei_point['DataReliability'] = nei_point['ReliabilityScore']
            nei_point = nei_point.reset_index()

            log.info('generating flow by facility output')
            nei_flowbyfacility = aggregate(nei_point, ['FacilityID','FlowName'])
            storeInventory(nei_flowbyfacility,'NEI_'+year,'flowbyfacility')
            log.debug(len(nei_flowbyfacility))
            #2017: 2184786
            #2016: 1965918
            #2014: 2057249
            #2011: 1840866

            log.info('generating flow by SCC output')
            nei_flowbyprocess = aggregate(nei_point, ['FacilityID',
                                                      'FlowName','Process'])
            nei_flowbyprocess['ProcessType'] = 'SCC'
            storeInventory(nei_flowbyprocess, 'NEI_'+year, 'flowbyprocess')
            log.debug(len(nei_flowbyprocess))
            #2017: 4055707

            log.info('generating flows output')
            nei_flows = nei_point[['FlowName', 'FlowID', 'Compartment']]
            nei_flows = nei_flows.drop_duplicates()
            nei_flows['Unit']='kg'
            nei_flows = nei_flows.sort_values(by='FlowName',axis=0)
            storeInventory(nei_flows, 'NEI_'+year, 'flow')
            log.debug(len(nei_flows))
            #2017: 293
            #2016: 282
            #2014: 279
            #2011: 277

            log.info('generating facility output')
            facility = nei_point[['FacilityID', 'FacilityName', 'Address',
                                  'City', 'State', 'Zip', 'Latitude',
                                  'Longitude', 'NAICS', 'County']]
            facility = facility.drop_duplicates('FacilityID')
            facility = facility.astype({'Zip':'str'})
            storeInventory(facility, 'NEI_'+year, 'facility')
            log.debug(len(facility))
            #2017: 87162
            #2016: 85802
            #2014: 85125
            #2011: 95565

            generate_metadata(year, datatype='inventory')
            validate_national_totals(nei_flowbyfacility, year)
                    
        elif args.Option == 'C':
            generate_national_totals(year)
        
