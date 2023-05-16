# NEI.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Imports NEI data and processes to Standardized EPA output format.
Uses the NEI data exports from EIS. Must contain locally downloaded data for
options A:C.
This file requires parameters be passed like:
    Option -Y Year

Option:
    A - for downloading NEI Point data and
        generating inventory files for StEWI:
        flowbyfacility
        flowbyprocess
        flows
        facilities
    B - for downloading national totals for validation

Year:
    2011-2020
"""

import argparse
import io
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from esupy.processed_data_mgmt import download_from_remote,\
    read_source_metadata
from esupy.util import strip_file_extension
from stewi.globals import DATA_PATH, write_metadata, USton_kg, lb_kg,\
    log, store_inventory, config, assign_secondary_context,\
    paths, aggregate, get_reliability_table_for_source, set_stewi_meta
from stewi.validate import update_validationsets_sources, validate_inventory,\
    write_validation_result
from stewi.formats import facility_fields


_config = config()['databases']['NEI']
EXT_DIR = 'NEI Data Files'
OUTPUT_PATH = Path(paths.local_path).joinpath(EXT_DIR)
NEI_DATA_PATH = DATA_PATH / 'NEI'


def read_data(year, file):
    """Read NEI data and return a dataframe based on identified columns.

    :param year : str, Year of NEI dataset for identifying field names
    :param file : str, File path containing NEI data (parquet).
    :returns df : DataFrame of NEI data from a single file
        with standardized column names.
    """
    nei_required_fields = pd.read_table(NEI_DATA_PATH
                                        .joinpath('NEI_required_fields.csv'),
                                        sep=',')
    nei_required_fields = nei_required_fields[[year, 'StandardizedEPA']]
    usecols = list(nei_required_fields[year].dropna())
    df = pd.read_parquet(file, columns=usecols)
    # change column names to Standardized EPA names
    df = df.rename(columns=pd.Series(list(nei_required_fields['StandardizedEPA']),
                                     index=list(nei_required_fields[year])).to_dict())
    return df


def standardize_output(year, source='Point'):
    """Read and parses NEI data.

    :param year : str, Year of NEI dataset
    :returns nei: DataFrame of parsed NEI data.
    """
    nei = pd.DataFrame()
    # read in nei files and concatenate all nei files into one dataframe
    nei_file_path = _config[year]['file_name']
    for file in nei_file_path:
        filename = OUTPUT_PATH.joinpath(file)
        if not filename.is_file():
            log.info(f'{file} not found in {OUTPUT_PATH}, '
                     'downloading source data')
            # download source file and metadata
            file_meta = set_stewi_meta(strip_file_extension(file))
            file_meta.category = EXT_DIR
            file_meta.tool = file_meta.tool.lower()
            download_from_remote(file_meta, paths)
        # concatenate all other files
        log.info(f'reading NEI data from {filename}')
        nei = pd.concat([nei, read_data(year, filename)])
        log.debug(f'{str(len(nei))} records')
    # convert TON to KG
    nei['FlowAmount'] = nei['FlowAmount'] * USton_kg

    log.info('adding Data Quality information')
    if source == 'Point':
        nei_reliability_table = get_reliability_table_for_source('NEI')
        nei_reliability_table['Code'] = nei_reliability_table['Code'].astype(float)
        nei['ReliabilityScore'] = nei['ReliabilityScore'].astype(float)
        nei = nei.merge(nei_reliability_table, left_on='ReliabilityScore',
                        right_on='Code', how='left')
        nei['DataReliability'] = nei['DQI Reliability Score']
        # drop Code and DQI Reliability Score columns
        nei = nei.drop(columns=['Code', 'DQI Reliability Score',
                                'ReliabilityScore'])

        nei['Compartment'] = 'air'
    else:
        nei['DataReliability'] = 3
    # add Source column
    nei['Source'] = source
    nei = nei.reset_index(drop=True)
    return nei


def generate_national_totals(year):
    """Download and parse pollutant national totals from 'Facility-level by
    Pollutant' data downloaded from EPA website. Used for validation.
    Creates NationalTotals.csv files.

    :param year : str, Year of NEI data for comparison.
    """
    log.info('Downloading national totals')

    # generate url based on data year
    build_url = _config['national_url']
    file = _config['national_version'][year]
    url = build_url.replace('__year__', year)
    url = url.replace('__file__', file)

    # make http request
    r = []
    try:
        r = requests.Session().get(url, verify=False)
    except requests.exceptions.ConnectionError:
        log.error(f"URL Connection Error for {url}")
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        log.error('Error in URL request!')

    # extract data from zip archive
    z = zipfile.ZipFile(io.BytesIO(r.content))
    # create a list of files contained in the zip archive
    znames = z.namelist()
    znames = [s for s in znames if '.csv' in s]
    df = pd.DataFrame()
    # for all of the .csv data files in the .zip archive,
    # read the .csv files into a dataframe
    # and concatenate with the master dataframe
    # captures various column headings across years
    usecols = ['pollutant code', 'pollutant_cd',
               'pollutant desc', 'pollutant_desc', 'description',
               'total emissions', 'total_emissions',
               'emissions uom', 'uom'
               ]

    for i in range(len(znames)):
        headers = pd.read_csv(z.open(znames[i]), nrows=0)
        cols = [x for x in headers.columns if x in usecols]
        df = pd.concat([df, pd.read_csv(z.open(znames[i]),
                                        usecols=cols)])

    # rename columns to match standard format
    df.columns = ['FlowID', 'FlowName', 'FlowAmount', 'UOM']
    # convert LB/TON to KG
    df['FlowAmount'] = np.where(df['UOM'] == 'LB', df['FlowAmount'] * lb_kg,
                                df['FlowAmount'] * USton_kg)
    df = df.drop(columns=['UOM'])
    # sum across all facilities to create national totals
    df = (df.groupby(['FlowID', 'FlowName'])['FlowAmount'].sum()
            .reset_index()
            .rename(columns={'FlowAmount': 'FlowAmount[kg]'}))
    # save national totals to .csv
    log.info(f'saving NEI_{year}_NationalTotals.csv to {DATA_PATH}')
    df.to_csv(DATA_PATH.joinpath(f'NEI_{year}_NationalTotals.csv'),
              index=False)

    # Update validationSets_Sources.csv
    validation_dict = {'Inventory': 'NEI',
                       'Version': file,
                       'Year': year,
                       'Name': 'NEI Data',
                       'URL': url,
                       'Criteria': 'Data Summaries tab, Facility-level by '
                       'Pollutant zip file download, summed to national level',
                       }
    update_validationsets_sources(validation_dict)


def validate_national_totals(nei_flowbyfacility, year):
    """Validate against national flow totals."""
    log.info('validating flow by facility against national totals')
    if not DATA_PATH.joinpath(f'NEI_{year}_NationalTotals.csv').is_file():
        generate_national_totals(year)
    else:
        log.info('using already processed national totals validation file')
    nei_national_totals = (
        pd.read_csv(DATA_PATH.joinpath(f'NEI_{year}_NationalTotals.csv'),
                    header=0, dtype={"FlowAmount[kg]": float})
          .rename(columns={'FlowAmount[kg]': 'FlowAmount'}))
    validation_result = validate_inventory(nei_flowbyfacility,
                                           nei_national_totals,
                                           group_by=['FlowName'],
                                           tolerance=5.0)
    write_validation_result('NEI', year, validation_result)


def generate_metadata(year, parameters):
    """Get metadata and writes to .json."""
    nei_file_path = _config[year]['file_name']
    source_meta = []
    for file in nei_file_path:
        meta = set_stewi_meta(strip_file_extension(file), EXT_DIR)
        source_meta.append(read_source_metadata(paths, meta, force_JSON=True))
    write_metadata(f'NEI_{year}', source_meta, datatype='inventory',
                   parameters=parameters)


def main(**kwargs):

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Download NEI data and \
                            generate StEWI inventory outputs and validate \
                            to national totals\
                        [B] Download national totals',
                        type = str)

    parser.add_argument('-Y', '--Year', nargs = '+',
                        help = 'What NEI year(s) you want to retrieve',
                        type = str)

    if len(kwargs) == 0:
        kwargs = vars(parser.parse_args())

    for year in kwargs['Year']:
        year = str(year)
        if kwargs['Option'] == 'A':
            nei_point = standardize_output(year)
            nei_point, parameters = (assign_secondary_context(
                nei_point, int(year), 'urb', 'rh', 'concat'))

            log.info('generating flow by facility output')
            nei_flowbyfacility = aggregate(nei_point, ['FacilityID', 'FlowName',
                                                       'Compartment'])
            store_inventory(nei_flowbyfacility, f'NEI_{year}', 'flowbyfacility')
            log.debug(len(nei_flowbyfacility))
            #2017: 2184786
            #2016: 1965918
            #2014: 2057249
            #2011: 1840866

            log.info('generating flow by SCC output')
            nei_flowbyprocess = aggregate(nei_point, ['FacilityID', 'Compartment',
                                                      'FlowName', 'Process'])
            nei_flowbyprocess['ProcessType'] = 'SCC'
            store_inventory(nei_flowbyprocess, f'NEI_{year}', 'flowbyprocess')
            log.debug(len(nei_flowbyprocess))
            #2017: 4055707

            log.info('generating flows output')
            nei_flows = nei_point[['FlowName', 'FlowID', 'Compartment']]
            nei_flows = nei_flows.drop_duplicates()
            nei_flows['Unit'] = 'kg'
            nei_flows = nei_flows.sort_values(by='FlowName', axis=0)
            store_inventory(nei_flows, f'NEI_{year}', 'flow')
            log.debug(len(nei_flows))
            #2017: 293
            #2016: 282
            #2014: 279
            #2011: 277

            log.info('generating facility output')
            facility = nei_point[[f for f in facility_fields
                                  if f in nei_point.columns]]
            facility = facility.drop_duplicates('FacilityID')
            facility = facility.astype({'Zip': 'str'})
            store_inventory(facility, f'NEI_{year}', 'facility')
            log.debug(len(facility))
            #2017: 87162
            #2016: 85802
            #2014: 85125
            #2011: 95565

            generate_metadata(year, parameters)

            if year in ['2011', '2014', '2017', '2020']:
                validate_national_totals(nei_flowbyfacility, year)
            else:
                log.info('no validation performed')

        elif kwargs['Option'] == 'B':
            if year in ['2011', '2014', '2017', '2020']:
                generate_national_totals(year)
            else:
                log.info(f'national totals do not exist for year {year}')


if __name__ == '__main__':
    main(Year=[2019, 2020], Option='A')
