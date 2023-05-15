# TRI.py (stewi)
# !/usr/bin/env python3
# coding=utf-8
"""
Downloads TRI Basic Plus files specified in paramaters for specified year
This file requires parameters be passed like:
    Option -Y Year -F File1 File2 ... FileN
    where Option is either A, B, C:
Option:
    A - for downloading and extracting files from TRI Data Plus web site
    B - for organizing TRI National Totals files from TRI_chem_release_Year.csv
    (this is expected to be download before and to be organized as it is
    described in TRI.py).
    C - for generating StEWI output files and validation from downloaded data
Files:
    1a - Releases and Other Waste Mgmt
    3a - Off Site Transfers
See more documentation of files at
https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-guides

Year:
    2008 - 2021

"""

import requests
import zipfile
from bs4 import BeautifulSoup
import pandas as pd
import time
import io
import argparse
import re
from pathlib import Path

from esupy.processed_data_mgmt import read_source_metadata
from stewi.globals import unit_convert, DATA_PATH, set_stewi_meta,\
    get_reliability_table_for_source, write_metadata, url_is_alive,\
    lb_kg, g_kg, config, store_inventory, log, paths, compile_source_metadata,\
    aggregate, assign_secondary_context, concat_compartment
from stewi.validate import update_validationsets_sources, validate_inventory,\
    write_validation_result
import stewi.exceptions


EXT_DIR = 'TRI Data Files'
OUTPUT_PATH = Path(paths.local_path).joinpath(EXT_DIR)
_config = config()['databases']['TRI']
TRI_DATA_PATH = DATA_PATH / 'TRI'


def visit(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    return soup


def link_zip(url, queries, year):
    soup = visit(url)
    TRI_zip_options = {}
    for link in soup.find_all(queries['TRI_year_reported']):
        TRI_zip_options[link.text] = link.get(queries['TRI_zip'])
    return TRI_zip_options[year]


def extract_TRI_data_files(link_zip, files, year):
    r_file = requests.get(link_zip)
    for file in files:
        df_columns = pd.read_csv(TRI_DATA_PATH
                                 .joinpath(f'TRI_File_{file}_columns.txt'),
                                 header=0)
        columns = list(df_columns['Names'])
        filename = f'US_{file}_{year}'
        dic = {}
        i = 0
        with zipfile.ZipFile(io.BytesIO(r_file.content)) as z:
            with io.TextIOWrapper(z.open(filename + '.txt', mode='r'),
                                  errors='replace') as txtfile:
                for line in txtfile:
                    dic[i] = pd.Series(re.split("\t", line)).truncate(after=len(columns)-1)
                    i+=1
        # remove the first row in the dictionary which is the original headers
        del dic[0]
        df = pd.DataFrame.from_dict(dic, orient='index')
        df.columns = columns
        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_PATH.joinpath(f'{filename}.csv'), index=False)
        log.info(f'{filename}.csv saved to {OUTPUT_PATH}')


def generate_national_totals(year):
    """Generate dataframe of national emissions and save to csv.

    Requires the chem_release dataset to be downloaded manually prior to running
    """
    filename = TRI_DATA_PATH.joinpath(f'TRI_chem_release_{year}.csv')
    df = pd.read_csv(filename, header=0)
    if 'CAS Number' not in df:
        raise stewi.exceptions.DataNotFoundError(
            message=f'Validation not available for TRI data in {year}')
    df = df.replace(',', 0.0).replace('.', 0.0)
    compartments = {'air': ['Fugitive Air Emissions',
                            'Point Source Air Emissions'],
                    'water': ['Surface Water Discharges'],
                    'soil': ['On-site Land Treatment',
                             'Other On-site Land Disposal',
                             'Off-site Land Treatment',
                             'Other Off-site Land Disposal']}
    # remove entries where all values are 0
    v = [col for col in df.columns if col not in ['Chemical', 'CAS Number']]
    df = df.loc[~(df[v] == 0).all(axis=1)]
    df_National = pd.DataFrame(columns=['Compartment', 'FlowName',
                                        'CAS', 'Unit', 'FlowAmount'])
    for compartment, columns in compartments.items():
        df_aux = df[['Chemical', 'CAS Number'] + columns].reset_index(drop=True)
        for column in columns:
            df_aux[column] = df_aux[column].str.replace(',', '').astype('float')
        df_aux = (df_aux
                  .assign(FlowAmount=lambda x: x[columns].sum(axis=1))
                  .assign(Unit='Pounds')
                  .assign(Compartment=compartment)
                  .rename(columns={'Chemical': 'FlowName',
                                   'CAS Number': 'CAS'})
                  .drop(columns=columns)
                  )
        df_National = pd.concat([df_National, df_aux], axis=0,
                                ignore_index=True)
        del df_aux
    del df
    df_National['FlowAmount'] = df_National['FlowAmount'].round(3)
    if df_National is None:
        log.warning('Totals not generated')
        return
    df_National = df_National.sort_values(by=['FlowName', 'Compartment'])
    log.info(f'saving TRI_{year}_NationalTotals.csv to {DATA_PATH}')
    df_National.to_csv(DATA_PATH.joinpath(f'TRI_{year}_NationalTotals.csv'),
                       index=False)

    # Update validationSets_Sources.csv
    date_created = time.strptime(time.ctime(filename.stat().st_ctime))
    date_created = time.strftime('%d-%b-%Y', date_created)
    validation_dict = {'Inventory': 'TRI',
                       #'Version': '',
                       'Year': year,
                       'Name': 'TRI Explorer',
                       'URL': 'https://enviro.epa.gov/triexplorer/tri_release.chemical',
                       'Criteria': 'Year, All of United States, All Chemicals, '
                       'All Industries, Details:(Other On-Site Disposal or '
                       'Other Releases, Other Off-Site Disposal or Other Releases)',
                       'Date Acquired': date_created,
                       }
    update_validationsets_sources(validation_dict, date_acquired=True)


def imp_fields(fname):
    """
    Import list of fields from TRI that are desired for LCI.
    """
    tri_req_fields = pd.read_csv(fname, header=None)
    tri_req_fields = list(tri_req_fields[0])
    return tri_req_fields


def concat_req_field(list):
    """
    Import in pieces grabbing main fields plus unique amount and basis
    of estimate fields assigns fields to variables
    """
    source_name = ['TRIFID','CHEMICAL NAME', 'CAS NUMBER',
                   'UNIT OF MEASURE'] + list
    return source_name


# Cycle through file importing by release type, the dictionary key
def import_TRI_by_release_type(d, year):
    # Import TRI file
    tri_release_output_fieldnames = ['FacilityID', 'CAS', 'FlowName',
                                     'Unit', 'FlowAmount', 'Basis of Estimate',
                                     'ReleaseType']
    df_list = []
    for k, v in d.items():
        dtype_dict = {'TRIFID': "str",
                      'CHEMICAL NAME': "str",
                      'CAS NUMBER': "str",
                      'UNIT OF MEASURE': "str",
                      }
        dtype_dict[v[4]] = "float64" # FlowAmount field
        if len(v) > 5:
            dtype_dict[v[5]] = "str" # Basis of Estimate field
        if k == 'offsiteland' or k == 'offsiteother':
            file = '3a'
        else:
            file = '1a'
        tri_csv = OUTPUT_PATH.joinpath(f'US_{file}_{year}.csv')
        try:
            tri_part = pd.read_csv(tri_csv, usecols=v,
                                   low_memory=False,
                                   dtype=dtype_dict)
            tri_part['ReleaseType'] = k
            tri_part.columns = tri_release_output_fieldnames
            tri_part = (
                tri_part
                .dropna(subset=['FlowAmount'])
                .assign(**{"Basis of Estimate": lambda x: 
                           x['Basis of Estimate'].str.strip()})
                .query('FlowAmount != 0')
            )
            df_list.append(tri_part)
        except FileNotFoundError:
            log.error(f'{file}.csv file not found in {tri_csv}')
    if len(df_list) == 0:
        raise stewi.exceptions.DataNotFoundError
    return pd.concat(df_list, ignore_index=True)


def validate_national_totals(inv, TRIyear):
    log.info('validating data against national totals')
    filename = DATA_PATH.joinpath(f'TRI_{TRIyear}_NationalTotals.csv')
    if filename.is_file():
        tri_national_totals = pd.read_csv(filename, header=0,
                                          dtype={"FlowAmount": float,
                                                 "CAS": str})
        tri_national_totals['FlowAmount_kg'] = 0
        tri_national_totals = unit_convert(tri_national_totals, 'FlowAmount_kg',
                                           'Unit', 'Pounds', lb_kg, 'FlowAmount')
        # drop old amount and units; rename cols to match reference format
        tri_national_totals = (tri_national_totals
                               .drop(columns=['FlowAmount', 'Unit'])
                               .rename(columns={'FlowAmount_kg': 'FlowAmount'}))
        inv = inv.assign(CAS=lambda x: x['CAS'].str.replace('-', ''))
        if inv is not None:
            validation_result = validate_inventory(inv, tri_national_totals,
                                                   group_by=['CAS', 'Compartment'],
                                                   tolerance=5.0)
            write_validation_result('TRI', TRIyear, validation_result)
    else:
        log.warning(f'validation file for TRI_{TRIyear} does not exist. '
                    'Please run option B')


def generate_TRI_files_csv(TRIyear):
    """
    Generate TRI inventories from downloaded files.
    :param TRIyear: str
    """
    tri_required_fields = imp_fields(TRI_DATA_PATH.joinpath('TRI_required_fields.txt'))
    keys = imp_fields(TRI_DATA_PATH.joinpath('TRI_keys.txt'))
    values = list()
    for p in range(len(keys)):
        start = 13 + 2*p
        end = start + 1
        values.append(concat_req_field(tri_required_fields[start: end + 1]))
    # Create dict of required fields on import for each release type
    import_dict = dict(zip(keys, values))
    # Build the TRI DataFrame
    tri = import_TRI_by_release_type(import_dict, TRIyear)
    # Import reliability scores for TRI
    tri = (pd.merge(tri, get_reliability_table_for_source('TRI'),
                    left_on='Basis of Estimate',
                    right_on='Code', how='left')
             .drop(columns=['Basis of Estimate', 'Code']))
    tri['DQI Reliability Score'] = tri['DQI Reliability Score'].fillna(value=5)
    # Replace source info with Context
    tri = pd.merge(tri, pd.read_csv(
        TRI_DATA_PATH.joinpath('TRI_ReleaseType_to_Compartment.csv')),
                   how='left')
    # Convert units to ref mass unit of kg
    tri['Amount_kg'] = 0.0
    tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Pounds', lb_kg, 'FlowAmount')
    tri = unit_convert(tri, 'Amount_kg', 'Unit', 'Grams', g_kg, 'FlowAmount')
    tri = (tri.drop(columns=['FlowAmount', 'Unit', 'ReleaseType'])
              .rename(columns={'Amount_kg': 'FlowAmount',
                               'DQI Reliability Score': 'DataReliability'}))

    # FACILITY - import and handle TRI facility data
    TRI_facility_name_crosswalk = {
        'TRIFID': ['FacilityID', 'str'],
        'FACILITY NAME': ['FacilityName', 'str'],
        'FACILITY STREET': ['Address', 'str'],
        'FACILITY CITY': ['City', 'str'],
        'FACILITY COUNTY': ['County', 'str'],
        'FACILITY STATE': ['State', 'str'],
        'FACILITY ZIP CODE': ['Zip', 'str'],
        'PRIMARY NAICS CODE': ['NAICS', 'str'],
        'LATITUDE': ['Latitude', 'float64'],
        'LONGITUDE': ['Longitude', 'float64'],
        }
    tri_facility = (pd.read_csv(OUTPUT_PATH.joinpath(f'US_1a_{TRIyear}.csv'),
                                usecols=TRI_facility_name_crosswalk.keys(),
                                low_memory=False,
                                dtype={k:v[1] for k, v in 
                                       TRI_facility_name_crosswalk.items()})
                      .drop_duplicates(ignore_index=True)
                      .rename(columns={k:v[0] for k, v in 
                                       TRI_facility_name_crosswalk.items()})
                      )
    tri_facility, parameters = assign_secondary_context(
        tri_facility, int(TRIyear), 'urb')
    store_inventory(tri_facility, f'TRI_{TRIyear}', 'facility')

    if 'urban_rural' in parameters:  # given urban/rural assignment success
        # merge & concat urban/rural into tri.Compartment before aggregation
        tri = tri.merge(tri_facility[['FacilityID', 'UrbanRural']].drop_duplicates(),
                        how='left', on='FacilityID')
        tri = concat_compartment(tri, True, 'urb')  # passes has_geo_pkgs=True

    tri = aggregate(tri, ['FacilityID', 'FlowName', 'CAS', 'Compartment'])
    validate_national_totals(tri, TRIyear)

    # FLOWS
    flows = (tri[['FlowName', 'CAS', 'Compartment']]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(FlowID=lambda x: x['CAS']))
    store_inventory(flows, f'TRI_{TRIyear}', 'flow')

    # FLOW BY FACILITY
    fbf = tri.drop(columns=['CAS'])
    store_inventory(fbf, f'TRI_{TRIyear}', 'flowbyfacility')
    return parameters


def generate_metadata(year, files, parameters=None, datatype='inventory'):
    """Get metadata and writes to .json."""
    if datatype == 'source':
        source_path = [str(OUTPUT_PATH.joinpath(f'US_{p}_{year}.csv')) for p in files]
        source_meta = compile_source_metadata(source_path, _config, year)
        source_meta['SourceType'] = 'Zip file'
        tri_url = _config['url']
        link_zip_TRI = link_zip(tri_url, _config['queries'], year)
        regex = 'https.*/(.*(?=/\w*.zip))'
        # tri_version = link_zip_TRI.split('/')[-2]
        try:
            tri_version = re.search(regex, link_zip_TRI).group(1)
        except AttributeError: # no match found from regex
            tri_version = 'last'
        source_meta['SourceVersion'] = tri_version
        write_metadata(f'TRI_{year}', source_meta, category=EXT_DIR,
                       datatype='source')
    else:
        source_meta = read_source_metadata(paths, set_stewi_meta(f'TRI_{year}',
                                                                 EXT_DIR),
                                           force_JSON=True)['tool_meta']
        write_metadata(f'TRI_{year}', source_meta, datatype=datatype,
                       parameters=parameters)


def main(**kwargs):

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A] Download and TRI flat files from TRI Data Plus.\
                        [B] Format national totals for TRI from download \
                        national files.\
                        [C] Generate StEWI inventory files from downloaded files',
                        type = str)

    parser.add_argument('-Y', '--Year', nargs = '+',
                        help = 'What TRI year you want to retrieve',
                        type = str)

    parser.add_argument('-F', '--Files', nargs = '+',
                        help = 'What TRI Files you want (e.g., 1a, 2a, etc).\
                        Check:\
                        https://www.epa.gov/toxics-release-inventory-tri-program/\
                            tri-basic-plus-data-files-guides',
                        default = ['1a','3a'],
                        required = False)

    if len(kwargs) == 0:
        kwargs = vars(parser.parse_args())

    files = kwargs.get('Files', ['1a', '3a'])

    for year in kwargs['Year']:
        year = str(year)
        if kwargs['Option'] == 'A':
            log.info('downloading TRI files from source for %s', year)
            tri_url = _config['url']
            if url_is_alive(tri_url):
                # link_zip_TRI = link_zip(tri_url, _config['queries'], year)
                link_zip_TRI = _config.get('zip_url').replace("{year}", year)
                extract_TRI_data_files(link_zip_TRI, files, year)
                generate_metadata(year, files, datatype='source')
            else:
                log.error('The URL in config.yaml ({}) for TRI is not '
                          'reachable.'.format(tri_url))

        elif kwargs['Option'] == 'B':
            # Website for National Totals
            # https://enviro.epa.gov/triexplorer/tri_release.chemical
            # Steps:
            # (1) Select Year of Data, All of United States, All Chemicals,
            # All Industry, CAS Number, 
            # and other needed option (this is based on the desired year)
            # Columns: check 'Other On-site Disposal or Other Releases' and
            # 'Other Off-site Disposal or Other Releases'
            # (2) Export to CSV
            # (3) Drop the not needed rows, including the extra dioxin row
            # at the bottom
            # (4) Organize the columns as they are needed (check existing files)
            # (5) Save the file like TRI_chem_release_year.csv in data folder
            # (6) Run this code

            generate_national_totals(year)

        elif kwargs['Option'] == 'C':
            log.info(f'generating TRI inventory from files for {year}')
            parameters = generate_TRI_files_csv(year)
            generate_metadata(year, files, parameters, datatype='inventory')


if __name__ == '__main__':
    main(Option='C', Year=[2021])
