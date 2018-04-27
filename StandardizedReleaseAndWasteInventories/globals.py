#!/usr/bin/env python

import pandas as pd


def url_is_alive(url):
    """
    Checks that a given URL is reachable.
    :param url: A URL
    :rtype: bool
    """
    import urllib
    request = urllib.request.Request(url)
    request.get_method = lambda: 'HEAD'
    try:
        urllib.request.urlopen(request)
        return True
    except urllib.request.HTTPError:
        return False


def download_table(filepath, url):
    import os
    if not os.path.exists(filepath):
        if url[-4:].lower() == '.zip':
            import zipfile
            import requests
            import io
            table_request = requests.get(url).content
            zip_file = zipfile.ZipFile(io.BytesIO(table_request))
            zip_file.extractall(filepath)
        elif 'xls' in url.lower() or url.lower()[-5:] == 'excel':
            import urllib
            import shutil
            with urllib.request.urlopen(url) as response, open(filepath, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
        elif 'json' in url.lower():
            import pandas as pd
            pd.read_json(url).to_csv(filepath, index=False)


# def read_iso_csv(filepath_or_buffer, sep=',', delimiter=None, header='infer', names=None, index_col=None, usecols=None, squeeze=False, prefix=None, mangle_dupe_cols=True, dtype=None, engine='python', converters=None, true_values=None, false_values=None, skipinitialspace=False, skiprows=None, nrows=None, na_values=None, keep_default_na=True, na_filter=True, verbose=False, skip_blank_lines=True, parse_dates=False, infer_datetime_format=False, keep_date_col=False, date_parser=None, dayfirst=False, iterator=False, chunksize=None, compression='infer', thousands=None, decimal=b'.', lineterminator=None, quotechar='"', quoting=0, escapechar=None, comment=None, encoding=None, dialect=None, tupleize_cols=None, error_bad_lines=True, warn_bad_lines=True, skipfooter=0, skip_footer=0, doublequote=True, delim_whitespace=False, as_recarray=None, compact_ints=None, use_unsigned=None, low_memory=True, buffer_lines=None, memory_map=False, float_precision=None):
#     # Try UTF-8 by default, then ISO 8859-1 (i.e. latin1). Cleanup extra characters if read as ISO.
#     try: output_df = pd.read_csv(filepath_or_buffer=filepath_or_buffer, sep=sep, delimiter=delimiter, header=header, names=names, index_col=index_col, usecols=usecols, squeeze=squeeze, prefix=prefix, mangle_dupe_cols=mangle_dupe_cols, dtype=dtype, engine=engine, converters=converters, true_values=true_values, false_values=false_values, skipinitialspace=skipinitialspace, skiprows=skiprows, nrows=nrows, na_values=na_values, keep_default_na=keep_default_na, na_filter=na_filter, verbose=verbose, skip_blank_lines=skip_blank_lines, parse_dates=parse_dates, infer_datetime_format=infer_datetime_format, keep_date_col=keep_date_col, date_parser=date_parser, dayfirst=dayfirst, iterator=iterator, chunksize=chunksize, compression=compression, thousands=thousands, decimal=decimal, lineterminator=lineterminator, quotechar=quotechar, quoting=quoting, escapechar=escapechar, comment=comment, encoding=encoding, dialect=dialect, tupleize_cols=tupleize_cols, error_bad_lines=error_bad_lines, warn_bad_lines=warn_bad_lines, skipfooter=skipfooter, skip_footer=skip_footer, doublequote=doublequote, delim_whitespace=delim_whitespace, as_recarray=as_recarray, compact_ints=compact_ints, use_unsigned=use_unsigned, low_memory=low_memory, buffer_lines=buffer_lines, memory_map=memory_map, float_precision=float_precision)
#     except UnicodeDecodeError:
#         import numpy as np
#         output_df = pd.read_csv(filepath_or_buffer=filepath_or_buffer, sep=sep, delimiter=delimiter, header=header, names=names, index_col=index_col, usecols=usecols, squeeze=squeeze, prefix=prefix, mangle_dupe_cols=mangle_dupe_cols, dtype=dtype, engine=engine, converters=converters, true_values=true_values, false_values=false_values, skipinitialspace=skipinitialspace, skiprows=skiprows, nrows=nrows, na_values=na_values, keep_default_na=keep_default_na, na_filter=na_filter, verbose=verbose, skip_blank_lines=skip_blank_lines, parse_dates=parse_dates, infer_datetime_format=infer_datetime_format, keep_date_col=keep_date_col, date_parser=date_parser, dayfirst=dayfirst, iterator=iterator, chunksize=chunksize, compression=compression, thousands=thousands, decimal=decimal, lineterminator=lineterminator, quotechar=quotechar, quoting=quoting, escapechar=escapechar, comment=comment, encoding="latin1", dialect=dialect, tupleize_cols=tupleize_cols, error_bad_lines=error_bad_lines, warn_bad_lines=warn_bad_lines, skipfooter=skipfooter, skip_footer=skip_footer, doublequote=doublequote, delim_whitespace=delim_whitespace, as_recarray=as_recarray, compact_ints=compact_ints, use_unsigned=use_unsigned, low_memory=low_memory, buffer_lines=buffer_lines, memory_map=memory_map, float_precision=float_precision)
#         del_chars = ''.join(chr(i) for i in list(range(32)) + list(range(127, 256)))
#         trans = str.maketrans(del_chars, ' ' * len(del_chars))
#         for column in output_df.select_dtypes([np.object]):
#             output_df[column] = output_df[column].str.replace('%3Csub%3E', '').str.replace('%3C/sub%3E', '')
#             for i in range(len(output_df[column])):
#                 output_df[column][i] = str(output_df[column][i]).encode("ascii", errors="ignore").decode()
#         for column in output_df: output_df = output_df.rename(columns={column: column.encode("latin1", errors="ignore").decode()})
#     return output_df


def import_table(filepath, skip_lines=0):
    if filepath[-3:].lower() == 'csv':
        # import_file = read_iso_csv(filepath)
        import_file = pd.read_csv(filepath)
    elif 'xls' in filepath[-4:].lower():
        import_file = pd.ExcelFile(filepath)
        import_file = {sheet: import_file.parse(sheet, skiprows=skip_lines) for sheet in import_file.sheet_names}
    return import_file


def get_local_file(filename, data_source=''):
    filepath = data_dir
    if data_source != '': filepath += data_source + '/'
    filepath += filename
    output_file = import_table(filepath)
    return output_file


# def get_data_file(filepath, data_source, url=''):
#     try: output_file = get_local_file(filename=filename, data_source=data_source)
#     except:
#         download_table(url=url, filepath=data_dir + data_source + '/' + filename)
#         output_file = get_local_file(filename=filename, data_source=data_source)
#     return output_file


def drop_excel_sheets(excel_dict, drop_sheets):
    for s in drop_sheets:
        try:
            excel_dict.pop(s)
        except KeyError:
            continue
    return excel_dict


def filter_inventory(inventory_df, criteria_file, filter_type, marker=None):
    """
    :param inventory_df: DataFrame to be filtered
    :param criteria_file: Can be a list of items to drop/keep, or a table of FlowName, FacilityID, etc. with columns marking rows to drop
    :param filter_type: drop, keep, mark_drop, mark_keep
    :param marker: Non-empty fields are considered marked by default. Option to specify 'x', 'yes', '1', etc.
    :return: DataFrame
    """
    if type(inventory_df) == pd.core.frame.DataFrame: output_df = inventory_df
    else:
        print('The exclude_params function only accepts CSV (*.csv), JSON (*.json), and DataFrames as input.')
        return
    criteria_table = globals.import_table(criteria_file)
    if filter_type in ('drop', 'keep'):
        for criteria_column in criteria_table:
            for column in output_df:
                if column == criteria_column:
                    criteria = set(criteria_table[criteria_column])
                    if filter_type == 'drop': output_df = output_df[~output_df[column].isin(criteria)]
                    elif filter_type == 'keep': output_df = output_df[output_df[column].isin(criteria)]
    elif filter_type in ('mark_drop', 'mark_keep'):
        standard_format = import_table('StandardizedReleaseAndWasteInventories/data/Standarized_Output_Format_EPA _Data_Sources.csv')
        must_match = standard_format['Name'][standard_format['Name'].isin(criteria_table.keys())]
        for criteria_column in criteria_table:
            if criteria_column in must_match: continue
            for field in must_match:
                if filter_type == 'mark_drop':
                    if marker is None: output_df = output_df[~output_df[field].isin(criteria_table[field][criteria_table[criteria_column] != ''])]
                    else: output_df = output_df[~output_df[field].isin(criteria_table[field][criteria_table[criteria_column] == marker])]
                if filter_type == 'mark_keep':
                    if marker is None: output_df = output_df[output_df[field].isin(criteria_table[field][criteria_table[criteria_column] != ''])]
                    else: output_df = output_df[output_df[field].isin(criteria_table[field][criteria_table[criteria_column] == marker])]
    return output_df.reset_index(drop=True)


def set_dir(directory_name):
    import os
    path1 = './' + directory_name + '/'
    path2 = 'StandardizedReleaseAndWasteInventories/' + directory_name + '/'
    if os.path.exists(path1): pathname = path1
    elif os.path.exists(path2): pathname = path2
    else:
        pathname = path1
        os.makedirs(pathname)
    return pathname


# Convert amounts. Note this could be replaced with a conversion utility
def unit_convert(df, coln1, coln2, unit, conversion_factor, coln3):
    df[coln1][df[coln2] == unit] = conversion_factor * df[coln3]
    return df


global output_dir
global data_dir
global reliability_table
output_dir = set_dir('output')
data_dir = set_dir('data')
reliability_table = pd.read_csv(data_dir + 'DQ_Reliability_Scores_Table3-3fromERGreport.csv',
                                usecols=['Source', 'Code', 'DQI Reliability Score'])
US_States_withDC = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
                    'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
                    'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
                    'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
                    'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
