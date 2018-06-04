#!/usr/bin/env python

import pandas as pd
import json
import os


global output_dir
global data_dir
global reliability_table

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'stewi/'

output_dir = modulepath + 'output/'
data_dir = modulepath + 'data/'

reliability_table = pd.read_csv(data_dir + 'DQ_Reliability_Scores_Table3-3fromERGreport.csv',
                                usecols=['Source', 'Code', 'DQI Reliability Score'])

inventory_metadata = {
'SourceType': 'Static File',  #Other types are "Web service"
'SourceFileName':'NA',
'SourceURL':'NA',
'SourceVersion':'NA',
'SourceAquisitionTime':'NA',
'StEWI_versions_version': '0.9'
}


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


def set_dir(directory_name):
    path = 'stewi/' + directory_name + '/'
    if os.path.exists(path): pathname = path
    else:
        pathname = path
        os.makedirs(pathname)
    return pathname


def import_table(path_or_reference, skip_lines=0):
    if '.core.frame.DataFrame' in str(type(path_or_reference)): import_file = path_or_reference
    elif path_or_reference[-3:].lower() == 'csv':
        # import_file = read_iso_csv(filepath)
        import_file = pd.read_csv(path_or_reference)
    elif 'xls' in path_or_reference[-4:].lower():
        import_file = pd.ExcelFile(path_or_reference)
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


def filter_inventory(inventory, criteria_table, filter_type, marker=None):
    """
    :param inventory_df: DataFrame to be filtered
    :param criteria_file: Can be a list of items to drop/keep, or a table of FlowName, FacilityID, etc. with columns
                          marking rows to drop
    :param filter_type: drop, keep, mark_drop, mark_keep
    :param marker: Non-empty fields are considered marked by default. Option to specify 'x', 'yes', '1', etc.
    :return: DataFrame
    """
    inventory = import_table(inventory); criteria_table = import_table(criteria_table)
    if filter_type in ('drop', 'keep'):
        for criteria_column in criteria_table:
            for column in inventory:
                if column == criteria_column:
                    criteria = set(criteria_table[criteria_column])
                    if filter_type == 'drop': inventory = inventory[~inventory[column].isin(criteria)]
                    elif filter_type == 'keep': inventory = inventory[inventory[column].isin(criteria)]
    elif filter_type in ('mark_drop', 'mark_keep'):
        standard_format = import_table(data_dir + 'flowbyfacility_format.csv')
        must_match = standard_format['Name'][standard_format['Name'].isin(criteria_table.keys())]
        for criteria_column in criteria_table:
            if criteria_column in must_match: continue
            for field in must_match:
                if filter_type == 'mark_drop':
                    if marker is None: inventory = inventory[~inventory[field].isin(criteria_table[field][criteria_table[criteria_column] != ''])]
                    else: inventory = inventory[~inventory[field].isin(criteria_table[field][criteria_table[criteria_column] == marker])]
                if filter_type == 'mark_keep':
                    if marker is None: inventory = inventory[inventory[field].isin(criteria_table[field][criteria_table[criteria_column] != ''])]
                    else: inventory = inventory[inventory[field].isin(criteria_table[field][criteria_table[criteria_column] == marker])]
    return inventory.reset_index(drop=True)


def filter_states(inventory_df, include_states=True, include_dc=True, include_territories=False):
    states_df = pd.read_csv(data_dir + 'state_codes.csv')
    states_filter = pd.DataFrame()
    states_list = []
    if include_states: states_list += list(states_df['states'].dropna())
    if include_dc: states_list += list(states_df['dc'].dropna())
    if include_territories: states_list += list(states_df['territories'].dropna())
    states_filter['State'] = states_list
    output_inventory = filter_inventory(inventory_df, states_filter, filter_type='keep')
    return output_inventory


def validate_inventory(inventory_df, reference_df, group_by='emission', tolerance=5.0):
    """
    Compare inventory resulting from script output with a reference DataFrame from another source
    :param inventory_df: DataFrame of inventory resulting from script output
    :param reference_df: Reference DataFrame to compare emission quantities against. Must have same keys as inventory_df
    :param group_by: 'emission' for species summed across facilities, 'facility' to check species by facility,
                      or 'overall' for summed mass of all species
    :param tolerance: Maximum acceptable percent difference between inventory and reference values
    :return: DataFrame containing 'Conclusion' of statistical comparison and 'Percent_Difference'
    """
    import numpy as np
    if pd.api.types.is_string_dtype(inventory_df['Amount']):
        inventory_df['Amount'] = inventory_df['Amount'].str.replace(',', '')
        inventory_df['Amount'] = pd.to_numeric(inventory_df['Amount'])
    if pd.api.types.is_string_dtype(reference_df['Amount']):
        reference_df['Amount'] = reference_df['Amount'].str.replace(',', '')
        reference_df['Amount'] = pd.to_numeric(reference_df['Amount'])
    if group_by == 'overall':
        inventory_sums = inventory_df['Amount'].sum()
        reference_sums = reference_df['Amount'].sum()
    else:
        if group_by == 'emission': group_by_columns = ['FlowName']
        elif group_by == 'facility': group_by_columns = ['FlowName', 'FacilityID']
        inventory_df = inventory_df.fillna(-np.pi)
        reference_df = reference_df.fillna(-np.pi)
        inventory_sums = inventory_df[group_by_columns + ['Amount']].groupby(group_by_columns).sum().reset_index()
        reference_sums = reference_df[group_by_columns + ['Amount']].groupby(group_by_columns).sum().reset_index()
    validation_df = inventory_sums.merge(reference_sums, how='outer', on=group_by_columns)
    amount_x_list = []
    amount_y_list = []
    pct_diff_list = []
    conclusion = []
    for index, row in validation_df.iterrows():
        amount_x = float(row['Amount_x'])
        amount_y = float(row['Amount_y'])
        if amount_x == -np.pi:
            amount_x_list.append(np.nan)
            if amount_y == -np.pi:
                pct_diff_list.append(0.0)
                amount_y_list.append(np.nan)
                conclusion.append('Both inventory and reference are null')
            else:
                amount_y_list.append(amount_y)
                if amount_y == 0.0:
                    pct_diff_list.append(0.0)
                    conclusion.append('Inventory is null, reference is zero')
                else:
                    pct_diff_list.append(100.0)
                    conclusion.append('Emission missing from inventory')
            continue
        elif amount_x == 0.0:
            if amount_y == 0.0:
                pct_diff_list.append(0.0)
                conclusion.append('Identical')
            else:
                pct_diff_list.append(100.0)
                conclusion.append('Inventory value is zero')
            amount_x_list.append(amount_x)
            amount_y_list.append(amount_y)
            continue
        if amount_y == -np.pi:
            amount_x_list.append(amount_x)
            amount_y_list.append(np.nan)
            if amount_x == 0.0:
                pct_diff_list.append(0.0)
                conclusion.append('Inventory is zero, reference is null')
            else:
                pct_diff_list.append(100.0)
                conclusion.append('Emission not found in reference')
            continue
        elif amount_y == 0.0:
            pct_diff_list.append(100.0)
            conclusion.append('Reference value is zero')
            amount_x_list.append(amount_x)
            amount_y_list.append(amount_y)
            continue
        pct_diff = 100.0 * abs(amount_y - amount_x) / amount_y
        pct_diff_list.append(pct_diff)
        amount_x_list.append(amount_x)
        amount_y_list.append(amount_y)
        if pct_diff == 0.0: conclusion.append('Identical')
        elif pct_diff <= tolerance: conclusion.append('Statistically similar')
        elif pct_diff > tolerance: conclusion.append('Percent difference exceeds tolerance')
    validation_df['Inventory_Amount'] = amount_x_list
    validation_df['Reference_Amount'] = amount_y_list
    validation_df['Percent_Difference'] = pct_diff_list
    validation_df['Conclusion'] = conclusion
    validation_df = validation_df.drop(['Amount_x', 'Amount_y'], axis=1)
    return validation_df


def validation_summary(validation_df):
    """
    Summarized output of validate_inventory function
    :param validation_df:
    :return: DataFrame containing 'Count' of each statistical conclusion and 'Avg_Pct_Difference'
    """
    validation_df['Count'] = validation_df['Conclusion']
    validation_summary_df = validation_df[['Count', 'Conclusion']].groupby('Conclusion').count()
    validation_summary_df['Avg_Pct_Difference'] = validation_df[['Percent_Difference', 'Conclusion']].groupby('Conclusion').mean()
    validation_summary_df.reset_index(inplace=True)
    return validation_summary_df


# Convert amounts. Note this could be replaced with a conversion utility
def unit_convert(df, coln1, coln2, unit, conversion_factor, coln3):
    df[coln1][df[coln2] == unit] = conversion_factor * df[coln3]
    return df


#Writes the metadata dictionary to a JSON file
def write_metadata(inventoryname,report_year, metadata_dict):
    with open(output_dir + inventoryname + '_' + report_year + '_metadata.json', 'w') as file:
        file.write(json.dumps(metadata_dict))


#Returns the metadata dictionary for an inventory
def read_metadata(inventoryname,report_year):
    with open(output_dir + 'RCRAInfo_' + report_year + '_metadata.json', 'r') as file:
        file_contents = file.read()
        metadata = json.loads(file_contents)
        return metadata


def get_required_fields(format='flowbyfacility'):
    fields = pd.read_csv(data_dir + format + '_format.csv')
    required_fields = fields[fields['required?'] == 1]
    required_fields = dict(zip(required_fields['Name'], required_fields['Type']))
    return required_fields


def get_optional_fields(format='flowbyfacility'):
    fields = pd.read_csv(data_dir + format + '_format.csv')
    optional_fields = fields[fields['required?'] == 0]
    optional_fields = dict(zip(optional_fields['Name'], optional_fields['Type']))
    return optional_fields

def checkforFile(filepath):
    if os.path.exists(filepath):
        return True
    else:
        return False

