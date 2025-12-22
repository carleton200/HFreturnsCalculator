from scripts.basicFunctions import recursLinkCalcs
from classes.nodeLibrary import nodeLibrary
import pandas as pd
import math

def testData():
    month = '1/31/2024'
    # Read nodeCalcs from Excel, matching the same method as for 'expected'
    excel_path = "Data_Testing_Excels/Recursion_Data_Test.xlsx"
    df_test = pd.read_excel(excel_path, sheet_name='Node_Calcs', header=0, nrows=15, usecols='A:M')
    test_headers = list(df_test.columns)
    # remove any empty column names (from trailing blank columns)
    test_headers = [h for h in test_headers if isinstance(h, str) and h.strip() != ""]
    # Clean data: convert NaN to None for consistency
    testCalcs = []
    for idx, row in df_test.iterrows():
        row_dict = {}
        for key in test_headers:
            value = row[key]
            # use None for nan
            if isinstance(value, float) and (math.isnan(value)):
                row_dict[key] = None
            else:
                row_dict[key] = value
        row_dict['dateTime'] = month
        for field in ('Return', 'Ownership', 'IRR ITD'):
            if row_dict.get(field) is not None:
                row_dict[field] = row_dict[field] * 100
        testCalcs.append(row_dict)

    clumpDict = {}
    clumpDict.setdefault(1,{}).setdefault('Y',{})[month] = [entry for entry in testCalcs if entry['Node'] == 'Y']

    clumpDict.setdefault(0,{}).setdefault('X',{})[month] = [entry for entry in testCalcs if entry['Node'] == 'X']
    clumpDict2 = {}
    clumpDict2.setdefault(0,{}).setdefault('Z',{})[month] = [entry for entry in testCalcs if entry['Node'] == 'Z']


    # Read expected full calculations from Excel
    excel_path = "Data_Testing_Excels/Recursion_Data_Test.xlsx"
    df = pd.read_excel(excel_path, sheet_name='Linked_Calcs', header=0, nrows=15, usecols='A:M')
    headers = list(df.columns)
    # remove any empty column names (from trailing blank columns)
    headers = [h for h in headers if isinstance(h, str) and h.strip() != ""]
    # Clean data: convert NaN to None for consistency with Python dicts
    expected = []
    for idx, row in df.iterrows():
        data = {}
        for key in headers:
            val = row[key]
            if pd.isna(val):
                val = None
            data[key] = val
        data['dateTime'] = month
        for field in ('Return', 'Ownership', 'IRR ITD'):
            if data.get(field) is not None:
                data[field] = data[field] * 100
        expected.append(data)

    falseBalances = [
                        {'Source name' : 'A', 'Target name' : 'X'},
                        {'Source name' : 'C', 'Target name' : 'X'},
                        {'Source name' : 'C', 'Target name' : 'Z'},
                        {'Source name' : 'B', 'Target name' : 'Y'},
                        {'Source name' : 'B', 'Target name' : 'Z'},
                        {'Source name' : 'X', 'Target name' : 'Y'},
                        {'Source name' : 'X', 'Target name' : 'Fund A'},
                        {'Source name' : 'X', 'Target name' : 'Fund B'},
                        {'Source name' : 'Y', 'Target name' : 'Fund C'},
                        {'Source name' : 'Y', 'Target name' : 'Fund D'},
                        {'Source name' : 'Z', 'Target name' : 'Fund E'},
                        {'Source name' : 'Z', 'Target name' : 'Fund F'},
                    ]
    nodeLib = nodeLibrary(falseBalances)

    return {'nodeCalcs': testCalcs, 'calculations' : expected}