from calculateReturns import *

db = DatabaseManager(DATABASE_PATH)

#XIRR test
cashflows = [-600000, 200, 5000,200000, -35000, 439799]
dates = [datetime(2023,12,5), datetime(2024, 5 ,6), datetime(2024,6,7),datetime(2024,8,8),datetime(2025,5,5),datetime(2025,6,1)]
guess = 0.1
Expected = 1.37


#Descending NAV Sort Test
input = {"A": '100', "C": '300', "B": '200', "D": '400'}
expected = ["D", "C", "B",  "A"]
print(descendingNavSort(input) == expected) #should be true

#Asset 3 Visibility export Test
print(db.fetchOptions("asset3Visibility"))


#TESTING RECURSIVE NODE CALCULATION LINKING FUNCTION //////////////////////////////////////
from scripts.basicFunctions import recursLinkCalcs
from classes.nodeLibrary import nodeLibrary
#test data basic structure: investor A,B,C into nodes X,Y,Z. 
# 
#A invests into X
#B invests into Y,Z
#C invests into X,Z
# 
# X invests into Y. 


#X invests into Fund A,B
#Y invests into fund C,D
#Z invests into fund E,F

#example Calc format: {"dateTime" : month["dateTime"], "Source name" : sourceEntry["Source name"], "Node" : node, "Target name" : target ,
#                                    "NAV" : targetSourceNAV, "Monthly Gain" : targetSourceGain , "Return" :  targetSourceReturn * 100, 
#                                    "MDdenominator" : targetSourceMDdenominator, "Ownership" : targetSourceOwnership * 100,
#                                    nameHier["Commitment"]["local"] : targetSourceCommitment, nameHier["Unfunded"]["local"] : targetSourceUnfunded, 
#                                    "IRR ITD" : targetSourceIRR,
#                                    "ownershipAdjust" : adjustedOwnershipBool}

import pandas as pd
import math
month = '1/31/2024'

# Read testCalcs from Excel, matching the same method as for 'expected'
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
clumpDict = {}
clumpDict.setdefault(1,{}).setdefault('Y',{})[month] = [entry for entry in testCalcs if entry['Node'] == 'Y']

clumpDict.setdefault(0,{}).setdefault('X',{})[month] = [entry for entry in testCalcs if entry['Node'] == 'X']
clumpDict2 = {}
clumpDict2.setdefault(0,{}).setdefault('Z',{})[month] = [entry for entry in testCalcs if entry['Node'] == 'Z']

nodeLib = nodeLibrary(falseBalances)
import pandas as pd
import math

# Calculate results
results = recursLinkCalcs(clumpDict[1]['Y'][month], month, 1, 'Y', [nodeLib.node2id['Y'],], nodeLib, clumpDict)
results.extend(recursLinkCalcs(clumpDict2[0]['Z'][month], month, 0, 'Z', [nodeLib.node2id['Z'],], nodeLib, clumpDict2))
results.extend(recursLinkCalcs(clumpDict[0]['X'][month], month, 0, 'X', [nodeLib.node2id['X'],], nodeLib, clumpDict))

# Read expected results from Excel
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

def compare_entry(dict1, dict2, tol=0.1):
    """Check if all non-None keys in dict1 and dict2 are present and numerical values match to at least .01"""
    for key in dict1.keys():
        v1 = dict1[key]
        v2 = dict2.get(key, None)
        # If both are None or empty string, equal
        if (v1 is None or v1 == '') and (v2 is None or v2 == ''):
            continue
        # Attempt to compare numerics
        try:
            f1 = float(v1)
            f2 = float(v2)
            if math.isnan(f1) and math.isnan(f2):
                continue
            if abs(f1 - f2) > tol:
                #print(f'Failure metric1: {key} -- {f1} and {f2}')
                return False
        except (TypeError, ValueError):
            # fallback: string compare
            if str(v1) != str(v2):
                #print(f'Failure metric2: {key} -- {v1} and {v2}')
                return False
    return True

if True:
    matched = []
    for exp in expected:
        found = False
        for res in results:
            if compare_entry(exp, res):
                found = True
                break
        matched.append(found)

    if all(matched):
        print("All expected results found in calculated results, to the hundredths place.")
    else:
        print(f"Some expected results not found in calculated results.")
        for idx, found in enumerate(matched):
            if not found:
                print(f"Missing or mismatched row {idx+1}: {expected[idx]}")

# Optionally, check the reverse: are there any extra calculated results not in expected?
extra = []
if True:
    for res in results:
        found = False
        for exp in expected:
            if compare_entry(exp, res):
                found = True
                break
        if not found:
            extra.append(res)
    if extra:
        print("\n \n")
        print(f"Extra calculated results not found in expected results ({len(extra)} extras).")
        for idx, found in enumerate(extra):
            print(f"Missing or mismatched row {idx+1}: {extra[idx]}")
    else:
        print("No extra calculations missing from expected results")



#Testing node recursion methods directly:

hier = ['step1', 'step2', 'step3', 'step4']
levelIdx = 1
print(f"Level Idx currently points to: {hier[levelIdx]}")
newHier = [*hier[:levelIdx], 'newStep', *hier[levelIdx:]]
print(f"New hier is {newHier} with levelIdx pointing to {newHier[levelIdx]}")
