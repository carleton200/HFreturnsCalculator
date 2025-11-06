from scripts.importList import *

currentVersion = "1.1.12"
demoMode = True
remoteDBmode = False
ownershipCorrect = True
importInterval = relativedelta(hours=3)
calculationPingTime = 2
ownershipFlagTolerance = 0.001

dynamoAPIenvName = "Dynamo_API"
mainURL = "https://api.dynamosoftware.com/api/v2.2"

nameHier = {
                "Family Branch" : {"api" : "Parent investor", "dynHigh" : "Parentinvestor", "local" : "Family Branch"},
                "Unfunded" : {"api" : "Remaining commitment change", "dynLow" : "RemainingCommitmentChange", "local" : "Unfunded", "value" : "CashFlowSys"},
                "Commitment" : {"api" : "Amount" , "dynLow" : "ValueInSystemCurrency", "local" : "Commitment"},
                "Transaction Time" : {"dynLow" : "TransactionTiming"},
                "sleeve" : {"sleeve" : "sleeve", "fund" : "Name", "local" : "subAssetSleeve"},
                "CashFlow" : {"dynLow" : "CashFlowSys", "dynHigh" : "CashFlowSys"}, 
                "Value" : {"local" : "NAV", "api" : "Value in system currency", "dynLow" : "ValueInSystemCurrency", "dynHigh" : "ValueInSystemCurrency"},
                "Classification" : {"local" : "Classification" , "dynLow" : "Target nameExposureHFClassificationLevel2"},
                "FundClass" : {"dynLow" : "Fundclass" , "dynHigh" : "Fundclass"},
                "subClassification" : {"local" : "HF Sub-Classification", "dynLow" : "Target nameExposureHFClassificationLevel2ExposureHFClassificationLevel3", "dynHigh" : "Target nameExposureHFClassificationLevel2ExposureHFClassificationLevel3"}
            }
mainTableNames = ["positions_low", "positions_high", "transactions_low", "transactions_high"]
#TODO: make this database stored variable later
assetClass1Order = ["Illiquid", "Liquid","Cash"]
assetClass2Order = ["Direct Private Equity", "Private Equity", "Direct Real Assets", "Real Assets", "Public Equity", "Long/Short", "Absolute Return", "Fixed Income", "Cash"] 
commitmentChangeTransactionTypes = ["Commitment", "Transfer of commitment", "Transfer of commitment (out)", "Secondary - Original commitment (by secondary seller)"]
ignoreInvTranTypes = [""]
headerOptions = ["Return","NAV", "Monthly Gain", "Ownership" , "MDdenominator", "Commitment", "Unfunded", "%"]
if not demoMode:
    headerOptions.append("IRR ITD")
dataOptions = ["Investor","Family Branch","Classification", "dateTime"]
tranAppHeaderOptions = ["Transaction Sum"]
tranAppDataOptions = ["Investor","Family Branch", "dateTime"]
assetLevelLinks = {1: {"Display" : "Asset Level 1", "Link" : "assetClass"}, 
                    2: {"Display" : "Asset Level 2", "Link" : "subAssetClass"}, 
                    3: {"Display" : "Asset Level 3", "Link" : "subAssetSleeve"},
                    0 : {"Display" : "Total Portfolio" , "Link" : "Total"},
                    -1 : {"Link" : "Family Branch"}}
displayLinks = {"assetClass" : "Asset Level 1", "subAssetClass" : "Asset Level 2" , "subAssetSleeve" : "Asset Level 3"}
tableNameKey = {1 : {0 : "positions_low", 1 : "positions_high"}, 0 : {0 : "transactions_low", 1 : "transactions_high"}}
for link in displayLinks.copy(): #builds out in reverse so it can work both ways
    displayLinks[displayLinks.get(link)] = link
balanceTypePriority = ["Actual", "Adjusted", "Manager Estimate"]
yearOptions = (1,2,3,5,7,10,12,15,20)

timeOptions = ["MTD","QTD","YTD", "ITD", "IRR ITD"] + [f"{y}YR" for y in yearOptions]
percent_headers = {option for option in timeOptions}
for header in ("Return","Ownership"):
    percent_headers.add(header)

if remoteDBmode:
    sqlPlaceholder = "%s"
else:
    sqlPlaceholder = "?"