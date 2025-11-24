from scripts.importList import *
from scripts.commonValues import *
from scripts.basicFunctions import *

def processPool(poolData : dict,selfData : dict, statusQueue, _, failed, transactionCalc: bool = False):
    #Function to take all the information for one pool, calculate all relevant information, and return a list of the calculations
    #Inputs:
    #   poolData: dict with information relevant to this specific pool
    #   selfData: dict with information common to every pool
    #   statusQueue: a multiprocessing Manager queue for all worker threads to send progress bar and status updates. Minimizes database wait time
    #   dbQueue: a multiprocessing manager queue for worker threads to send final database updates to allow the worker to complete and not block the database
    #   failed: a multiprocessing variable. Begins negative. If any worker flags it as true, all workers will see it and halt if they hit the failure checkpoint
    try:
        noCalculations = selfData.get("noCalculations") #boolean of whether or not previous calculations exist to pull from
        months = selfData.get("months") #list of pre-prepared data for each month
        fundList = selfData.get("fundList") #list of funds/investments and some accompanying data (such as asset class level 3)
        calculations = []
        earliestChangeDate = poolData.get("earliestChangeDate") #earliest date for new data from last API pull
        pool = poolData.get("poolName")
        cache = poolData.get("cache") #dataset of all relevant transactions and account balances for the pool
        if not cache:
            print(f"No data found for pool {pool}, so skipping calculations")
            logging.warning(f"No data found for pool {pool}, so skipping calculations")
            statusQueue.put((pool,1,"Completed")) #allows the completion of calculations
            return [], {}
        newMonths = []

        if not noCalculations: #if there are calculations, find all months before the data pull, and then pull those calculations
            for month in months:
                #if the calculations for the month have already been complete, pull the old data
                if earliestChangeDate > datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S"):
                    calculations.extend(cache.get("calculations", {}).get(month["dateTime"], []))
                else:
                    newMonths.append(month)
        else:
            newMonths = months #check all months if there are no previous calculations
        IRRtrack = {} #dict of each fund's cash flows and dates for IRR calculation
        IRRinvestorTrack = {} #dict of each investor's cash flows and dates for IRR calculation
        if transactionCalc: #run transaction app calculations
            for month in newMonths: #loops through every month relevant to the pool
                statusQueue.put((pool,len(newMonths),"Working")) #puts to queue to update loading bar status. Allows computations to continue
                if failed.value: #if other workers failed, halt the process
                    print(f"Exiting worker {pool} due to other failure...")
                    return []
                poolCashFlow = 0
                lowTransactions = cache.get("transactions_low", {}).get(month["dateTime"], [])
                for transaction in lowTransactions: #get fund data, cash flows, and commitment alterations
                    if transaction["TransactionType"] not in commitmentChangeTransactionTypes and transaction[nameHier["CashFlow"]["dynLow"]] not in (None, "None"):
                        poolCashFlow -= float(transaction[nameHier["CashFlow"]["dynLow"]])
                investorPoolCashFlow = 0
                transactions = cache.get("transactions_high", {}).get(month["dateTime"], []) #all cashflow and commitment based transactions for investors into the pool for the month
                for tran in transactions:
                    if tran["TransactionType"] not in commitmentChangeTransactionTypes and tran[nameHier["CashFlow"]["dynHigh"]] not in (None, "None"):
                        investorPoolCashFlow -= float(tran[nameHier["CashFlow"]["dynHigh"]])
                difference = round(poolCashFlow - investorPoolCashFlow,2) * -1
                if difference != 0:
                    monthPoolEntry = {"dateTime" : month["dateTime"], "Pool" : pool, 
                                            "Transaction Sum" : difference,
                    }
                    calculations.append(monthPoolEntry) #append to calculations for use in report generation and aggregation

                #end of months loop
            statusQueue.put((pool,len(newMonths),"Completed")) #push completed status update to the main thread
            return calculations
        for month in newMonths: #loops through every month relevant to the pool
            monthFundIRRtrack = {}
            statusQueue.put((pool,len(newMonths),"Working")) #puts to queue to update loading bar status. Allows computations to continue
            if failed.value: #if other workers failed, halt the process
                print(f"Exiting worker {pool} due to other failure...")
                return [], {}
            totalDays = int(datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S").day  - datetime.strptime(month["tranStart"], "%Y-%m-%dT%H:%M:%S").day) + 1 #total days in month for MD den
            poolFunds = cache.get("positions_low", {}).get(month["dateTime"], []) #account balances for the pool
            #find MD denominator for each investor
            #find total gain per pool
            funds = []
            fundNames = []
            startEntries = {}
            endEntries = {}
            for account in poolFunds: #finds all fund starting and ending balances for the month
                if account["Target name"] not in fundNames:
                    fundNames.append(account["Target name"])
                    funds.append({"fundName" : account["Target name"], "hidden" : False})
                if account["Date"] == month["accountStart"]:
                    if account["Target name"] not in startEntries:
                        startEntries[account["Target name"]] = [account,]
                    else:
                        startEntries[account["Target name"]].append(account)
                elif account["Date"] == month["endDay"]:
                    if account["Target name"] not in endEntries:
                        endEntries[account["Target name"]] = [account,]
                    else:
                        endEntries[account["Target name"]].append(account)

            hiddenFunds = cache.get("transactions_low", {}).get(month["dateTime"], [])
            #funds that do not have account positions but are relevant to the pool (ex: deferred liabilities)
            allPoolTransactions = {}
            for transaction in hiddenFunds:
                if transaction["Target name"] not in fundNames:
                    fundNames.append(transaction["Target name"])
                    funds.append({"fundName" : transaction["Target name"], "hidden" : True})
                if transaction["Target name"] not in allPoolTransactions:
                    allPoolTransactions[transaction["Target name"]] = [transaction,]
                else:
                    allPoolTransactions[transaction["Target name"]].append(transaction)
            poolGainSum = 0
            poolNAV = 0
            poolMDdenominator = 0
            poolWeightedCashFlow = 0
            poolCashFlow = 0
            fundEntryList = []
            for fundDict in funds: #iterate through all funds to find the pool NAV and MD den
                fund = fundDict["fundName"]
                if fund in (None,'None'):
                    continue
                assetClass = None
                subAssetClass = None
                fundClassification = None
                fundSubClassification = None
                startEntryCache = startEntries.get(fund, [])
                endEntryCache = endEntries.get(fund, [])
                startEntry = copy.deepcopy(startEntryCache)
                endEntry = copy.deepcopy(endEntryCache)
                createFinalValue = False
                noStartValue = False
                if len(startEntry) < 1: #no start value, so NAV = 0
                    startEntry = [{nameHier["Value"]["dynLow"] : 0}]  #nameHier is a dictionary for common references to specific names. 
                    noStartValue = True
                    commitment = 0
                    unfunded = 0
                else: #instantiate starting data
                    assetClass = startEntry[0]["ExposureAssetClass"]
                    subAssetClass = startEntry[0]["ExposureAssetClassSub-assetClass(E)"]
                    fundClassification = startEntry[0]["Target nameExposureHFClassificationLevel2"]
                    fundSubClassification = startEntry[0].get(nameHier["subClassification"]["dynLow"])
                    commitment = float(startEntry[0].get(nameHier["Commitment"]["local"],0))
                    unfunded = float(startEntry[0].get(nameHier["Unfunded"]["local"],0))
                if len(startEntry) > 1: #combines the values for fund sub classes for calculations
                    split = {}
                    foundDuplicate = False
                    for entry in startEntry: #split the entries by fundclass to check for duplicates
                        fundSubKey = ""
                        fundClass = entry.get(nameHier["FundClass"]["dynLow"])
                        subAccount = entry.get('InvestsThrough')
                        for key in (fundClass, subAccount):
                            if key is not None:
                                fundSubKey += key
                        if fundSubKey not in split:
                            split[fundSubKey] = [entry,]
                        else:
                            split[fundSubKey].append(entry)
                            foundDuplicate = True
                    singleEntries = []
                    if foundDuplicate: #if duplicates, loop through to find the best balance type
                        for fundKeyEntries in split: #loop by fund
                            if len(split.get(fundKeyEntries)) > 1: #check if duplicates
                                foundType = False
                                for balanceType in balanceTypePriority: #loop through balance types by priority
                                    for entry in split.get(fundKeyEntries): #loop through the duplicate entries
                                        if entry.get("Balancetype") == balanceType and entry.get(nameHier["Value"]["dynLow"]) not in (None,"None"): #if the balance type is preferred, add the entry and break
                                            singleEntries.append(entry)
                                            foundType = True
                                            break
                                    if foundType: #stop balance type checking if found
                                        break
                                if not foundType: #reaches if nothing was found
                                    for entry in split.get(fundKeyEntries): #loop through to find the first with a value
                                        if entry.get(nameHier["Value"]["dynLow"]) not in (None,"None"): #if the balance type is preferred, add the entry and break
                                            singleEntries.append(entry)
                                            foundType = True
                                            break
                                    if not foundType: #final attempt just take the first entry
                                        singleEntries.append(split.get(fundKeyEntries)[0])
                            else: #no duplicates for this fund
                                singleEntries.append(split.get(fundKeyEntries)[0])
                    else:
                        singleEntries.extend(startEntry)
                    NAV = 0
                    for entry in singleEntries:
                        if entry.get(nameHier["Value"]["dynLow"]) not in (None,0,"None"):
                            NAV += float(entry[nameHier["Value"]["dynLow"]]) #adds values to the first index
                    startEntry[0][nameHier["Value"]["dynLow"]] = str(NAV)
                if len(endEntry) < 1: #no end account balance yet, so create it.  
                    # !!!!!!!!! Should I remove this and just not use the fund if an end balance is not there?
                    createFinalValue = True
                    endEntry = [{nameHier["Value"]["dynLow"] : 0}]
                elif assetClass is None or subAssetClass is None or fundClassification is None: #first of several attempts to find the fund information.
                    assetClass = endEntry[0]["ExposureAssetClass"]
                    subAssetClass = endEntry[0]["ExposureAssetClassSub-assetClass(E)"]
                    fundClassification = endEntry[0]["Target nameExposureHFClassificationLevel2"]
                    fundSubClassification = endEntry[0].get(nameHier["subClassification"]["dynLow"])
                if len(endEntry) > 1: #combine sub funds for calculations
                    split = {}
                    foundDuplicate = False
                    for entry in endEntry: #split the entries by fundclass to check for duplicates
                        fundSubKey = ""
                        fundClass = entry.get(nameHier["FundClass"]["dynLow"])
                        subAccount = entry.get('InvestsThrough')
                        for key in (fundClass, subAccount):
                            if key is not None:
                                fundSubKey += key
                        if fundSubKey not in split:
                            split[fundSubKey] = [entry,]
                        else:
                            split[fundSubKey].append(entry)
                            foundDuplicate = True
                    singleEntries = []
                    if foundDuplicate: #if duplicates, loop through to find the best balance type
                        for fundKeyEntries in split: #loop by fund
                            if len(split.get(fundKeyEntries)) > 1: #check if duplicates
                                foundType = False
                                for balanceType in balanceTypePriority: #loop through balance types by priority
                                    for entry in split.get(fundKeyEntries): #loop through the duplicate entries
                                        if entry.get("Balancetype") == balanceType and entry.get(nameHier["Value"]["dynLow"]) not in (None,"None"): #if the balance type is preferred, add the entry and break
                                            singleEntries.append(entry)
                                            foundType = True
                                            break
                                    if foundType: #stop balance type checking if found
                                        break
                                if not foundType: #reaches if nothing was found
                                    for entry in split.get(fundKeyEntries): #loop through to find the first with a value
                                        if entry.get(nameHier["Value"]["dynLow"]) not in (None,"None"): #if the balance type is preferred, add the entry and break
                                            singleEntries.append(entry)
                                            foundType = True
                                            break
                                    if not foundType: #final attempt just take the first entry
                                        singleEntries.append(split.get(fundKeyEntries)[0])
                            else: #no duplicates for this fund
                                singleEntries.append(split.get(fundKeyEntries)[0])
                    else:
                        singleEntries.extend(endEntry)
                    NAV = 0
                    for entry in singleEntries:
                        if entry.get(nameHier["Value"]["dynLow"]) not in (None,0,"None"):
                            NAV += float(entry.get(nameHier["Value"]["dynLow"])) #adds values to the first index
                    endEntry[0][nameHier["Value"]["dynLow"]] = str(NAV)
                startEntry = startEntry[0]
                if startEntry.get(nameHier["Value"]["dynLow"]) == 0:
                    noStartValue = True
                endEntry = endEntry[0]
                fundTransactions = allPoolTransactions.get(fund,[]) 
                cashFlowSum = 0
                weightedCashFlow = 0
                if noStartValue: #if the fund was not activate at BOM, find the active days
                    activeDays = 0
                    for transaction in (tran for tran in fundTransactions if tran[nameHier["CashFlow"]["dynLow"]] not in (None, "None",0.0)):
                        backDate = calculateBackdate(transaction, noStartValue)
                        activeDays = max(activeDays,totalDays - int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day) + backDate)
                    dayCalcDenominator = activeDays
                else:
                    dayCalcDenominator = totalDays
                for transaction in fundTransactions: #get fund data, cash flows, and commitment alterations
                    if assetClass is None or assetClass == "None":
                        assetClass = transaction["SysProp_FundTargetNameAssetClass(E)"]
                    if subAssetClass is None or subAssetClass == "None":
                        subAssetClass = transaction["SysProp_FundTargetNameSub-assetClass(E)"]
                    if fundClassification is None or fundClassification == "None":
                        fundClassification = transaction["Target nameExposureHFClassificationLevel2"]
                    if transaction["TransactionType"] not in commitmentChangeTransactionTypes and transaction[nameHier["CashFlow"]["dynLow"]] not in (None, "None"):
                        cashFlowSum -= float(transaction[nameHier["CashFlow"]["dynLow"]])
                        backDate = calculateBackdate(transaction, noStartValue) #Uses dynamo transaction time logic to decide to subtract one day or not
                        weightedCashFlow -= float(transaction[nameHier["CashFlow"]["dynLow"]])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day) + backDate)/dayCalcDenominator if dayCalcDenominator != 0.0 else 0.0
                        if transaction.get(nameHier["Unfunded"]["dynLow"]) not in (None,"None"):
                            unfunded += float(transaction[nameHier["Unfunded"]["value"]])
                        if fund not in IRRtrack:
                            IRRtrack[fund] = {"cashFlows" : [], "dates" : []}
                        IRRtrack[fund]["cashFlows"].append(float(transaction[nameHier["CashFlow"]["dynLow"]]))
                        IRRtrack[fund]["dates"].append(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S") - relativedelta(days=backDate))
                        if fund not in monthFundIRRtrack:
                            monthFundIRRtrack[fund] = {"cashFlows" : [], "dates" : []}
                        monthFundIRRtrack[fund]["cashFlows"].append(float(transaction[nameHier["CashFlow"]["dynLow"]]))
                        monthFundIRRtrack[fund]["dates"].append(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S") - relativedelta(days=backDate))
                    elif transaction["TransactionType"] in commitmentChangeTransactionTypes and transaction.get("TransactionType") not in (None,"None"):
                        commitment += float(transaction[nameHier["Commitment"]["dynLow"]])
                        unfunded += float(transaction[nameHier["Commitment"]["dynLow"]])
                try:
                    if startEntry[nameHier["Value"]["dynLow"]] in (None, "None"):
                        startEntry[nameHier["Value"]["dynLow"]] = 0
                    if endEntry[nameHier["Value"]["dynLow"]] in (None, "None"):
                        endEntry[nameHier["Value"]["dynLow"]] = 0
                    if createFinalValue:
                        #implies there is no gain (Cash account with no interest?)
                        endEntry[nameHier["Value"]["dynLow"]] = float(startEntry[nameHier["Value"]["dynLow"]]) + cashFlowSum    
                    fundGain = (float(endEntry[nameHier["Value"]["dynLow"]]) - float(startEntry[nameHier["Value"]["dynLow"]]) - cashFlowSum)
                    fundMDdenominator = float(startEntry[nameHier["Value"]["dynLow"]]) + weightedCashFlow
                    fundNAV = float(endEntry[nameHier["Value"]["dynLow"]])
                    fundReturn = abs(fundGain/fundMDdenominator * 100) * findSign(fundGain) if fundMDdenominator != 0 else 0
                    if fund in IRRtrack:
                        IRRitd = calculate_xirr([*IRRtrack[fund]["cashFlows"], fundNAV], [*IRRtrack[fund]["dates"], datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S")])
                    else:
                        IRRitd = None
                    if unfunded < 0:
                        unfunded = 0 #corrects for if original commitment was not logged properly
                    if createFinalValue: #builds an entry to put into the database and cache if it is missing
                        fundEOMentry = {"Date" : month["endDay"], "Source name" : pool, "Target name" : fund , nameHier["Value"]["dynLow"] : endEntry[nameHier["Value"]["dynLow"]],
                                            "Balancetype" : "Calculated_R", "ExposureAssetClass" : assetClass, "ExposureAssetClassSub-assetClass(E)" : subAssetClass,
                                            nameHier["Commitment"]["local"] : commitment, nameHier["Unfunded"]["local"] : unfunded,
                                            nameHier["sleeve"]["local"] : fundList.get(fund,None), nameHier["Classification"]["dynLow"] : fundClassification}
                        # update cache for subsequent months
                        for m in newMonths:
                            if m["accountStart"] <= month["endDay"] <= m["endDay"]:
                                cache.setdefault("positions_low", {}).setdefault(m["dateTime"], []).append(fundEOMentry)
                    else: #update database and cache with the calculated commitment, unfunded, and sleeve (asset lvl 3)
                        # update cache for all months referencing this date
                        for m in newMonths:
                            if m["accountStart"] <= month["endDay"] <= m["endDay"]:
                                for lst in cache.get("positions_low", {}).get(m["dateTime"], []):
                                    if lst["Target name"] == fund and lst["Date"] == month["endDay"]:
                                        lst[nameHier["Commitment"]["local"]] = commitment
                                        lst[nameHier["Unfunded"]["local"]] = unfunded
                                        lst[nameHier["sleeve"]["local"]] = fundList.get(fund)
                    #sum each fund value into the pool totals
                    poolGainSum += fundGain
                    poolMDdenominator += fundMDdenominator
                    poolNAV += fundNAV
                    poolCashFlow += cashFlowSum
                    poolWeightedCashFlow += weightedCashFlow
                    monthFundEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Fund", "Pool" : pool, "Fund" : fund ,
                                    "assetClass" : assetClass, "subAssetClass" : subAssetClass,
                                    "NAV" : fundNAV, "Monthly Gain" : fundGain, "Return" : fundReturn , 
                                    "MDdenominator" : fundMDdenominator, "Ownership" : "", "Classification" : fundClassification,
                                    "Calculation Type" : "Total Fund",
                                    "IRR ITD" : IRRitd,
                                    nameHier["sleeve"]["local"] : fundList.get(fund),
                                    nameHier["Commitment"]["local"] : commitment,
                                    nameHier["Unfunded"]["local"] : unfunded,
                                    nameHier["subClassification"]["local"] : fundSubClassification
                                    }
                    if fund not in (None,"None"): #removing blank funds (found duplicate of Monogram in 'HF Direct Investments Pool, LLC - PE (2021)' with most None values)
                        calculations.append(monthFundEntry) #append to calculations for use in report generation and aggregation
                        fundEntryList.append(monthFundEntry) #fund data stored on its own for investor calculations


                except Exception as e:
                    print(f"Skipped fund {fund} for {pool} in {month["Month"]} because: {e} {e.args}")
                    #Testing flag. skips fund if the values are zero and cause an error
            if poolNAV == 0 and poolCashFlow == 0:
                #skips the pool if there is no cash flow or value in the pool
                continue
            poolReturn = abs(poolGainSum/poolMDdenominator * 100) * findSign(poolGainSum) if poolMDdenominator != 0 else 0
            monthPoolEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Pool", "Pool" : pool, "Fund" : None ,
                            "assetClass" : poolData.get("assetClass"), "subAssetClass" : poolData.get("subAssetClass") ,
                            "NAV" : poolNAV, "Monthly Gain" : poolGainSum, "Return" : poolReturn , "MDdenominator" : poolMDdenominator,
                                "Ownership" : None, "Calculation Type" : "Total Fund"} 
                            #generic pool data for investors calculations
            investorStartEntries = {}
            investorEndEntries = {}
            investorPositions = cache.get("positions_high", {}).get(month["dateTime"], []) #account balances for investors into the pool for the month
            for pos in investorPositions: #find start and end entries for each investor and sort them
                investor = pos["Source name"]
                if pos["Date"] == month["accountStart"]:
                    if investor not in investorStartEntries:
                        investorStartEntries[investor] = [pos,]
                    else:
                        investorStartEntries[investor].append(pos)
                if pos["Date"] == month["endDay"]:
                    if investor not in investorEndEntries:
                        investorEndEntries[investor] = [pos,]
                    else:
                        investorEndEntries[investor].append(pos)

            allInvestorTransactions = {}
            transactions = cache.get("transactions_high", {}).get(month["dateTime"], []) #all cashflow and commitment based transactions for investors into the pool for the month
            for tran in transactions: #sort by investor
                investor = tran["Source name"]
                if investor not in allInvestorTransactions:
                    allInvestorTransactions[investor] = [tran,]
                else:
                    allInvestorTransactions[investor].append(tran)


            investorMDdenominatorSum = 0
            tempInvestorDicts = {}
            poolOwnershipSum = 0
            for investor in set(investorStartEntries.keys()) | set(investorEndEntries.keys()) | set(allInvestorTransactions.keys()): 
                #iterate through each investor in the pool for the month
                #pool level loop for investors
                investorWeightedCashFlow = 0
                investorCashFlowSum = 0
                tempInvestorDict = {}
                startEntry_cache = investorStartEntries.get(investor)
                if startEntry_cache: #use starting entry
                    if len(startEntry_cache) > 1:
                        # Choose the balance where Balancetype is the highest of the list, otherwise just the first
                        type_precedence = ["Calculated_R", "Actual", "Adjusted"] # Define type precedence
                        # Sort entries by type precedence and then fall back to first
                        def type_rank(entry):
                            btype = entry.get("Balancetype", "")
                            if btype in type_precedence:
                                return type_precedence.index(btype)
                            else:
                                return len(type_precedence)
                        # Get the entry with the minimum rank
                        startEntry = sorted(startEntry_cache, key=type_rank)[0]
                    else:
                        startEntry = startEntry_cache[0]
                    noStartValue = False
                else: #if no starting entry, take necessary variables and zero out the value
                    end_cache = investorEndEntries.get(investor)
                    if end_cache:
                        startEntry = copy.deepcopy(end_cache[0])
                        startEntry[nameHier["Value"]["dynHigh"]] = 0
                        noStartValue = True
                    else:
                        continue #ignore the investor completely if there is no starting or ending value
                if startEntry.get(nameHier["Value"]["dynHigh"]) in (None,"None"):
                    startEntry[nameHier["Value"]["dynHigh"]] = 0 #prevent float conversion errors
                investorTransactions = allInvestorTransactions.get(investor,[]) #all investor transactions in the pool for the month
                
                for transaction in investorTransactions: 
                    if transaction.get(nameHier["CashFlow"]["dynHigh"]) not in (None,"None"):
                        investorCashFlowSum -= float(transaction[nameHier["CashFlow"]["dynHigh"]])
                        backDate = calculateBackdate(transaction) #dynamo revert by a day logic
                        investorWeightedCashFlow -= float(transaction[nameHier["CashFlow"]["dynHigh"]])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day) + backDate)/totalDays
                investorMDdenominator = float(startEntry[nameHier["Value"]["dynHigh"]]) + investorWeightedCashFlow
                tempInvestorDict["MDden"] = investorMDdenominator
                tempInvestorDict["cashFlow"] = investorCashFlowSum
                tempInvestorDict["startVal"] = float(startEntry[nameHier["Value"]["dynHigh"]])
                tempInvestorDict["ExposureAssetClass"] = startEntry["ExposureAssetClass"]
                tempInvestorDict["ExposureAssetClassSub-assetClass(E)"] = startEntry["ExposureAssetClassSub-assetClass(E)"]
                tempInvestorDict[nameHier["Family Branch"]["local"]] = startEntry[nameHier["Family Branch"]["dynHigh"]]
                if tempInvestorDict["startVal"] == 0 and investorCashFlowSum == 0:
                    continue #ignore investors with no value
                EOM = investorEndEntries.get(investor,[])
                if len(EOM) > 0:
                    if EOM[0].get(nameHier["Value"]["dynHigh"]) in (None,"None"):
                        EOM[0][nameHier["Value"]["dynHigh"]] = 0
                if round(tempInvestorDict.get("startVal") + tempInvestorDict.get("cashFlow")) != 0 and len(EOM) > 0 and round(float(EOM[0].get(nameHier["Value"]["dynHigh"],0))) != 0:
                    #only accounts for investor gain (MD den) if they have not exited
                    #exit check: starting value + cashflow is zero OR there is no ending value
                    investorMDdenominatorSum += investorMDdenominator
                tempInvestorDicts[investor] = tempInvestorDict #store investor calculations for secondary iteration for fund level data
            monthPoolEntryInvestorList = [] #stores investor data for third iteration (not needed to be split, but remnant from old logic.)
            ownershipAdjustDict = {}
            for investor in tempInvestorDicts.keys():
                # second investor iteration to find the gain, return,ownership, and NAV values at pool level (i think it is not needed to be split, but remnant from old logic.)
                EOMcheck = investorEndEntries.get(investor,[])
                if len(EOMcheck) > 0:
                    if EOMcheck[0].get(nameHier["Value"]["dynHigh"]) in (None,"None"):
                        EOMcheck[0][nameHier["Value"]["dynHigh"]] = 0 #prevents float conversion errors
                investorMDdenominator = tempInvestorDicts[investor]["MDden"]
                if investorMDdenominatorSum == 0:
                    investorGain = 0 #0 if no true value in the pool. avoids errors
                else:
                    investorGain = poolGainSum * investorMDdenominator / investorMDdenominatorSum
                if investorMDdenominator == 0:
                    investorReturn = 0 #0 if investor has no value in pool. avoids error
                else:
                    investorReturn = abs(investorGain / investorMDdenominator) * findSign(investorGain)
                if round(tempInvestorDicts[investor]["startVal"] + tempInvestorDicts[investor]["cashFlow"]) == 0 or len(EOMcheck) == 0 or round(float(EOMcheck[0].get(nameHier["Value"]["dynHigh"],0))) == 0: 
                    #zero values if exited investor
                    #exit check: start value and cashflow sums to zero OR no end value OR end value is zero
                    investorEOM = 0
                    investorGain = 0
                    investorMDdenominator = 0
                    investorReturn = 0
                else:
                    investorEOM = tempInvestorDicts[investor]["startVal"] + tempInvestorDicts[investor]["cashFlow"] + investorGain
                monthPoolEntryInvestor = copy.deepcopy(monthPoolEntry) #uses pool data as template
                monthPoolEntryInvestor["Investor"] = investor
                monthPoolEntryInvestor[nameHier["Family Branch"]["local"]] = tempInvestorDicts[investor][nameHier["Family Branch"]["local"]]
                monthPoolEntryInvestor["NAV"] = investorEOM
                monthPoolEntryInvestor["Monthly Gain"] = investorGain
                monthPoolEntryInvestor["Return"] = investorReturn * 100
                monthPoolEntryInvestor["MDdenominator"] = investorMDdenominator
                ownershipPerc = investorEOM/poolNAV * 100 if poolNAV != 0 else 0
                monthPoolEntryInvestor["Ownership"] = ownershipPerc
                poolOwnershipSum += ownershipPerc
                monthPoolEntryInvestorList.append([monthPoolEntryInvestor, EOMcheck])
            adjustedOwnershipBool = abs(poolOwnershipSum - 100) > ownershipFlagTolerance #boolean for if ownership is adjusted. Tolerance for thousandth of a percent off
            for investorEntry, EOMcheck in monthPoolEntryInvestorList:
                investor = investorEntry["Investor"]
                investorEOM = investorEntry["NAV"]
                investorOwnership = investorEntry["Ownership"] * 100 /  poolOwnershipSum if poolOwnershipSum != 0 and ownershipCorrect else investorEntry["Ownership"]
                if len(EOMcheck) > 0: #only update the database for the investor if they have account balances
                    #update cache for the following month's calculations
                    if round(float(EOMcheck[0].get(nameHier["Value"]["dynHigh"],0))) != round(investorEOM): #don't push an update if the values are the same
                        for m in newMonths:
                            if m["accountStart"] <= month["endDay"] <= m["endDay"]: #access the both the current month and next month
                                for lst in cache.get("positions_high", {}).get(m["dateTime"], []):
                                    if lst["Source name"] == investor and lst["Target name"] == pool and lst["Date"] == month["endDay"]:
                                        #access the EOM current month and BOM next month as endDay hits both of those
                                        lst[nameHier["Value"]["dynHigh"]] = investorEOM #this does not represent adjusted values
                                        lst["Balancetype"] = "Calculated_R"
                #final (3rd) investor level iteration to use the pool level results for the investor to calculate the fund level information
                for fundEntry in fundEntryList:
                    fund = fundEntry["Fund"]
                    fundInvestorNAV = investorOwnership / 100 * fundEntry["NAV"]
                    fundInvestorGain = fundEntry["Monthly Gain"] / monthPoolEntry["Monthly Gain"] * investorEntry["Monthly Gain"] if monthPoolEntry["Monthly Gain"] != 0 else 0
                    fundInvestorMDdenominator = investorEntry["MDdenominator"] / monthPoolEntry["MDdenominator"] * fundEntry["MDdenominator"] if monthPoolEntry["MDdenominator"] != 0 else 0
                    fundInvestorReturn = abs(fundInvestorGain / fundInvestorMDdenominator) * findSign(fundInvestorGain) if fundInvestorMDdenominator != 0 else 0
                    fundInvestorOwnership = fundInvestorNAV /  fundEntry["NAV"] if fundEntry["NAV"] != 0 else 0
                    #account for commitment calculations on closed funds
                    tempFundOwnership = fundInvestorOwnership if fundInvestorOwnership != 0 else investorOwnership / 100
                    fundInvestorCommitment = fundEntry[nameHier["Commitment"]["local"]] * tempFundOwnership 
                    fundInvestorUnfunded = fundEntry[nameHier["Unfunded"]["local"]] * tempFundOwnership

                    if investorEntry["MDdenominator"] != 0 and investorMDdenominatorSum != 0: 
                        #only run IRR data if there is investor value
                        if investor not in IRRinvestorTrack:
                            IRRinvestorTrack[investor] = {}
                        if fund not in IRRinvestorTrack[investor]:
                            IRRinvestorTrack[investor][fund] = {"cashFlows" : [], "dates" : []}
                        cashflows = monthFundIRRtrack.get(fundEntry["Fund"], {}).get("cashFlows", [])
                        dates = monthFundIRRtrack.get(fundEntry["Fund"], {}).get("dates", [])
                        for cashflow, date in zip(cashflows, dates):
                            adjustedCashflow = cashflow * investorEntry["MDdenominator"] / investorMDdenominatorSum #ratio the cashflow to their MDdenominator
                            IRRinvestorTrack[investor][fund]["cashFlows"].append(adjustedCashflow)
                            IRRinvestorTrack[investor][fund]["dates"].append(date)
                    if investorEntry["Investor"] in IRRinvestorTrack and fundEntry["Fund"] in IRRinvestorTrack[investorEntry["Investor"]]:
                        fundInvestorIRR = calculate_xirr([*IRRinvestorTrack[investorEntry["Investor"]][fund]["cashFlows"], fundInvestorNAV], [*IRRinvestorTrack[investorEntry["Investor"]][fund]["dates"], datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S")])
                    else:
                        fundInvestorIRR = None
                    monthFundInvestorEntry = {"dateTime" : month["dateTime"], "Investor" : investorEntry["Investor"], "Pool" : pool, "Fund" : fundEntry["Fund"] ,
                                    "assetClass" : fundEntry["assetClass"], "subAssetClass" : fundEntry["subAssetClass"],
                                    "NAV" : fundInvestorNAV, "Monthly Gain" : fundInvestorGain , "Return" :  fundInvestorReturn * 100, 
                                    "MDdenominator" : fundInvestorMDdenominator, "Ownership" : fundInvestorOwnership * 100,
                                    "Classification" : fundEntry["Classification"], nameHier["Family Branch"]["local"] : investorEntry[nameHier["Family Branch"]["local"]],
                                    nameHier["Commitment"]["local"] : fundInvestorCommitment, nameHier["Unfunded"]["local"] : fundInvestorUnfunded, 
                                    "Calculation Type" : "Total Fund",
                                    "IRR ITD" : fundInvestorIRR,
                                    nameHier["sleeve"]["local"] : fundList.get(fund),
                                    "ownershipAdjust" : adjustedOwnershipBool,
                                    nameHier["subClassification"]["local"] : fundEntry[nameHier["subClassification"]["local"]]
                                    }
                    calculations.append(monthFundInvestorEntry) #add fund level data to calculations for use in aggregation and report generation
            #end of months loop
        #commands to add database updates to the queues
        dynTables = {}
        
        for table in mainTableNames:
            dynTables[table] = []
            if "positions_" in table: #removes duplicates by requiring a balance key
                uniqueBalances = {accountBalanceKey(entry): entry for monthL in cache.get(table, {}) for entry in cache.get(table, {}).get(monthL, [])}
                dynTables[table].extend([entry for _,entry in uniqueBalances.items()])
            else:
                for monthL in cache.get(table, {}).keys():
                    dynTables[table].extend(cache.get(table, {}).get(monthL, []))
        statusQueue.put((pool,len(newMonths),"Completed")) #push completed status update to the main thread
        return calculations, dynTables
    except Exception as e: #halt operations for failure or force close/cancel
        statusQueue.put((pool,len(newMonths),"Failed"))
        print(f"Worker for {poolData.get("poolName")} failed.")
        failed = True
        try:
            trace = traceback.format_exc()
            print(trace)
            logging.error(trace)
        except:
            pass
        logging.error(e)
        print("\n")
        return [], {}
