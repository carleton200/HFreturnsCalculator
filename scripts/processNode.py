from collections import defaultdict
from datetime import datetime
import logging
from operator import xor
import traceback
import copy

from dateutil.relativedelta import relativedelta
from scripts.commonValues import contributionPhrases, nameHier, balanceTypePriority, mainTableNames, ownershipCorrect, ownershipFlagTolerance, pTransferTtypes, redemptionPhrases
from scripts.basicFunctions import calculateBackdate, calculate_xirr, accountBalanceKey, findSign
from scripts.processInvestments import processAboveBelow, processOneLevelInvestments

def processNode(nodeData : dict,selfData : dict, statusQueue, _, failed, transactionCalc: bool = False):
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
        tranEffects = selfData.get('tranEffects',{})
        calculationDict = {}
        earliestChangeDate = nodeData.get("earliestChangeDate") #earliest date for new data from last API pull
        node = nodeData.get('name')
        cache = nodeData.get("cache") #dataset of all relevant transactions and account balances for the pool
        if not cache:
            print(f"No data found for node {node}, so skipping calculations")
            logging.warning(f"No data found for node {node}, so skipping calculations")
            statusQueue.put((node,1,"Completed")) #allows the completion of calculations
            return [], {}
        newMonths = []

        if not noCalculations: #if there are calculations, find all months before the data pull, and then pull those calculations
            for month in months:
                #if the calculations for the month have already been complete, pull the old data
                if earliestChangeDate > datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S"):
                    calculationDict.setdefault(month['dateTime'],[]).extend(cache.get("calculations", {}).get(month["dateTime"], []))
                else:
                    newMonths.append(month)
        else:
            newMonths = months #check all months if there are no previous calculations
        IRRtrack = {} #dict of each fund's cash flows and dates for IRR calculation
        redeSourceTrack = defaultdict(dict) #dict of each investor's distributions to date (defaults to 0.0)
        if transactionCalc: #run transaction app calculations
            return processAboveBelow(newMonths,cache,node,failed,statusQueue)
        for month in newMonths: #loops through every month relevant to the pool
            monthFundIRRtrack = {}
            statusQueue.put((node,len(newMonths),"Working")) #puts to queue to update loading bar status. Allows computations to continue
            if failed.value: #if other workers failed, halt the process
                print(f"Exiting worker {node} due to other failure...")
                return [], {}
            totalDays = int(datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S").day  - datetime.strptime(month["tranStart"], "%Y-%m-%dT%H:%M:%S").day) + 1 #total days in month for MD den
            positionsBelow = cache.get("positions_below", {}).get(month["dateTime"], []) #account balances for the pool
            transactionsBelow = cache.get("transactions_below", {}).get(month["dateTime"], []) #account balances for the pool
            _ , cache,aboveData = processOneLevelInvestments(month,node,node,newMonths,cache,positionsBelow,transactionsBelow,IRRtrack,tranEffects)
            #calculationDict.setdefault(month['dateTime'],[]).extend(calculationExtend)
            if aboveData['skip']:
                pass #allows exited nodes to continue as zeros
                #continue #if there is no below, dont calculate above
            monthFundIRRtrack = aboveData['monthFundIRRtrack']
            monthNodeCalc = aboveData['monthNodeCalc']
            nodeNAV = aboveData['nodeNAV']
            aboveStartEntries = {}
            aboveEndEntries = {}
            abovePositions = cache.get("positions_above", {}).get(month["dateTime"], []) #account balances for investors into the pool for the month
            for pos in abovePositions: #find start and end entries for each investor and sort them
                source = pos["Source name"]
                if pos["Date"] == month["accountStart"]:
                    if source not in aboveStartEntries:
                        aboveStartEntries[source] = [pos,]
                    else:
                        aboveStartEntries[source].append(pos)
                if pos["Date"] == month["endDay"]:
                    if source not in aboveEndEntries:
                        aboveEndEntries[source] = [pos,]
                    else:
                        aboveEndEntries[source].append(pos)

            aboveTransactionDict = {}
            aboveTransactions = cache.get("transactions_above", {}).get(month["dateTime"], []) #all cashflow and commitment based transactions for investors into the pool for the month
            for tran in aboveTransactions: #sort by investor
                source = tran["Source name"]
                if source not in aboveTransactionDict:
                    aboveTransactionDict[source] = [tran,]
                else:
                    aboveTransactionDict[source].append(tran)


            aboveMDdenominatorSum = 0
            tempAboveDicts = {}
            pTransferDict = defaultdict(list)
            pTransfers = cache.get("pTransfers", {}).get(month["dateTime"], [])
            for pT in pTransfers:
                source = pT.get('TransferFromInvestingEntity','')
                pTransferDict[source].append(pT)
            nodeOwnershipSum = 0
            for source in set(aboveStartEntries.keys()) | set(aboveEndEntries.keys()) | set(aboveTransactionDict.keys() | set(redeSourceTrack.keys())): 
                #iterate through each investor in the pool for the month
                #node level loop for investors
                sourceWeightedCashFlow = 0
                sourceCashFlow = 0
                tempAboveDict = {}
                startEntry_cache = aboveStartEntries.get(source)
                if startEntry_cache: #use starting entry
                    if len(startEntry_cache) > 1:
                        # Choose the balance where Balancetype is the highest of the list, otherwise just the first
                        type_precedence = balanceTypePriority # Define type precedence
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
                    end_cache = aboveEndEntries.get(source)
                    if end_cache: #continue if there is a future entry
                        startEntry = copy.deepcopy(end_cache[0])
                        startEntry[nameHier["Value"]["dynHigh"]] = 0
                        noStartValue = True
                    else: #make an empty starting entry to build from
                        startEntry = {}
                if startEntry.get(nameHier["Value"]["dynHigh"]) in (None,"None"):
                    startEntry[nameHier["Value"]["dynHigh"]] = 0 #prevent float conversion errors
                investorTransactions = aboveTransactionDict.get(source,[]) #all investor transactions in the pool for the month
                redemptions = startEntry.get('Redemptions',0.0)
                contributions = startEntry.get('Contributions',0.0)
                for transaction in investorTransactions: 
                    cashFlow = transaction.get(nameHier["CashFlow"]["dynHigh"])
                    if  cashFlow not in (None,"None"):
                        cashFlow = float(cashFlow)
                        sourceCashFlow -= cashFlow
                        backDate = calculateBackdate(transaction, noStartValue=noStartValue) #dynamo revert by a day logic
                        backDate = 0
                        sourceWeightedCashFlow -= cashFlow  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day) + backDate)/totalDays
                        if backDate:
                            for m in newMonths:
                                if m["tranStart"] <= month["endDay"] <= m["endDay"]:
                                    for lst in cache.get('transactions_above', {}).get(m["dateTime"], []):
                                        if all(lst[header] == transaction[header] for header in list(lst.keys())): #if all values match
                                            date = datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S") - relativedelta(days=backDate) #datetime and subtract a day
                                            date = datetime.strftime(date, "%Y-%m-%dT%H:%M:%S")  #revert to string
                                            lst['Calculation Date'] = date #add calculation date to transaction in cache
                        tType = transaction.get('TransactionType','')  
                        if transaction.get('HFCashFlowType','') not in (None,'None') and 'overall' in transaction.get('HFCashFlowType','').lower():
                            if tType in tranEffects.get('Redemptions',[]):
                                redemptions += cashFlow
                            elif tType in tranEffects.get('Contributions',[]):
                                contributions -= cashFlow
                sourceMDdenominator = float(startEntry[nameHier["Value"]["dynHigh"]]) + sourceWeightedCashFlow
                tempAboveDict["MDden"] = sourceMDdenominator
                tempAboveDict["cashFlow"] = sourceCashFlow
                tempAboveDict["startVal"] = float(startEntry[nameHier["Value"]["dynHigh"]])
                tempAboveDict['Contributions'] = contributions
                tempAboveDict['Redemptions'] = redemptions
                sEOM = aboveEndEntries.get(source,[])
                if len(sEOM) > 0:
                    if sEOM[0].get(nameHier["Value"]["dynHigh"]) in (None,"None"):
                        sEOM[0][nameHier["Value"]["dynHigh"]] = 0
                aboveMDdenominatorSum += sourceMDdenominator
                tempAboveDicts[source] = tempAboveDict #store source calculations for secondary iteration for target level data
            monthNodeSourceEntryList = [] #stores investor data for third iteration (not needed to be split, but remnant from old logic.)
            for source in tempAboveDicts.keys():
                # second investor iteration to find the gain, return,ownership, and NAV values at pool level (i think it is not needed to be split, but remnant from old logic.)
                EOMcheck = aboveEndEntries.get(source,[])
                if len(EOMcheck) > 0:
                    if EOMcheck[0].get(nameHier["Value"]["dynHigh"]) in (None,"None"):
                        EOMcheck[0][nameHier["Value"]["dynHigh"]] = 0 #prevents float conversion errors
                sourceMDdenominator = tempAboveDicts[source]["MDden"]
                if aboveMDdenominatorSum == 0:
                    sourceGain = 0 #0 if no true value in the pool. avoids errors
                else:
                    sourceGain = aboveData['nodeGain'] * sourceMDdenominator / aboveMDdenominatorSum
                if sourceMDdenominator == 0:
                    sourceReturn = 0 #0 if investor has no value in pool. avoids error
                else:
                    sourceReturn = abs(sourceGain / sourceMDdenominator) * findSign(sourceGain)
                if round(tempAboveDicts[source]["startVal"] + tempAboveDicts[source]["cashFlow"]) == 0 or len(EOMcheck) == 0 or round(float(EOMcheck[0].get(nameHier["Value"]["dynHigh"],0))) == 0: 
                    #zero values if exited source
                    #exit check: start value and cashflow sums to zero OR no end value OR end value is zero
                    sourceEOM = tempAboveDicts[source]["startVal"] + tempAboveDicts[source]["cashFlow"] + sourceGain
                    if False: #this code is to remove values from exited investors. The NAV EOM zeroing out may still be beneficial for small scale errors
                        sourceEOM = 0
                        sourceGain = 0
                        sourceMDdenominator = 0
                        sourceReturn = 0
                else:
                    sourceEOM = tempAboveDicts[source]["startVal"] + tempAboveDicts[source]["cashFlow"] + sourceGain
                monthNodeSourceEntry = copy.deepcopy(monthNodeCalc) #uses node data as template
                monthNodeSourceEntry["Source name"] = source
                monthNodeSourceEntry["NAV"] = sourceEOM
                monthNodeSourceEntry["Monthly Gain"] = sourceGain
                monthNodeSourceEntry["Return"] = sourceReturn * 100
                monthNodeSourceEntry["MDdenominator"] = sourceMDdenominator
                ownershipPerc = sourceEOM/nodeNAV * 100 if nodeNAV != 0 else 0
                monthNodeSourceEntry["Ownership"] = ownershipPerc
                for key in ('Redemptions','Contributions', 'startVal'):
                    monthNodeSourceEntry[key] = tempAboveDicts[source][key]
                nodeOwnershipSum += ownershipPerc
                monthNodeSourceEntryList.append([monthNodeSourceEntry, EOMcheck])
            adjustedOwnershipBool = abs(nodeOwnershipSum - 100) > ownershipFlagTolerance and ownershipCorrect #boolean for if ownership is adjusted. Tolerance for thousandth of a percent off
            fundEntryList = aboveData['fundEntryList']
            for idx, sourceEntry, _ in ([idx,*mnE] for idx, mnE in enumerate(monthNodeSourceEntryList) if mnE[0]['Source name'] in pTransferDict):
                #iterate through any sources w/ partner transfers to allocate their Redemptions and contributions
                source = sourceEntry['Source name']
                for pT in pTransferDict[source]:
                    if pT.get('Amountinsystemcurrency') not in (None,'None'):
                        date = pT['Date']
                        if date == month['tranStart']:
                            alloDec = float(pT.get('Amountinsystemcurrency')) / sourceEntry['startVal'] if sourceEntry['startVal'] != 0 else 0
                        elif date == month['endDay']:
                            alloDec = float(pT.get('Amountinsystemcurrency')) / sourceEntry['NAV'] if sourceEntry['NAV'] != 0 else 0
                        else:
                            print(f'WARNING: Partner transfer date not at BOM or EOM')
                            continue
                    elif pT.get('Percent') not in (None,'None'): 
                        alloDec = float(pT.get('Percent'))
                    else:
                        print(f'WARNING: Partner transfer passed with no value')
                        continue #Failed
                    amounts = {}
                    for h in ('Redemptions','Contributions'):
                        val = monthNodeSourceEntryList[idx][0].get(h)
                        if val not in (None,'None'):
                            amounts[h] = alloDec * val
                            monthNodeSourceEntryList[idx][0][h] -= alloDec * val
                    recievingPs = pT.get('Transferto').split(';')
                    if not amounts or len(recievingPs) == 0:
                        print('WARNING: No amounts found or no target partners')
                        continue
                    else:
                        amounts = {k : v/len(recievingPs) for k,v in amounts.items()} #evenly divide to partners
                    for p in recievingPs:
                        p = p.strip()
                        for idx, sourceEntry, _ in ([idx,*mnE] for idx, mnE in enumerate(monthNodeSourceEntryList) if mnE[0]['Source name'] == p):
                            for h in amounts.keys():
                                if h not in monthNodeSourceEntryList[idx][0]:
                                    monthNodeSourceEntryList[idx][0][h] = 0.0
                                monthNodeSourceEntryList[idx][0][h] += amounts[h]
            for sourceEntry, EOMcheck in monthNodeSourceEntryList:
                source = sourceEntry["Source name"]
                sourceEOM = sourceEntry["NAV"]
                sourceOwnership = sourceEntry["Ownership"] * 100 /  nodeOwnershipSum if nodeOwnershipSum != 0 and ownershipCorrect else sourceEntry["Ownership"]
                if len(EOMcheck) > 0:
                    #update cache for the following month's calculations
                    if any(round(float(EOMcheck[0].get(header,0))) != round(val) for header, val  in ([nameHier["Value"]["dynHigh"],sourceEOM],['Redemptions',sourceEntry['Redemptions']],['Contributions',sourceEntry['Contributions']])): #don't push an update if the values are the same
                        bTypeChange = round(float(EOMcheck[0].get(nameHier["Value"]["dynHigh"],0))) != round(sourceEOM)
                        for m in newMonths:
                            if m["accountStart"] <= month["endDay"] <= m["endDay"]: #access the both the current month and next month
                                for lst in cache.get("positions_above", {}).get(m["dateTime"], []):
                                    if lst["Source name"] == source and lst["Target name"] == node and lst["Date"] == month["endDay"]:
                                        #access the EOM current month and BOM next month as endDay hits both of those
                                        lst[nameHier["Value"]["dynHigh"]] = sourceEOM #this does not represent adjusted values
                                        if bTypeChange:
                                            lst["Balancetype"] = "Calculated_R"
                                        for key in ('Redemptions','Contributions'):
                                            lst[key] = sourceEntry[key]
                elif len(EOMcheck) == 0: #continue a zero for exited fund calculations
                    sourceEOMentry = {"Date" : month["endDay"], "Source name" : source, "Target name" : node , nameHier["Value"]["dynLow"] : sourceEOM,
                                        "Balancetype" : "Calculated_R"
                                        }
                    for key in ('Redemptions','Contributions'):
                        sourceEOMentry[key] = sourceEntry[key]
                    # update cache for subsequent months
                    for m in newMonths:
                        if m["accountStart"] <= month["endDay"] <= m["endDay"]:
                            cache.setdefault("positions_above", {}).setdefault(m["dateTime"], []).append(sourceEOMentry)

                #final (3rd) investor level iteration to use the pool level results for the investor to calculate the fund level information
                srcOwnDec = sourceOwnership / 100
                srcMDdenDec = sourceEntry["MDdenominator"] / aboveMDdenominatorSum if aboveMDdenominatorSum != 0 else 0
                targetDecSum = 0
                for targetEntry in fundEntryList:
                    targetNAV = targetEntry["NAV"]
                    targetDec = targetNAV / nodeNAV if nodeNAV != 0 else 0
                    targetDecSum += targetDec
                    target = targetEntry["Target name"]
                    targetSourceNAV = srcOwnDec * targetNAV
                    targetSourceGain = srcMDdenDec * targetEntry["Monthly Gain"]
                    targetSourceMDdenominator = srcMDdenDec * targetEntry["MDdenominator"]
                    targetSourceReturn = abs(targetSourceGain / targetSourceMDdenominator) * findSign(targetSourceGain) if targetSourceMDdenominator != 0 else 0
                    targetSourceOwnership = targetSourceNAV /  targetNAV if targetNAV != 0 else 0
                    #account for commitment calculations on closed funds
                    tempFundOwnership = targetSourceOwnership if targetSourceOwnership != 0 else srcOwnDec
                    targetSourceCommitment = targetEntry[nameHier["Commitment"]["local"]] * tempFundOwnership 
                    targetSourceUnfunded = targetEntry[nameHier["Unfunded"]["local"]] * tempFundOwnership
                    monthTargetSourceEntry = {"dateTime" : month["dateTime"], "Source name" : sourceEntry["Source name"], "Node" : node, "Target name" : target ,
                                    "NAV" : targetSourceNAV, "Monthly Gain" : targetSourceGain , "Return" :  targetSourceReturn * 100, 
                                    "MDdenominator" : targetSourceMDdenominator, "Ownership" : targetSourceOwnership * 100,
                                    nameHier["Commitment"]["local"] : targetSourceCommitment, nameHier["Unfunded"]["local"] : targetSourceUnfunded, 
                                    "IRR ITD" : targetEntry['IRR ITD'],
                                    "ownershipAdjust" : xor(adjustedOwnershipBool, targetNAV == 0) and targetNAV != 0,
                                    'fDist' : targetEntry.get('Distributions',0.0),
                                    'fCont' : targetEntry.get('Contributions',0.0),
                                            }
                    for key in ('Redemptions','Contributions'):
                        monthTargetSourceEntry[key] = sourceEntry[key] * targetDec
                    calculationDict.setdefault(month['dateTime'],[]).append(monthTargetSourceEntry) #add fund level data to calculations for use in aggregation and report generation
            #end of months loop
        #commands to add database updates to the queues
        dynTables = {}
        
        for table in mainTableNames:
            dynTables[table] = []
            if "positions" == table: #removes duplicates by requiring a balance key
                uniqueBalances = {accountBalanceKey(entry): entry for monthL in cache.get('positions_below', {}) for entry in cache.get('positions_below', {}).get(monthL, [])}
                for monthL in cache.get('positions_above', {}): #now add in for positions above
                    for entry in cache.get('positions_above', {}).get(monthL, []):
                        uniqueBalances[accountBalanceKey(entry)] = entry
                dynTables[table].extend([entry for _,entry in uniqueBalances.items()])
            elif table == 'transactions':
                for tableName in ('transactions_below','transactions_above'):
                    for monthL in cache.get(tableName, {}).keys():
                        dynTables[table].extend(cache.get(tableName, {}).get(monthL, []))
        statusQueue.put((node,len(newMonths),"Completed")) #push completed status update to the main thread
        return calculationDict, dynTables
    except Exception as e: #halt operations for failure or force close/cancel
        statusQueue.put((node,len(newMonths),"Failed"))
        print(f"Worker for {nodeData.get('name')} failed.")
        failed = True
        try:
            trace = traceback.format_exc()
            print(trace)
            logging.error(trace)
        except:
            pass
        logging.error(e)
        print("\n")
        return {}, {}
