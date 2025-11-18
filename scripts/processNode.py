from scripts.importList import *
from scripts.commonValues import *
from scripts.basicFunctions import *
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
        calculations = []
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
                    calculations.extend(cache.get("calculations", {}).get(month["dateTime"], []))
                else:
                    newMonths.append(month)
        else:
            newMonths = months #check all months if there are no previous calculations
        IRRtrack = {} #dict of each fund's cash flows and dates for IRR calculation
        IRRsourceTrack = {} #dict of each investor's cash flows and dates for IRR calculation
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
            calculationExtend, cache,aboveData = processOneLevelInvestments(month,node,node,newMonths,cache,positionsBelow,transactionsBelow,IRRtrack)
            calculations.extend(calculationExtend)
            if aboveData['skip']:
                continue #if there is no below, dont calculate above
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
            nodeOwnershipSum = 0
            for source in set(aboveStartEntries.keys()) | set(aboveEndEntries.keys()) | set(aboveTransactionDict.keys()): 
                #iterate through each investor in the pool for the month
                #pool level loop for investors
                sourceWeightedCashFlow = 0
                sourceCashFlow = 0
                tempAboveDict = {}
                startEntry_cache = aboveStartEntries.get(source)
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
                    end_cache = aboveEndEntries.get(source)
                    if end_cache:
                        startEntry = copy.deepcopy(end_cache[0])
                        startEntry[nameHier["Value"]["dynHigh"]] = 0
                        noStartValue = True
                    else:
                        continue #ignore the source completely if there is no starting or ending value
                if startEntry.get(nameHier["Value"]["dynHigh"]) in (None,"None"):
                    startEntry[nameHier["Value"]["dynHigh"]] = 0 #prevent float conversion errors
                investorTransactions = aboveTransactionDict.get(source,[]) #all investor transactions in the pool for the month
                
                for transaction in investorTransactions: 
                    if transaction.get(nameHier["CashFlow"]["dynHigh"]) not in (None,"None"):
                        sourceCashFlow -= float(transaction[nameHier["CashFlow"]["dynHigh"]])
                        backDate = calculateBackdate(transaction, noStartValue=noStartValue) #dynamo revert by a day logic
                        sourceWeightedCashFlow -= float(transaction[nameHier["CashFlow"]["dynHigh"]])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day) + backDate)/totalDays
                sourceMDdenominator = float(startEntry[nameHier["Value"]["dynHigh"]]) + sourceWeightedCashFlow
                tempAboveDict["MDden"] = sourceMDdenominator
                tempAboveDict["cashFlow"] = sourceCashFlow
                tempAboveDict["startVal"] = float(startEntry[nameHier["Value"]["dynHigh"]])
                if tempAboveDict["startVal"] == 0 and sourceCashFlow == 0:
                    continue #ignore investors with no value
                sEOM = aboveEndEntries.get(source,[])
                if len(sEOM) > 0:
                    if sEOM[0].get(nameHier["Value"]["dynHigh"]) in (None,"None"):
                        sEOM[0][nameHier["Value"]["dynHigh"]] = 0
                if round(tempAboveDict.get("startVal") + tempAboveDict.get("cashFlow")) != 0 and len(sEOM) > 0 and round(float(sEOM[0].get(nameHier["Value"]["dynHigh"],0))) != 0:
                    #only accounts for investor gain (MD den) if they have not exited
                    #exit check: starting value + cashflow is zero OR there is no ending value
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
                    sourceReturn = sourceGain / sourceMDdenominator
                if round(tempAboveDicts[source]["startVal"] + tempAboveDicts[source]["cashFlow"]) == 0 or len(EOMcheck) == 0 or round(float(EOMcheck[0].get(nameHier["Value"]["dynHigh"],0))) == 0: 
                    #zero values if exited source
                    #exit check: start value and cashflow sums to zero OR no end value OR end value is zero
                    sourceEOM = 0
                    sourceGain = 0
                    sourceMDdenominator = 0
                    sourceReturn = 0
                else:
                    sourceEOM = tempAboveDicts[source]["startVal"] + tempAboveDicts[source]["cashFlow"] + sourceGain
                monthNodeSourceEntry = copy.deepcopy(monthNodeCalc) #uses pool data as template
                monthNodeSourceEntry["Source name"] = source
                monthNodeSourceEntry["NAV"] = sourceEOM
                monthNodeSourceEntry["Monthly Gain"] = sourceGain
                monthNodeSourceEntry["Return"] = sourceReturn * 100
                monthNodeSourceEntry["MDdenominator"] = sourceMDdenominator
                ownershipPerc = sourceEOM/nodeNAV * 100 if nodeNAV != 0 else 0
                monthNodeSourceEntry["Ownership"] = ownershipPerc
                nodeOwnershipSum += ownershipPerc
                monthNodeSourceEntryList.append([monthNodeSourceEntry, EOMcheck])
            adjustedOwnershipBool = abs(nodeOwnershipSum - 100) > ownershipFlagTolerance #boolean for if ownership is adjusted. Tolerance for thousandth of a percent off
            fundEntryList = aboveData['fundEntryList']
            for sourceEntry, EOMcheck in monthNodeSourceEntryList:
                source = sourceEntry["Source name"]
                sourceEOM = sourceEntry["NAV"]
                sourceOwnership = sourceEntry["Ownership"] * 100 /  nodeOwnershipSum if nodeOwnershipSum != 0 and ownershipCorrect else sourceEntry["Ownership"]
                if len(EOMcheck) > 0: #only update the database for the investor if they have account balances
                    #update cache for the following month's calculations
                    if round(float(EOMcheck[0].get(nameHier["Value"]["dynHigh"],0))) != round(sourceEOM): #don't push an update if the values are the same
                        for m in newMonths:
                            if m["accountStart"] <= month["endDay"] <= m["endDay"]: #access the both the current month and next month
                                for lst in cache.get("positions_above", {}).get(m["dateTime"], []):
                                    if lst["Source name"] == source and lst["Target name"] == node and lst["Date"] == month["endDay"]:
                                        #access the EOM current month and BOM next month as endDay hits both of those
                                        lst[nameHier["Value"]["dynHigh"]] = sourceEOM #this does not represent adjusted values
                                        lst["Balancetype"] = "Calculated_R"
                #final (3rd) investor level iteration to use the pool level results for the investor to calculate the fund level information
                for targetEntry in fundEntryList:
                    target = targetEntry["Target name"]
                    targetSourceNAV = sourceOwnership / 100 * targetEntry["NAV"]
                    targetSourceGain = targetEntry["Monthly Gain"] / monthNodeCalc["Monthly Gain"] * sourceEntry["Monthly Gain"] if monthNodeCalc["Monthly Gain"] != 0 else 0
                    targetSourceMDdenominator = sourceEntry["MDdenominator"] / monthNodeCalc["MDdenominator"] * targetEntry["MDdenominator"] if monthNodeCalc["MDdenominator"] != 0 else 0
                    targetSourceReturn = targetSourceGain / targetSourceMDdenominator if targetSourceMDdenominator != 0 else 0
                    targetSourceOwnership = targetSourceNAV /  targetEntry["NAV"] if targetEntry["NAV"] != 0 else 0
                    #account for commitment calculations on closed funds
                    tempFundOwnership = targetSourceOwnership if targetSourceOwnership != 0 else sourceOwnership / 100
                    targetSourceCommitment = targetEntry[nameHier["Commitment"]["local"]] * tempFundOwnership 
                    targetSourceUnfunded = targetEntry[nameHier["Unfunded"]["local"]] * tempFundOwnership

                    if sourceEntry["MDdenominator"] != 0 and aboveMDdenominatorSum != 0: 
                        #only run IRR data if there is investor value
                        if source not in IRRsourceTrack:
                            IRRsourceTrack[source] = {}
                        if target not in IRRsourceTrack[source]:
                            IRRsourceTrack[source][target] = {"cashFlows" : [], "dates" : []}
                        cashflows = monthFundIRRtrack.get(target, {}).get("cashFlows", [])
                        dates = monthFundIRRtrack.get(target, {}).get("dates", [])
                        for cashflow, date in zip(cashflows, dates):
                            adjustedCashflow = cashflow * sourceEntry["MDdenominator"] / aboveMDdenominatorSum #ratio the cashflow to their MDdenominator
                            IRRsourceTrack[source][target]["cashFlows"].append(adjustedCashflow)
                            IRRsourceTrack[source][target]["dates"].append(date)
                    if sourceEntry["Source name"] in IRRsourceTrack and target in IRRsourceTrack[sourceEntry["Source name"]]:
                        targetSourceIRR = calculate_xirr([*IRRsourceTrack[sourceEntry["Source name"]][target]["cashFlows"], targetSourceNAV], [*IRRsourceTrack[sourceEntry["Source name"]][target]["dates"], datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S")])
                    else:
                        targetSourceIRR = None
                    monthTargetSourceEntry = {"dateTime" : month["dateTime"], "Source name" : sourceEntry["Source name"], "Node" : node, "Target name" : target ,
                                    "NAV" : targetSourceNAV, "Monthly Gain" : targetSourceGain , "Return" :  targetSourceReturn * 100, 
                                    "MDdenominator" : targetSourceMDdenominator, "Ownership" : targetSourceOwnership * 100,
                                    nameHier["Commitment"]["local"] : targetSourceCommitment, nameHier["Unfunded"]["local"] : targetSourceUnfunded, 
                                    "IRR ITD" : targetSourceIRR,
                                    "ownershipAdjust" : adjustedOwnershipBool}
                    calculations.append(monthTargetSourceEntry) #add fund level data to calculations for use in aggregation and report generation
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
        return calculations, dynTables
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
        return [], {}
