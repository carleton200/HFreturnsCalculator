from scripts.importList import *
from scripts.commonValues import *
from scripts.basicFunctions import *
import traceback

def processAboveBelow(newMonths,cache,node,failed,statusQueue):
    calculations = []
    for month in newMonths: #loops through every month relevant to the pool
        statusQueue.put((node,len(newMonths),"Working")) #puts to queue to update loading bar status. Allows computations to continue
        if failed.value: #if other workers failed, halt the process
            print(f"Exiting worker '{node}' due to other failure...")
            return []
        belowCashFlow = 0
        lowTransactions = cache.get("transactions_below", {}).get(month["dateTime"], [])
        for transaction in lowTransactions: #get fund data, cash flows, and commitment alterations
            if transaction["TransactionType"] not in commitmentChangeTransactionTypes and transaction[nameHier["CashFlow"]["dynLow"]] not in (None, "None"):
                belowCashFlow -= float(transaction[nameHier["CashFlow"]["dynLow"]])
        aboveCashFlow = 0
        transactionsAbove = cache.get("transactions_above", {}).get(month["dateTime"], []) #all cashflow and commitment based transactions for investors into the pool for the month
        for tran in transactionsAbove:
            if tran["TransactionType"] not in commitmentChangeTransactionTypes and tran[nameHier["CashFlow"]["dynHigh"]] not in (None, "None"):
                aboveCashFlow -= float(tran[nameHier["CashFlow"]["dynHigh"]])
        difference = round(belowCashFlow - aboveCashFlow,2) * -1
        if difference != 0:
            monthPoolEntry = {"dateTime" : month["dateTime"], "Node" : node, 
                                    "Transaction Sum" : difference,
            }
            calculations.append(monthPoolEntry) #append to calculations for use in report generation and aggregation

        #end of months loop
    statusQueue.put((node,len(newMonths),"Completed")) #push completed status update to the main thread
    return calculations

def processOneLevelInvestments(month, node, invSourceName, newMonths, cache, positions,transactions, IRRtrack):
    #function to handle the target level investment data. Pass only data from one source name (node or investor)
    investments = set()
    startEntries = {}
    endEntries = {}
    monthFundIRRtrack = {}
    calculationExtend = []
    posTableName = 'positions_below' if node == invSourceName else 'positions' #node vs non-nodal data
    totalDays = int(datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S").day  - datetime.strptime(month["tranStart"], "%Y-%m-%dT%H:%M:%S").day) + 1 #total days in month for MD den
    for account in positions: #finds all fund starting and ending balances for the month
        investments.add(account["Target name"])
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

    #funds that do not have account positions but are relevant to the pool (ex: deferred liabilities)
    targetTransactionsDict = {}
    for transaction in transactions:
        investments.add(transaction['Target name'])
        if transaction["Target name"] not in targetTransactionsDict:
            targetTransactionsDict[transaction["Target name"]] = [transaction,]
        else:
            targetTransactionsDict[transaction["Target name"]].append(transaction)
    nodeGain = 0
    nodeNAV = 0
    nodeMDdenominator = 0
    nodeWeightedCashFlow = 0
    nodeCashFlow = 0
    fundEntryList = []
    for investment in investments: #iterate through all funds to find the pool NAV and MD den
        startEntry = copy.deepcopy(startEntries.get(investment, []))
        endEntry = copy.deepcopy(endEntries.get(investment, []))
        createFinalValue = False
        noStartValue = False
        if len(startEntry) < 1: #no start value, so NAV = 0
            startEntry = [{nameHier["Value"]["dynLow"] : 0}]  #nameHier is a dictionary for common references to specific names. 
            noStartValue = True
            commitment = 0
            unfunded = 0
        else: #instantiate starting data
            commitment = float(startEntry[0].get(nameHier["Commitment"]["local"],0))
            unfunded = float(startEntry[0].get(nameHier["Unfunded"]["local"],0))
        if len(startEntry) > 1: #combines the values for fund sub classes for calculations
            startEntry = handleFundClasses(startEntry)
        if len(endEntry) < 1: #no end account balance yet, so create it.  
            # TODO question. Should I remove this and just not use the fund if an end balance is not there?
            createFinalValue = True
            endEntry = [{nameHier["Value"]["dynLow"] : 0}]
        if len(endEntry) > 1: #combine sub funds for calculations
            endEntry = handleFundClasses(endEntry)
        startEntry = startEntry[0]
        if startEntry.get(nameHier["Value"]["dynLow"]) == 0:
            noStartValue = True
        endEntry = endEntry[0]
        targetTransactions = targetTransactionsDict.get(investment,[]) 
        invCashFlowSum = 0
        invWeightedCashFlow = 0
        for transaction in targetTransactions: #get fund data, cash flows, and commitment alterations
            if transaction["TransactionType"] not in commitmentChangeTransactionTypes and transaction[nameHier["CashFlow"]["dynLow"]] not in (None, "None"):
                invCashFlowSum -= float(transaction[nameHier["CashFlow"]["dynLow"]])
                backDate = calculateBackdate(transaction, noStartValue) #Uses dynamo transaction time logic to decide to subtract one day or not
                invWeightedCashFlow -= float(transaction[nameHier["CashFlow"]["dynLow"]])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day) + backDate)/totalDays
                if transaction.get(nameHier["Unfunded"]["dynLow"]) not in (None,"None"):
                    unfunded += float(transaction[nameHier["Unfunded"]["value"]])
                if investment not in IRRtrack:
                    IRRtrack[investment] = {"cashFlows" : [], "dates" : []}
                IRRtrack[investment]["cashFlows"].append(float(transaction[nameHier["CashFlow"]["dynLow"]]))
                IRRtrack[investment]["dates"].append(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S") - relativedelta(days=backDate))
                if investment not in monthFundIRRtrack:
                    monthFundIRRtrack[investment] = {"cashFlows" : [], "dates" : []}
                monthFundIRRtrack[investment]["cashFlows"].append(float(transaction[nameHier["CashFlow"]["dynLow"]]))
                monthFundIRRtrack[investment]["dates"].append(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S") - relativedelta(days=backDate))
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
                endEntry[nameHier["Value"]["dynLow"]] = float(startEntry[nameHier["Value"]["dynLow"]]) + invCashFlowSum    
            invGain = (float(endEntry[nameHier["Value"]["dynLow"]]) - float(startEntry[nameHier["Value"]["dynLow"]]) - invCashFlowSum)
            invMDdenominator = float(startEntry[nameHier["Value"]["dynLow"]]) + invWeightedCashFlow
            invNAV = float(endEntry[nameHier["Value"]["dynLow"]])
            invReturn = invGain/invMDdenominator * 100 * findSign(invGain) if invMDdenominator != 0 else 0
            if investment in IRRtrack:
                IRRitd = calculate_xirr([*IRRtrack[investment]["cashFlows"], invNAV], [*IRRtrack[investment]["dates"], datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S")])
            else:
                IRRitd = None
            if unfunded < 0:
                unfunded = 0 #corrects for if original commitment was not logged properly
            if createFinalValue: #builds an entry to put into the database and cache if it is missing
                fundEOMentry = {"Date" : month["endDay"], "Source name" : invSourceName, "Target name" : investment , nameHier["Value"]["dynLow"] : endEntry[nameHier["Value"]["dynLow"]],
                                    "Balancetype" : "Calculated_R", nameHier["Commitment"]["local"] : commitment, nameHier["Unfunded"]["local"] : unfunded,
                                    }
                # update cache for subsequent months
                for m in newMonths:
                    if m["accountStart"] <= month["endDay"] <= m["endDay"]:
                        cache.setdefault(posTableName, {}).setdefault(m["dateTime"], []).append(fundEOMentry)
            else: #update database and cache with the calculated commitment, unfunded, and sleeve (asset lvl 3)
                # update cache for all months referencing this date
                
                for m in newMonths:
                    if m["accountStart"] <= month["endDay"] <= m["endDay"]:
                        for lst in cache.get(posTableName, {}).get(m["dateTime"], []):
                            if lst["Target name"] == investment and lst["Date"] == month["endDay"]:
                                lst[nameHier["Commitment"]["local"]] = commitment
                                lst[nameHier["Unfunded"]["local"]] = unfunded
            #sum each fund value into the pool totals
            nodeGain += invGain
            nodeMDdenominator += invMDdenominator
            nodeNAV += invNAV
            nodeCashFlow += invCashFlowSum
            nodeWeightedCashFlow += invWeightedCashFlow
            monthInvCalc = {"dateTime" : month["dateTime"], "Source name" : invSourceName ,  "Target name" : investment , "Node" : node,
                            "NAV" : invNAV, "Monthly Gain" : invGain, "Return" : invReturn , 
                            "MDdenominator" : invMDdenominator, "Ownership" : "", 
                            "IRR ITD" : IRRitd,
                            nameHier["Commitment"]["local"] : commitment,
                            nameHier["Unfunded"]["local"] : unfunded,
                            }
            if investment not in (None,"None"): #removing blank funds (found duplicate of Monogram in 'HF Direct Investments Pool, LLC - PE (2021)' with most None values)
                calculationExtend.append(monthInvCalc) #append to calculations for use in report generation and aggregation
                fundEntryList.append(monthInvCalc) #fund data stored on its own for investor calculations

        except Exception as e:
            print(f"Skipped fund {investment} for {invSourceName} in {month["Month"]} because: {traceback.format_exc()}")
            #Testing flag. skips fund if the values are zero and cause an error
    skipUpper = nodeNAV == 0 and nodeCashFlow == 0#skips the pool if there is no cash flow or value in the pool
    poolReturn = nodeGain/nodeMDdenominator * 100 * findSign(nodeGain) if nodeMDdenominator != 0 else 0
    monthNodeCalc = {"dateTime" : month["dateTime"], "Source name" : invSourceName, "Target name" : None, "Node" : node,
                    "NAV" : nodeNAV, "Monthly Gain" : nodeGain, "Return" : poolReturn , "MDdenominator" : nodeMDdenominator,
                        "Ownership" : None} 
                    #generic pool data for investors calculations
    aboveData = {'skip' : skipUpper, 'monthNodeCalc' : monthNodeCalc, 'monthFundIRRtrack' : monthFundIRRtrack, 'nodeGain' : nodeGain, 'nodeNAV' : nodeNAV, 'fundEntryList' : fundEntryList}
    return calculationExtend, cache, aboveData

def processInvestments(nodeData : dict,selfData : dict, statusQueue, _, failed, transactionCalc: bool = False):
    #Function to take all the information for one pool, calculate all relevant information, and return a list of the calculations
    #Inputs:
    #   poolData: dict with information relevant to this specific pool
    #   selfData: dict with information common to every pool
    #   statusQueue: a multiprocessing Manager queue for all worker threads to send progress bar and status updates. Minimizes database wait time
    #   dbQueue: a multiprocessing manager queue for worker threads to send final database updates to allow the worker to complete and not block the database
    #   failed: a multiprocessing variable. Begins negative. If any worker flags it as true, all workers will see it and halt if they hit the failure checkpoint
    try:
        months = selfData.get("months") #list of pre-prepared data for each month
        fundList = selfData.get("fundList") #list of funds/investments and some accompanying data (such as asset class level 3)
        calculations = []
        node = nodeData.get('name')
        cache = nodeData.get("cache") #dataset of all relevant transactions and account balances for the pool
        if not cache:
            print(f"No data found for direct investing data, so skipping calculations")
            logging.warning(f"No data found for direct investing data, so skipping calculations")
            statusQueue.put((node,1,"Completed")) #allows the completion of calculations
            return [], {}
        newMonths = months #check all months if there are no previous calculations
        IRRtrack = {} #dict of each fund's cash flows and dates for IRR calculation
        if transactionCalc: #run transaction app calculations
            return processAboveBelow(newMonths,cache,node,failed,statusQueue)
        for month in newMonths: #loops through every month relevant to the pool
            statusQueue.put((node,len(newMonths),"Working")) #puts to queue to update loading bar status. Allows computations to continue
            if failed.value: #if other workers failed, halt the process
                print(f"Exiting worker {node} due to other failure...")
                return [], {}
            allPositions = cache.get("positions", {}).get(month["dateTime"], []) #account balances for the pool
            allTransactions = cache.get("transactions", {}).get(month["dateTime"], []) #account balances for the pool
            sourceNames = set(pos['Source name'] for pos in allPositions) or set(tran['Source name'] for tran in allTransactions)
            for sourceName in sourceNames:
                #Divice the data by source name (investor) for the investment calc function
                invPositions = [pos for pos in allPositions if pos['Source name'] == sourceName]
                invTransactions = [tran for tran in allTransactions if tran['Source name'] == sourceName]
                calculationExtend, cache, _ = processOneLevelInvestments(month,node,sourceName,newMonths,cache,invPositions,invTransactions,IRRtrack)
                calculations.extend(calculationExtend)
            #end of months loop
        #commands to add database updates to the queues
        dynTables = {}
        
        for table in mainTableNames:
            dynTables[table] = []
            if "positions" == table: #removes duplicates by requiring a balance key
                uniqueBalances = {accountBalanceKey(entry): entry for monthL in cache.get(table, {}) for entry in cache.get(table, {}).get(monthL, [])}
                dynTables[table].extend([entry for _,entry in uniqueBalances.items()])
            else:
                for monthL in cache.get(table, {}).keys():
                    dynTables[table].extend(cache.get(table, {}).get(monthL, []))
        for idx, _ in enumerate(calculations): #build to final calculation format from the node style
            calculations[idx].pop('node')
            calculations[idx]['nodePath'] = None
            
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
