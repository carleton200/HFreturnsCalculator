def processPoolTransactions(poolData : dict,selfData : dict, statusQueue, dbQueue, failed):
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
                monthPoolEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Pool", "Pool" : pool, 
                                        "Transaction Sum" : difference,
                                        "Calculation Type" : "Total Pool",
                }
                calculations.append(monthPoolEntry) #append to calculations for use in report generation and aggregation

            #end of months loop
        statusQueue.put((pool,len(newMonths),"Completed")) #push completed status update to the main thread
        return calculations
    except Exception as e: #halt operations for failure or force close/cancel
        statusQueue.put((pool,len(newMonths),"Failed"))
        print(f"Worker for {poolData.get("poolName")} failed.")
        print(traceback.format_exc())
        print("\n")
        return []
