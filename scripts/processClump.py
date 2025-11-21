
from scripts.basicFunctions import accountBalanceKey, nodalToLinkedCalculations, recursLinkCalcs
from scripts.processNode import processNode
import traceback, logging


def processClump(clumpData : list[dict],nodePaths : dict, selfData : dict, statusQueue, _, failed, transactionCalc: bool = False):
    #function to take in the data for a full clump (group of nodes that are connected) and split the data for node processing
    # must run the nodes from the deepest level upwards, and port the updated account balances into the upper level nodes to properly adjust calculations
    try:
        deepestNode = max((nodePaths[nodeDict['name']]['lowestLevel'] for nodeDict in clumpData))
        clumpDataIdxs = {nodeDict['name'] : idx for idx, nodeDict in enumerate(clumpData)}
        Id2Node = {nodeDict['id'] : nodeDict['name'] for nodeDict in nodePaths}
        nodeList = list(clumpDataIdxs.keys())
        months = selfData['months']
        linkedClumpCalculations = []
        clumpCalculations = []
        clumpCalculationsDict = {}
        clumpPositions = []
        clumpTransactions = []
        for nodeLevel in reversed(range(deepestNode + 1)): #iterate from the deepest nodes upward
            for nodeData in (nodeDict for nodeDict in clumpData if nodePaths[nodeDict['name']]['lowestLevel'] == nodeLevel): #run all nodes at the level
                nodeName = nodeData['name']
                nodeCalculations, nodeDynTables = processNode(nodeData,selfData,statusQueue,_,failed,transactionCalc)
                clumpCalculations.extend(nodeCalculations)
                clumpCalculationsDict.setdefault(nodeLevel,{})[nodeName] = nodeCalculations
                #pull account balances relevant to an upper node
                nodeAboves = nodePaths[nodeName]['above']
                for aboveID in nodeAboves:
                    aboveName = nodePaths[list(nodePaths.keys())[aboveID]]['name']
                    linkedPosByMonth = {}
                    #all positions from the completed node that tie to the above node as the above node was the source
                    for row in (pos for pos in nodeDynTables.get('positions',[]) if pos['Source name'] == aboveName):
                        for m in months: #find the month the account balance or transaction belongs in
                            start = m["accountStart"]
                            date = row.get("Date")
                            if not (start <= date <= m["endDay"]):
                                continue
                            linkedPosByMonth.setdefault(m["dateTime"], []).append(row)
                    aboveNodePosBelow  : dict[list[dict]] = clumpData[clumpDataIdxs[aboveName]]['cache']['positions_below'] #pull the below positions of the above node
                    for month in (aboveNodePosBelow or linkedPosByMonth):
                        newPosBelowDict = {accountBalanceKey(entry) : entry for entry in linkedPosByMonth.get(month,[])} #set a dict to the edited entries of the below node
                        for pos in aboveNodePosBelow.get(month,[]): #add in the below positions of the above node if they do not already exist from the below node
                            newPosBelowDict.setdefault(accountBalanceKey(pos),pos) #TODO: check if this is safe w fund classes and multiple account balances. Don't want data deleted. Should likely be okay w balanceTypePriority as well since I overwrite them on above anyways
                        clumpData[clumpDataIdxs[aboveName]]['cache']['positions_below'][month] = [entry for entry in newPosBelowDict.values()] #set their below positions to the proper 
                if nodeLevel == 0: #if highest level, add above and below
                    clumpPositions.extend(nodeDynTables.get('positions',[]))
                    clumpTransactions.extend(nodeDynTables.get('transactions',[]))
                else: #if lower node, add only below data and above data if not attached to a node (direct to investor). The other above will be handled by the upper levels
                    clumpPositions.extend([pos for pos in nodeDynTables.get('positions',[]) if pos['Source name'] == nodeName or pos['Source name'] not in nodeList])
                    clumpTransactions.extend([tran for tran in nodeDynTables.get('transactions',[]) if tran['Source name'] == nodeName or tran['Source name'] not in nodeList])
        #TODO: build connection of nodeCalculations to have final calculation output
        #TODO: can set if the max level is 0 (most pools) then use the method present in processInvestments (make into function in basicFUnctions)
        # otherwise, string them together using the above and below data
        maxNodeLevel = max(list(clumpCalculationsDict.keys()))
        for node, nCalcs in clumpCalculationsDict[maxNodeLevel].items():
            if maxNodeLevel == 0: #one node, use the pool logic and just swap the node to nodePath
                linkedClumpCalculations.extend(nodalToLinkedCalculations((calc for monthCalcs in nCalcs.items() for calc in monthCalcs),nodePath=[node,]))
            else:
                linkedClumpCalculations.extend(recursLinkCalcs(maxNodeLevel,node,nodePaths,clumpCalculationsDict))

                
        return clumpCalculations, {'positions' : clumpPositions, 'transactions' : clumpTransactions}
    except Exception as e: #halt operations for failure or force close/cancel
        statusQueue.put(('DummyFail',99,"Failed"))
        print(f"Clump processing failed.")
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
