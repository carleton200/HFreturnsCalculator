
from classes.nodeLibrary import nodeLibrary
from scripts.basicFunctions import accountBalanceKey, nodalToLinkedCalculations, recursLinkCalcs
from scripts.processNode import processNode
import traceback, logging


def processClump(clumpData : list[dict],nodeLib : nodeLibrary, selfData : dict, statusQueue, _, failed, transactionCalc: bool = False):
    #function to take in the data for a full clump (group of nodes that are connected) and split the data for node processing
    # must run the nodes from the deepest level upwards, and port the updated account balances into the upper level nodes to properly adjust calculations
    try:
        deepestNode = max((nodeLib.nodePaths[nodeDict['name']]['lowestLevel'] for nodeDict in clumpData))
        clumpDataIdxs = {nodeDict['name'] : idx for idx, nodeDict in enumerate(clumpData)}
        nodeList = list(clumpDataIdxs.keys())
        months = selfData['months']
        linkedClumpCalculations = []
        clumpCalculations = []
        clumpCalculationsDict = {}
        clumpPositions = []
        clumpTransactions = []
        for nodeLevel in reversed(range(deepestNode + 1)): #iterate from the deepest nodes upward
            for nodeData in (nodeDict for nodeDict in clumpData if nodeLib.nodePaths[nodeDict['name']]['lowestLevel'] == nodeLevel): #run all nodes at the level
                nodeName = nodeData['name']
                nodeCalculations, nodeDynTables = processNode(nodeData,selfData,statusQueue,_,failed,transactionCalc)
                clumpCalculations.extend(nodeCalculations)
                clumpCalculationsDict.setdefault(nodeLevel,{})[nodeName] = nodeCalculations
                #pull account balances relevant to an upper node
                nodeAboves = nodeLib.nodePaths[nodeName]['above']
                for aboveID in nodeAboves:
                    aboveName = nodeLib.id2node[aboveID]
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
        maxNodeLevel = max(list(clumpCalculationsDict.keys()))
        for nodeLevel in reversed(range(maxNodeLevel + 1)): #iterate from the bottom up
            for node in clumpCalculationsDict[nodeLevel]:
                for monthDT, baseCalcs in clumpCalculationsDict[nodeLevel][node].items():
                    #recursively link each nodes targets (targets don't include other nodes) up to the highest above for a full link 
                    linkedClumpCalculations.extend(recursLinkCalcs(baseCalcs, monthDT, nodeLevel ,node,[nodeLib.node2id[node],], nodeLib,clumpCalculationsDict))

                
        return linkedClumpCalculations, {'positions' : clumpPositions, 'transactions' : clumpTransactions}
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
