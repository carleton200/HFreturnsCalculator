from classes import nodeLibrary
import copy
import traceback
import queue
from datetime import datetime
import pandas as pd
import pyxirr
from collections import deque, defaultdict
from oldScripts.temp import MultiSelectBox
from scripts.commonValues import nameHier, nodePathSplitter, balanceTypePriority, nonFundCols, masterFilterOptions, smallHeaders
from scripts.instantiate_basics import gui_queue, APIexecutor
import re

def infer_sqlite_type(val):
    # Try to infer column types in SQLite: INTEGER, REAL, TEXT, or BLOB
    if val is None:
        return "TEXT"
    try:
        # int, but not bool (bool is a subclass of int in Python)
        if type(val) is int:
            return "INTEGER"
        if type(val) is float:
            return "REAL"
        if type(val) is bytes:
            return "BLOB"
        if type(val) is bool:
            return "BOOL"
        # Try conversion for number-like strings
        sval = str(val)
        try:
            int(sval)
            return "INTEGER"
        except Exception:
            pass
        try:
            float(sval)
            return "REAL"
        except Exception:
            pass
        return "TEXT"
    except Exception:
        return "TEXT"


def nodalToLinkedCalculations(calcs, nodePath : list[int] = None):
    for idx, _ in enumerate(calcs): #build to final calculation format from the node style
        calcs[idx].pop('Node')
        calcs[idx]['nodePath'] = " " + nodePathSplitter.join((str(n) for n in nodePath)) + " " if nodePath else None
    return calcs

def handleFundClasses(entryList):
    split = {}
    foundDuplicate = False
    for entry in entryList: #split the entries by fundclass to check for duplicates
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
        for fundSubKey in split: #loop by fund
            if len(split.get(fundSubKey)) > 1: #check if duplicates
                foundType = False
                for balanceType in balanceTypePriority: #loop through balance types by priority
                    for entry in split.get(fundSubKey): #loop through the duplicate entries
                        if entry.get("Balancetype") == balanceType and entry.get(nameHier["Value"]["dynLow"]) not in (None,"None"): #if the balance type is preferred, add the entry and break
                            singleEntries.append(entry)
                            foundType = True
                            break
                    if foundType: #stop balance type checking if found
                        break
                if not foundType: #reaches if nothing was found
                    for entry in split.get(fundSubKey): #loop through to find the first with a value
                        if entry.get(nameHier["Value"]["dynLow"]) not in (None,"None"): #if the balance type is preferred, add the entry and break
                            singleEntries.append(entry)
                            foundType = True
                            break
                    if not foundType: #final attempt take first entry
                        singleEntries.append(split.get(fundSubKey)[0])
            else: #no duplicates for this fund
                singleEntries.append(split.get(fundSubKey)[0])
    else:
        singleEntries.extend(entryList)
    tempNAV = 0
    for entry in singleEntries:
        if entry.get(nameHier["Value"]["dynLow"]) not in (None,0,"None"):
            tempNAV += float(entry.get(nameHier["Value"]["dynLow"])) #adds values to the first index
    entryList[0][nameHier["Value"]["dynLow"]] = str(tempNAV)
    return entryList

def get_connected_node_groups(nodePaths):
    """
    Returns a list of sets, each containing the names of nodes that are connected directly
    or transitively via 'above' and 'below' relationships in nodePaths.
    """
    # Build undirected graph from above/below relationships
    

    # Make an adjacency list (undirected)
    adjacency = defaultdict(set)
    for node, info in nodePaths.items():
        # Convert id sets to names
        aboves = info.get('above', set())
        belows = info.get('below', set())
        for a_id in aboves:
            # find node name for each id
            for other_name, other_info in nodePaths.items():
                if other_info['id'] == a_id:
                    adjacency[node].add(other_name)
                    adjacency[other_name].add(node)
        for b_id in belows:
            for other_name, other_info in nodePaths.items():
                if other_info['id'] == b_id:
                    adjacency[node].add(other_name)
                    adjacency[other_name].add(node)

    visited = set()
    groups = []

    # BFS to find connected components
    for node in nodePaths:
        if node not in visited:
            group = set()
            q = deque([node])
            while q:
                current = q.popleft()
                if current not in visited:
                    visited.add(current)
                    group.add(current)
                    for neighbor in adjacency[current]:
                        if neighbor not in visited:
                            q.append(neighbor)
            groups.append(group)
    return groups

def recursLinkCalcs(baseCalcs, monthDT, nodeLvl : int, node :str, currPath: list, nodeLib : nodeLibrary, clumpCalculationsDict: dict[dict[list[dict]]]):
    linkedCalcs = []
    aboveIds = nodeLib.nodePaths[node]['above']
    aboveNodes = list(nodeLib.id2node[aboveID] for aboveID in aboveIds)
    aboveCalcDict = {aboveNode : [] for aboveNode in aboveNodes}
    baseCalcs = [calc for calc in baseCalcs if calc['Target name'] in nodeLib.targets]
    for belowCalc in baseCalcs:
        if belowCalc['Source name'] not in aboveNodes:
            #Scenario 1: calcs have linked to their highest level (investor) Return with nodePath
            tempCalc = belowCalc.copy()
            tempCalc.pop('Node')
            tempCalc['nodePath'] = " " + nodePathSplitter.join([str(item) for item in reversed(currPath)]) + " "
            linkedCalcs.append(tempCalc)
        else:
            #Scenario 2: Calc needs to be linked and divided amongst the higher nodal levels and split by node for further recursion
            aboveNode = belowCalc['Source name']
            aboveCnctCalcs = [calc for calc in clumpCalculationsDict[nodeLvl - 1].get(aboveNode,{}).get(monthDT,[]) if calc['Target name'] == node]
            for aboveCalc in aboveCnctCalcs: #string the higher level calculations directly to the node's lower level target by ownership
                tempCalc = belowCalc.copy()
                tempCalc['Source name'] = aboveCalc['Source name']
                nodeOwnershipFrac = aboveCalc['Ownership'] / 100
                targetOwnershipFrac = nodeOwnershipFrac * belowCalc['Ownership'] / 100
                for field in ('NAV','Monthly Gain', 'MDdenominator', 'Commitment', 'Unfunded'): #split by ownership of the node's investment
                    tempCalc[field] = tempCalc[field] * nodeOwnershipFrac
                tempCalc['ownershipAdjust'] = aboveCalc['ownershipAdjust'] or tempCalc['ownershipAdjust'] #any adjustment triggers true
                tempCalc['Ownership'] = targetOwnershipFrac * 100
                tempCalc['IRR ITD'] = None
                tempCalc['Return'] = tempCalc['Monthly Gain'] / tempCalc['MDdenominator'] * 100 if tempCalc['MDdenominator'] != 0.0 else 0.0
                aboveCalcDict[aboveNode].append(tempCalc)
    for aboveNode, aboveCalcs in aboveCalcDict.items(): 
        #split all higher linked calculations to their recursion. Ones already at their peak will be returned by scenario 1
        aboveID = nodeLib.node2id[aboveNode]
        linkedCalcs.extend(recursLinkCalcs(aboveCalcs,monthDT,nodeLvl - 1, aboveNode, [*currPath,aboveID], nodeLib, clumpCalculationsDict))
    return linkedCalcs
                


def calculate_xirr(cash_flows, dates, guess : float = None):
    try:
        if cash_flows[-1] == 0:
            #indicates closed fund. Remove the NAV as the cashflows should show the fund emptying
            if len(cash_flows) > 2 and cash_flows[-2] != 0:
                cash_flows = cash_flows[:-1]
                dates = dates[:-1]
            else:
                return None #if only two cashflows, it is just a singular investment
        if not( any(cf > 0 for cf in cash_flows) and any(cf < 0 for cf in cash_flows)):
            return None #indicates no returns yet or no investments
        result = pyxirr.xirr(dates, cash_flows)
        if result:
            return result * 100
        else:
            return None
    except pyxirr.InvalidPaymentsError as e:
        print(f"Skipping XIRR calculation due to InvalidPaymentsError: {e} \n Cash flows: {cash_flows} \n Dates: {dates}")
        return None
    except RuntimeWarning as e:
        #print(f"Skipping XIRR calculation due to RuntimeWarning: {e}")
        return None
    except Exception as e:
        print(f"Skipping XIRR calculation due to Exception: {e} \n Cash flows: {cash_flows} \n Dates: {dates}")
        return None
def descendingNavSort(input : dict):
    return sorted(input.keys(), key=lambda x: float(input.get(x,0.0)) * -1)

def findSign(num: float):
    if num == 0:
        return 0
    return num / abs(num)
def separateRowCode(label):
        header = re.sub(r'##\(.*\)##', '', label, flags=re.DOTALL)
        code = re.findall(r'##\(.*\)##', label, flags=re.DOTALL)[0]
        return header, code

def accountBalanceKey(accEntry : dict):
    try:
        key = accEntry["Date"] + "_" + accEntry["Source name"] + "_" + accEntry["Target name"]
        for accountField in ("Balancetype", 'InvestsThrough','Fundclass'):
            key += accEntry.get(accountField, "") if accEntry.get(accountField, "") is not None else ""
    except:
        print(f"Failed for entry: {accEntry}")
        raise
    return key
def annualizeITD(cumITD, monthCount):
    if monthCount < 12: #ITD for less than a year is essentially YTD style
        return (cumITD - 1) * 100
    elif cumITD > 0:
        return ((cumITD ** (12/monthCount)) - 1) * 100
    else:
        return 'N/A'

def calculateBackdate(transaction,noStartValue = False):
    time = transaction.get(nameHier["Transaction Time"]["dynLow"])
    monthDay = datetime.strptime(transaction.get("Date"), "%Y-%m-%dT%H:%M:%S").day
    if noStartValue:
        if time not in (None,"None") and time.lower() == "end of day":
            backDate = 0 #"no start value and end of day"
        else:
            backDate = 1 #"no start value and not end of day"
    elif time in (None,"None"):
        if monthDay == 1:
            backDate = 1 #"First day of month"
        else:
            backDate = 0#"No timing and not first day of month"
    elif time.lower() == "end of day":
        backDate = 0#"End of day"
    else:
        backDate = 1 #"Beginning of day"
    return backDate

def submitAPIcall(self, fn, *args, **kwargs):
    fut = APIexecutor.submit(fn, *args, **kwargs)
    self.apiFutures.add(fut)
    fut.add_done_callback(self.apiFutures.discard)  # remove when done
    return

def updateStatus(self, pool,totalLoops, status = "Working"):
    try:
        failure = any(self.workerProgress.get(progKey).get("status") == "Failed" for progKey in self.workerProgress)
        if pool == 'DummyFail':
            self.workerProgress['DummyFail'] = {'pool' : pool, 'completed' : 1, 'total' : totalLoops, 'status' : status}
        elif status == "Initialization":
            self.workerProgress[pool] = {'pool' : pool, 'completed' : -1, 'total' : totalLoops, 'status' : status}
        elif status == "Working":
            self.workerProgress[pool]["completed"] += 1
            self.workerProgress[pool]["status"] = status
        elif status == "Completed":
            self.workerProgress[pool]["completed"] += 1
            self.workerProgress[pool]["status"] = status
        else:
            self.workerProgress[pool]["status"] = status
    except Exception as e:
        print(f"Error updating status: {e}")
    return failure

def poll_queue():
    try:
        while True:
            callback = gui_queue.get_nowait()
            if callback:
                try:
                    callback()  # Run the GUI update in the main thread
                except Exception as e:
                    trace = traceback.format_exc()
                    print(f"Error occured while attempting to run background gui update: {e}. \n traceback: \n {trace}")
    except queue.Empty:
        pass

def handleDuplicateFields(rows, fields):
    #adds spaces to any fields with duplicate values so the values will not point back to the same field
    try:
        fieldDict = defaultdict(set)
        spaceDict = {f : " " * (i + 1) for i, f in enumerate(fields)}
        for row in rows:
            for field in fields:
                fieldDict[field].add(row[field])
        duplicates = set()
        for f, vals in fieldDict.items():
            for f2 in (f2 for f2 in fieldDict if f2 != f):
                duplicates.update(vals.intersection(fieldDict[f2]))
        if duplicates:
            for i, r in enumerate(rows):
                for field in fields:
                    if r[field] in duplicates:
                        rows[i][field] = r[field] + spaceDict[field]
            print(f"Duplicate values processed: {duplicates}")
    except Exception as e:
        print(f"ERROR: failed to handle duplicate fields: {e.args}")
    return rows
def filt2Query(db, filterDict : dict[MultiSelectBox], startDate : datetime, endDate : datetime) -> (str,list[str]):
    condStatement = ""
    parameters = []
    invSelections = filterDict["Source name"].checkedItems()
    famSelections = filterDict["Family Branch"].checkedItems()
    if invSelections != [] or famSelections != []: #handle investor level
        invsF = set()
        for fam in famSelections:
            invsF.update(db.pullInvestorsFromFamilies(fam))
        invsI = set(invSelections)
        if invSelections != [] and famSelections != []: #Intersect if both are selected
            invs = invsF.intersection(invsI)
        else: #Union if only one is valid
            invs = invsF.union(invsI)
        placeholders = ','.join('?' for _ in invs)
        if condStatement in ("", " WHERE"):
            condStatement = f' WHERE [Source name] in ({placeholders})'
        else:
            condStatement += f' AND [Source name] in ({placeholders})'
        parameters.extend(invs)
    if filterDict['Node'].checkedItems() != []:
        selectedNodes = filterDict['Node'].checkedItems()
        sNodeIds = [" "+ str(node['id'])+" " for node in db.fetchNodes() if str(node['id']) in selectedNodes]
        # Build LIKE conditions to check if any node ID appears within the nodePath column
        if sNodeIds:
            likeConditions = ' OR '.join('[nodePath] LIKE ?' for _ in sNodeIds)
            if condStatement in (""," WHERE"):
                condStatement = f"WHERE ({likeConditions})"
            else:
                condStatement += f" AND ({likeConditions})"
            # Add each node ID with wildcards to search for it within the column
            for sNodeId in sNodeIds:
                parameters.append(f'%{sNodeId}%')
        else:
            print(f"Warning: Failed to find corresponding node Id's for {selectedNodes}")
    filterParamDict = {}
    for filter in masterFilterOptions:
        if filter["key"] not in nonFundCols:
            if filterDict[filter["key"]].checkedItems() != []:
                filterParamDict[filter['key']] = filterDict[filter["key"]].checkedItems()
    if filterParamDict:
        if condStatement == "":
            condStatement = " WHERE"
        filteredFunds = db.pullFundsFromFilters(filterParamDict)
        for param in filteredFunds:
            parameters.append(param)
        placeholders = ','.join('?' for _ in filteredFunds)
        if condStatement in ("", " WHERE"):
            condStatement = f"WHERE [Target name] IN ({placeholders})"
        else:
            condStatement += f" AND [Target name] IN ({placeholders})"
    # Add time filter to condStatement for database-level filtering
    startDateStr = startDate.strftime("%Y-%m-%d %H:%M:%S") #TODO: is this even necessary?
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    if condStatement == "":
        condStatement = f" WHERE [dateTime] >= ? AND [dateTime] <= ?"
    else:
        condStatement += f" AND [dateTime] >= ? AND [dateTime] <= ?"
    parameters.append(startDateStr)
    parameters.append(endDateStr)
    return condStatement,parameters
def headerUnits(headers):
    units = []
    for h in headers:
        if h in smallHeaders:
            units.append(1)
        else:
            units.append(2.5)
    total_units = sum(units)
    return units, total_units
