from scripts.importList import *
from scripts.commonValues import *
from scripts.instantiate_basics import *

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

def handleFundClasses(entryList):
    split = {}
    foundDuplicate = False
    for entry in entryList: #split the entries by fundclass to check for duplicates
        fundClass = entry.get(nameHier["FundClass"]["dynLow"])
        if fundClass not in split:
            split[fundClass] = [entry,]
        else:
            split[fundClass].append(entry)
            foundDuplicate = True
    singleEntries = []
    if foundDuplicate: #if duplicates, loop through to find the best balance type
        for fundClass in split: #loop by fund
            if len(split.get(fundClass)) > 1: #check if duplicates
                foundType = False
                for balanceType in balanceTypePriority: #loop through balance types by priority
                    for entry in split.get(fundClass): #loop through the duplicate entries
                        if entry.get("Balancetype") == balanceType and entry.get(nameHier["Value"]["dynLow"]) not in (None,"None"): #if the balance type is preferred, add the entry and break
                            singleEntries.append(entry)
                            foundType = True
                            break
                    if foundType: #stop balance type checking if found
                        break
                if not foundType: #reaches if nothing was found
                    for entry in split.get(fundClass): #loop through to find the first with a value
                        if entry.get(nameHier["Value"]["dynLow"]) not in (None,"None"): #if the balance type is preferred, add the entry and break
                            singleEntries.append(entry)
                            foundType = True
                            break
                    if not foundType: #final attempt take first entry
                        singleEntries.append(split.get(fundClass)[0])
            else: #no duplicates for this fund
                singleEntries.append(split.get(fundClass)[0])
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
    from collections import deque, defaultdict

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

def findNodeStructure(sources,nodes,targets, entries):
    nodeStruc = {node : {'id' : idx, 'name' : node, 'lowestLevel' : 0, 'above' : set(), 'below' : set()} for idx, node in enumerate(sorted(nodes))}
    searchEntryDict = {f"{entry['Source name']} > {entry['Target name']}" : entry for entry in entries if entry['Source name'] not in sources and entry['Target name'] not in targets} #only node to node data is helpful
    #cuts down to only unique source --> target values
    searchEntries = [entry for entry in searchEntryDict.values()]
    idx = 0
    while True and idx < 10: #max idx as 10 as a safety measure
        changeMade = False
        for entry in searchEntries:
            src = entry['Source name']
            tgt = entry['Target name']
            #if a target is from a source, it must be at LEAST one level beneath. Another source may push it further
            if nodeStruc[tgt]['lowestLevel'] < nodeStruc[src]['lowestLevel'] + 1:
                nodeStruc[tgt]['lowestLevel'] = nodeStruc[src]['lowestLevel'] + 1
                changeMade = True
            nodeStruc[src]['below'].add(nodeStruc[tgt]['id'])
            nodeStruc[tgt]['above'].add(nodeStruc[src]['id'])
        if not changeMade:
            break
        idx += 1
        if idx == 9:
            print("Warning: node structure search has reached maximum index. Should not occur")
    print(f"maximum node depth [zero index]: {idx}")
    return nodeStruc

def findNodes(table1, table2 = None):
    tableEntries = [*table1,*table2] if table2 else table1
    targets = set(entry.get("Target name") for entry in tableEntries)
    sources = set(entry.get("Source name") for entry in tableEntries)
    nodes = targets & sources
    targets = targets - nodes
    sources = sources - nodes
    return targets,sources,nodes

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
def accountBalanceKey(accEntry : dict):
    try:
        key = accEntry["Date"] + "_" + accEntry["Source name"] + "_" + accEntry["Target name"]
        for accountField in ("Balancetype"):
            key += accEntry.get(accountField, "") if accEntry.get(accountField, "") is not None else ""
    except:
        print(f"Failed for entry: {accEntry}")
        raise
    return key
def annualizeITD(cumITD, monthCount):
    if monthCount < 12: #ITD for less than a year is essentially YTD style
        return (cumITD - 1) * 100
    elif cumITD > 0:
        return (cumITD ** (12/monthCount)) - 1
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