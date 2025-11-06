from scripts.importList import *
from scripts.commonValues import *
from scripts.instantiate_basics import *
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
def accountBalanceKey(accEntry):
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
        if status == "Initialization":
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