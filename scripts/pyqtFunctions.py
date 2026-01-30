
import os
from PyQt5.QtWidgets import QFileDialog, QMessageBox
from datetime import datetime
from dateutil.relativedelta import relativedelta
from classes.DatabaseManager import DatabaseManager
from classes.widgetClasses import MultiSelectBox
from scripts.commonValues import fullPortStr, masterFilterOptions, maxPDFheaderUnits, nonFundCols, sqlPlaceholder
from scripts.instantiate_basics import ASSETS_DIR
from scripts.render_report import render_report
from scripts.basicFunctions import headerUnits

from PyQt5.QtWidgets import QApplication, QMessageBox
import threading

def basicHoldingsReportExport(self , sourceName = None, classification = None):
    if not hasattr(self,'filteredReturnsTableData'):
        QMessageBox.warning(self,'No Table Loaded Yet','WARNING: No table has been loaded and formatted yet for export. Cancelling...')
        return
    elif self.buildTableLoadingBar.isVisible():
        QMessageBox.warning(self,'New table processing','WARNING: The table is currently rebuilding. Allow the table to fully build before attempting to export it. Cancelling...')
        return
    data = self.filteredReturnsTableData
    if self.headerSort.active:
        headerOrder = self.headerSort.popup.get_checked_sorted_items()
    else:
        headerOrder = None
    _,unitMax = headerUnits(headerOrder)
    print(f'Max header units {unitMax}')
    if unitMax > maxPDFheaderUnits + 4:
        r = QMessageBox.question(self,'Continue?','Warning: More headers selected than the recommended maximum for pdf export. Text may be very small or poorly formatted. Continue?')
        if not r or r != QMessageBox.Yes:
            return
    tempDirPath = os.path.join(ASSETS_DIR,'temp')
    if not os.path.exists(tempDirPath):
        os.mkdir(tempDirPath)
    outPath = os.path.join(ASSETS_DIR, 'temp','tempHoldingsReport.pdf')
    #build_holdings_pdf(outPath, data)
    report_date = self.dataEndSelect.currentText()
    report_date = datetime.strptime(report_date,'%B %Y')
    footerData = {'reportDate' : report_date, 'portfolioSource' : sourceName, 'classification' : classification, 'headerUnits' : unitMax}
    render_report(outPath,data,self.tableColorDepths, holdings_header_order=headerOrder, footerData= footerData, onlyHoldings = True)

def controlTable(rApp, reset : bool = False, reenable : bool = True, filterChoices : dict[list] = {}, sortHierarchy : list[str] = None, benchmarks : list[str] = None, visChoices : dict[bool] = {}, endDate : datetime = None):
    try:
        rApp.setEnabled(False) #hold the entire app from user input
        QMessageBox.informativeText
        blockMSG = QMessageBox(rApp)
        blockMSG.setWindowTitle('Notice')
        blockMSG.setText('Application will be frozen until the report generation is complete.')
        blockMSG.setStandardButtons(QMessageBox.NoButton)
        blockMSG.setModal(False)  # Make it non-modal so it doesn't block
        blockMSG.show()
        QApplication.processEvents()
        #Begin controls -------------
        if reset:
            rApp.instantiateFilters()
        for key, choices in filterChoices.items():
            rApp.filterDict[key].clearSelection()
            rApp.filterDict[key].setCheckedItems(choices)
        if sortHierarchy:
            rApp.sortHierarchy.clearSelection()
            rApp.sortHierarchy.setCheckedItems(sortHierarchy)
        for key, boolC in visChoices.items():
            rApp.filterRadioBtnDict[key].setChecked(boolC)
        if benchmarks:
            rApp.benchmarkSelection.clearSelection()
            rApp.benchmarkSelection.setCheckedItems(benchmarks)
        if endDate:
            rApp.dataEndSelect.setCurrentText(endDate.strftime('%B %Y'))
        #Build table -----
        cancelEvent = threading.Event() #useless here but the function wants it
        rApp.buildTable(cancelEvent)
        QApplication.processEvents()
        rApp.populateReturnsTable(rApp.currentTableData, rApp.currentTableFlags) #enforces full table processing. Will populate twice
        table = rApp.filteredReturnsTableData
        blockMSG.destroy()
        rApp.setEnabled(reenable)
        return table
    except:
        blockMSG.destroy()
        rApp.setEnabled(True)
        raise
def comboInvestorOpts(db: DatabaseManager, invSelections,famSelections):
    if invSelections != [] or famSelections != []:
        invsF = set()
        for fam in famSelections:
            invsF.update(db.pullInvestorsFromFamilies(fam))
        invsI = set(invSelections)
        if invSelections != [] and famSelections != []: #Intersect if both are selected
            invs = invsF.intersection(invsI)
        else: #Union if only one is valid
            invs = invsF.union(invsI)
    return invs
def filt2Query(db, filterDict : dict[MultiSelectBox], startDate : datetime, endDate : datetime, invSort:bool = False) -> (str,list[str]):
    condStatement = ""
    parameters = []
    invSelections = filterDict["Source name"].checkedItems()
    famSelections = filterDict["Family Branch"].checkedItems()
    if invSelections != [] or famSelections != []: #handle investor level
        invs = comboInvestorOpts(db,invSelections,famSelections)
        placeholders = ','.join(sqlPlaceholder for _ in invs)
        if condStatement in ("", " WHERE"):
            condStatement = f' WHERE [Source name] in ({placeholders})'
        else:
            condStatement += f' AND [Source name] in ({placeholders})'
        parameters.extend(invs)
    elif invSort: #if grouped by an investor level, must pull individualized data
        condStatement = f' WHERE [Source name] != {sqlPlaceholder}'
        parameters.append(fullPortStr)
    else: #if no investor selection, the full portfolio values will work the same and be faster
        condStatement = f' WHERE [Source name] = {sqlPlaceholder}'
        parameters.append(fullPortStr)
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