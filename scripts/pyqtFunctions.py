
import os
from PyQt5.QtWidgets import QFileDialog, QMessageBox
from datetime import datetime
from dateutil.relativedelta import relativedelta
from scripts.commonValues import maxPDFheaderUnits
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
    if unitMax > maxPDFheaderUnits:
        r = QMessageBox.question(self,'Continue?','Warning: too many headers selected for pdf export. Export may not format well. Continue?')
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