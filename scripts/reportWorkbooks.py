from PyQt5.QtWidgets import QApplication, QMessageBox
import pandas as pd
import threading

def portfolioSnapshot(calcs, rApp, investors :list[str] = []):
    sheetNames = ['assets and flows', 'portfolio returns', 'foundations', 
                'overall family breakdown', 'overal family breakdown2', 'returns vs benchmark']
                # INSERT_YOUR_CODE
    dataFrames = {name : pd.DataFrame() for name in sheetNames}

    
    #set filter values first then run it
    rApp.setEnabled(False) #hold the entire app from user input
    QMessageBox.informativeText
    blockMSG = QMessageBox(rApp)
    blockMSG.setWindowTitle('Notice')
    blockMSG.setText('Application will be frozen until the report generation is complete.')
    blockMSG.setStandardButtons(QMessageBox.NoButton)
    blockMSG.setModal(False)  # Make it non-modal so it doesn't block
    blockMSG.show()
    QApplication.processEvents()
    rApp.instantiateFilters()
    rApp.filterDict['Source name'].setCheckedItems(investors)
    rApp.benchmarkSelection.setCheckedItems(['Overall Policy Benchmark','Overall Implementation Benchmark','60% MSCI ACWI / 40% BB Aggregate'])
    cancelEvent = threading.Event() #useless here but the function wants it
    rApp.buildTable(cancelEvent)
    table = rApp.currentTableData
    AFdf = dataFrames['assets and flows']
    keyLinks = {'' : '', '$MM' : 'NAV', 'Month' : 'MTD'}



    rApp.setEnabled(True) #hold the entire app from user input
    blockMSG.destroy()
    
    # Create an Excel writer using pandas (with openpyxl engine)
    # Since we don't know output filename requirement, use an in-memory object or a stub filename if needed
    # For this function, let's return the writer (or the Excel file path if saving), but here let's do minimal example

    # Create an empty DataFrame for each sheet and write to Excel
    with pd.ExcelWriter("portfolio_snapshot_TEST.xlsx", engine="openpyxl") as writer:
        for name in sheetNames:
            # Make an empty DataFrame for this sheet, so sheet exists
            df = dataFrames[name]
            # Sheet names in pandas/Excel can't be longer than 31 characters, so truncate if needed
            clean_name = str(name)[:31]
            df.to_excel(writer, sheet_name=clean_name, index=False)