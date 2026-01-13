from PyQt5.QtWidgets import QApplication, QMessageBox
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas as pd
import threading

from scripts.basicFunctions import separateRowCode

def portfolioSnapshot(calcs, trans, rApp, investors :list[str], asOfDate):
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
    try:
        print('Processing report snapshot data...')
        AFdf = dataFrames['assets and flows']
        aFdict = {name : [0,0,0] for name in ('Starting Value','Ins','(Outs)','Gains/(Losses)','Ending Value')}
        prevMonthDt = asOfDate - relativedelta(months=1)
        yrMonthDt = asOfDate - relativedelta(years=1)
        eomDT = asOfDate + relativedelta(months = 1) - relativedelta(days = 1)
        print(f'as of dt: {asOfDate}')
        print(f'prev month dt: {prevMonthDt}')
        print(f'eom dt: {eomDT}')
        print(f'yr dt: {yrMonthDt}')
        print(f'Processing {len(calcs)} calculations...')
        for c in calcs:
            cDt = datetime.strptime(c['dateTime'],'%Y-%m-%d 00:00:00')
            NAV = c['NAV']
            #Handle ending val (add 0 index value to 1 and 2 once done as they are the same)
            if cDt == asOfDate:
                aFdict['Ending Value'][0] += NAV
            #Handle Gain
            if  cDt < eomDT:#before as of date
                mGain = c['Monthly Gain']
                aFdict['Gains/(Losses)'][2] += mGain
                if cDt >= yrMonthDt: #within the year
                    aFdict['Gains/(Losses)'][1] += mGain
                    if cDt == asOfDate: #the months gain
                        aFdict['Gains/(Losses)'][0] += mGain
            #Handle starting val
            if cDt == prevMonthDt:
                aFdict['Starting Value'][0] += NAV
            elif cDt == yrMonthDt:
                aFdict['Starting Value'][1] += NAV
        for num in (1,2):
            aFdict['Ending Value'][num] = aFdict['Ending Value'][0]
        print(f'Processing {len(trans)} transactions...')
        for t in trans:
            tDt = t['Date'].replace('T',' ')
            tDt = datetime.strptime(tDt,'%Y-%m-%d 00:00:00')
            cashFlow = t.get('CashFlowSys')
            if cashFlow in (None,'None',0,0):
                continue
            #Handle in/outs
            if tDt <= eomDT: #before as of date
                if cashFlow < 0: #since inception
                    aFdict['Ins'][2] -= cashFlow
                else:
                    aFdict['(Outs)'][2] -= cashFlow
                if tDt >= yrMonthDt: #within year
                    if cashFlow < 0:
                        aFdict['Ins'][1] -= cashFlow
                    else:
                        aFdict['(Outs)'][1] -= cashFlow
                    if tDt >= asOfDate: #within month selected
                        if cashFlow < 0:
                            aFdict['Ins'][0] -= cashFlow
                        else:
                            aFdict['(Outs)'][0] -= cashFlow
        print('Finalizing dataframe...')
        for key in aFdict.keys():
            for idx in range(len(aFdict[key])):
                aFdict[key][idx] /= 1000000 #convert to show by millions
        AFdf = pd.DataFrame(
            [
                [*aFdict['Starting Value']],
                [*aFdict['Ins']],
                [*aFdict['(Outs)']],
                [*aFdict['Gains/(Losses)']],
                [*aFdict['Ending Value']]
            ],
            columns=['Month', '1 Year', 'Inception'],
            index=['Starting Value', 'Ins', '(Outs)', 'Gains/(Losses)', 'Ending Value']
        )
        AFdf.insert(0, '$ Millions', AFdf.index)
        AFdf.reset_index(drop=True, inplace=True)
        dataFrames['assets and flows'] = AFdf

        #Performance Data Collection by table builds
        rApp.instantiateFilters()
        rApp.filterDict['Source name'].setCheckedItems(investors)
        rApp.sortHierarchy.setCheckedItem("assetClass")
        rApp.filterRadioBtnDict['Target name'].setChecked(False)
        # HF Portfolio ------------------------------------
        benches = ['Overall Policy Benchmark','Overall Implementation Benchmark','60% MSCI ACWI / 40% BB Aggregate']
        rApp.benchmarkSelection.setCheckedItems(benches)
        cancelEvent = threading.Event() #useless here but the function wants it
        rApp.buildTable(cancelEvent)
        table = rApp.currentTableData
        keyLinks = {'NAV' : '$MM', 'MTD' : 'Month', 'YTD' : 'YTD',
                    '1YR' : '1 Year', '3YR' : '3 Year' ,'ITD':  'Inception' }
        rowHeaders = ['Illiquid','Liquid','Cash','HF Capital',*benches]
        acSearchDict = {item : item for item in ('Illiquid','Liquid',*benches)} #dict for rowKey to excel table row name
        acSearchDict['Total'] = 'HF Capital'
        acSearchDict['Cash '] = 'Cash'
        df = buildPerfDF(table,keyLinks,rowHeaders,acSearchDict,['NAV',])
        dataFrames['portfolio returns'] = df

        # Foundations ------------------------------------
        rApp.filterDict['Classification'].clearSelection()
        rApp.filterDict['Classification'].setCheckedItem('Foundation')
        rApp.sortHierarchy.clearSelection()
        rApp.sortHierarchy.setCheckedItem("subAssetClass")
        rApp.buildTable(cancelEvent)
        table = rApp.currentTableData
        keyLinks = {'NAV' : '$MM', '%' : '% Allocation'}
        rowHeaders = ['Absolute Return','Fixed Income','Public Equity','Long/Short','Cash','Total']
        searchDict = {item : item for item in ('Absolute Return','Fixed Income','Public Equity','Long/Short','Cash','Total')} #dict for rowKey to excel table row name
        searchDict['Cash  '] = 'Cash'
        df = buildPerfDF(separateRowCode,table,keyLinks,rowHeaders,searchDict,['NAV',],'Asset Class')
        dataFrames['foundations'] = df
        #
    except:
        rApp.setEnabled(True) #release the app back to the user
        blockMSG.destroy()
        raise
    rApp.setEnabled(True) #release the app back to the user
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
    return dataFrames

def buildPerfDF(table,keyLinks,rowHeaders,searchDict, cashHeaders = [], cornerTxt = ''):
    perfDictTemplate = {val : 0.0 for _,val in keyLinks.items()}
    perfDict = {val : perfDictTemplate.copy() for _, val in searchDict.items()}
    for rowKey, rowDict in table.items(): #iterate through table with performance data
        row_name, _ = separateRowCode(rowKey)
        if row_name in searchDict.keys(): #check if the row has the relevant data
            prName = searchDict[row_name]
            for orig, pr in keyLinks.items(): #for each datapoint, pull the table data
                val = rowDict.get(orig)
                if val:
                    if orig not in cashHeaders:
                        val = f'{val}%'
                    else:
                        val = val / 1000000
                else:
                    val = ''
                perfDict[prName][pr] = val
    df = pd.DataFrame.from_dict(perfDict, orient='index')
    df = df.loc[rowHeaders].copy()  # Ensure the order matches row_order
    df.insert(0, cornerTxt, rowHeaders)  # Keep the row headers in a visible column
    df.reset_index(drop=True, inplace=True)
    return df