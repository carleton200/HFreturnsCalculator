import traceback
from PyQt5.QtWidgets import QApplication, QMessageBox
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas as pd
import threading

from scripts.basicFunctions import separateRowCode
from scripts.commonValues import defaultSports

def portfolioSnapshot(calcs, rApp, investors :list[str], asOfDate : datetime):
    sheetNames = ['assets_and_flows', 'portfolio returns', 'foundations', 
                'overall_family_breakdown', 'overall_family_breakdown2', 'returns_vs_benchmark']
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
        AFdf = dataFrames['assets_and_flows']
        aFdict = {name : [0,0,0] for name in ('Starting Value','Ins','(Outs)','Gains/(Losses)','Ending Value')}
        prevMonthDt = asOfDate - relativedelta(months=1)
        yrMonthDt = asOfDate - relativedelta(years=1)
        eomDT = asOfDate + relativedelta(months = 1) - relativedelta(days = 1)
        print(f'as of dt: {asOfDate}')
        print(f'prev month dt: {prevMonthDt}')
        print(f'eom dt: {eomDT}')
        print(f'yr dt: {yrMonthDt}')
        print(f'Processing {len(calcs)} calculations...')
        def addInOut(aFdict,lvl : int,contributions,redemptions,distributions, subtract:bool = False):
            if subtract:
                modify = -1
            else:
                modify = 1
            if contributions not in (None,'None','',0.0):
                aFdict['Ins'][lvl] += contributions * modify
            if redemptions not in (None,'None','',0.0):
                aFdict['(Outs)'][lvl] += redemptions * modify
            if distributions not in (None,'None','',0.0):
                aFdict['(Outs)'][lvl] += distributions * modify
            return aFdict
        for c in calcs:
            cDt = datetime.strptime(c['dateTime'],'%Y-%m-%d 00:00:00')
            NAV = c['NAV']
            contributions = c.get('Contributions')
            redemptions = c.get('Redemptions')
            distributions = c.get('Distributions')
            #Handle ending val (add 0 index value to 1 and 2 once done as they are the same)
            # 0: Month, 1: 1 Year   2: Inception
            if cDt == asOfDate:
                aFdict['Ending Value'][0] += NAV
                aFdict = addInOut(aFdict,0,contributions,redemptions,distributions) #add current to subtract the previous month
                aFdict = addInOut(aFdict,1,contributions,redemptions,distributions) #add current to subtract the previous year
                aFdict = addInOut(aFdict,2,contributions,redemptions,distributions) #current is the Inception
            elif cDt == prevMonthDt:
                aFdict = addInOut(aFdict,0,contributions,redemptions,distributions,subtract=True) #Month is Current minus previous month's
            elif cDt == yrMonthDt:
                aFdict = addInOut(aFdict,1,contributions,redemptions,distributions,subtract=True) #1 Year is Current minus previous year's
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
        dataFrames['assets_and_flows'] = AFdf

        #Performance Data Collection by table builds
        rApp.instantiateFilters()
        rApp.filterDict['Source name'].setCheckedItems(investors)
        rApp.sortHierarchy.setCheckedItem("assetClass")
        rApp.filterRadioBtnDict['Target name'].setChecked(False)
        # HF Portfolio ------------------------------------
        benches = ['Overall Policy Benchmark','Overall Implementation Benchmark','60% MSCI ACWI / 40% BB Aggregate']
        rApp.benchmarkSelection.setCheckedItems(benches)
        cancelEvent = threading.Event() #useless here but the function wants it
        rApp.buildTable(cancelEvent, populateTable = False)
        table = rApp.currentTableData
        keyLinks = {'NAV' : '$MM', 'MTD' : 'Month', 'YTD' : 'YTD',
                    '1YR' : '1 Year', '3YR' : '3 Year' ,'ITD':  'Inception' }
        rowHeaders = ['Illiquid','Liquid','Cash','HF Capital',*benches]
        acSearchDict = {item : item for item in ('Illiquid','Liquid',*benches)} #dict for rowKey to excel table row name
        acSearchDict['Total'] = 'HF Capital'
        acSearchDict['Cash '] = 'Cash'
        df = buildPerfDF(table,keyLinks,rowHeaders,acSearchDict,cashHeaders=['NAV',])
        dataFrames['portfolio returns'] = df

        # Foundations ------------------------------------
        rApp.filterDict['Classification'].clearSelection()
        rApp.filterDict['Classification'].setCheckedItem('Foundation')
        rApp.sortHierarchy.clearSelection()
        rApp.sortHierarchy.setCheckedItem("subAssetClass")
        rApp.buildTable(cancelEvent, populateTable = False)
        table = rApp.currentTableData
        keyLinks = {'NAV' : '$MM', '%' : '% Allocation'}
        rowHeaders = ['Absolute Return','Fixed Income','Public Equity','Long/Short','Cash','Total']
        searchDict = {item : item for item in ('Absolute Return','Fixed Income','Public Equity','Long/Short','Cash','Total')} #dict for rowKey to excel table row name
        searchDict['Cash  '] = 'Cash'
        df = buildPerfDF(table,keyLinks,rowHeaders,searchDict,cashHeaders=['NAV',],cornerTxt='Asset Class')
        dataFrames['foundations'] = df
        #Overal Family Breakdown -------------------------------------
        rApp.filterDict['Classification'].clearSelection()
        rApp.sortHierarchy.clearSelection()
        rApp.sortHierarchy.setCheckedItems(["Classification",'subClassification'])
        rApp.filterDict['Classification'].setCheckedItems(['Foundation','HFC','Non-HFC'])
        rApp.monthlyTableBtn.setChecked(True)
        rApp.returnOutputType.setCurrentText('NAV')
        rApp.buildTable(cancelEvent, populateTable = False)
        table = rApp.currentTableData
        currMonth = datetime.strftime(asOfDate,'%B %Y')
        prevMonth = datetime.strftime(prevMonthDt,'%B %Y')
        keyLinks = {prevMonth : 'LM $MM', 1:"delta_mm", currMonth : 'CM $MM', 0 : '%'}
        rowHeaders = ['HFC','Gate City Energy','Non-HFC','Pilot','Sports','HFC Foundation','Non-HFC Foundation','Total']
        searchDict = {item : item for item in rowHeaders} #dict for rowKey to excel table row name
        df = buildPerfDF(table,keyLinks,rowHeaders,searchDict,cashHeaders=['LM $MM','CM $MM'],cornerTxt='Asset', special = 'ofb')
        dataFrames['overall_family_breakdown'] = df
        #Sports Data ----------------------------------------
        rowHeaders = (*defaultSports, 'Total')
        sportsData = rApp.db.loadFromDB('sportsData',condStatement = ' WHERE month = ?', inputs = (currMonth,))
        if investors:
            invPh = ','.join('?' for _ in investors)
            investorSportsData = rApp.db.loadFromDB('investorSportsData', f' WHERE month = ? AND investor IN ({invPh})', inputs =(currMonth,*investors))
        else:
            investorSportsData = [] #will just equal the total if full portfolio. Don't pull data for no reason
        sportsDict = {}
        for sport in defaultSports:
            sportData = [r for r in sportsData if r.get('team') == sport]
            if len(sportData) == 1:
                sDget = sportData[0].get
                equity = sDget('equity','')
                sportsDict[sport] = {'share_pct' : sDget('share',''), 'team_value' : sDget('teamValue',''), 
                                        'debt' : sDget('debt',''), 'equity' : equity}
                try:
                    equity = float(equity)
                except:
                    continue
                if investors:
                    famShare = sportsDict[sport]['equity']
                    try:
                        famShare = float(famShare)
                    except:
                        continue
                    invSpData = [r for r in investorSportsData if r.get('team') == sport]
                    for r in invSpData:
                        own = r.get('ownership')
                        try:
                            own = float(own)
                        except:
                            continue
                        famShare += equity * own / 100
                else: #no investor filter means full portfolio so the full value
                    famShare = equity
                sportsDict[sport]['family_share'] = famShare
                if 'Total' not in sportsDict:
                    sportsDict['Total'] = {'team_value' : 0.0, 'debt' : 0.0, 'equity' : 0.0, 'family_share' : 0.0}
                for h in ('team_value','debt','equity','family_share'):
                    sportsDict['Total'][h] += sportsDict[sport][h]
        if sportsDict:
            sportDf = dictToExcelDf(sportsDict,[h for h in rowHeaders if h in sportsDict],'sports')
        dataFrames['overall_family_breakdown2'] = sportDf
        #Returns vs Benchmarks ------------------------------
        rApp.monthlyTableBtn.setChecked(False)
        rApp.complexTableBtn.setChecked(True)
        rApp.filterDict['Classification'].clearSelection()
        rApp.filterDict['Classification'].setCheckedItem('HFC')
        rApp.sortHierarchy.clearSelection()
        rApp.sortHierarchy.setCheckedItem('subAssetClass')
        rApp.showBenchmarkLinksBtn.setChecked(True)
        rApp.buildTable(cancelEvent, populateTable = False)
        table = rApp.currentTableData
        benchLinks = rApp.db.fetchBenchmarkLinks()

 
        "mm",
        "alloc_pct", "tgt_pct", "alloc_delta",
        "m_rtn", "m_bm", "m_delta",
        "ytd_rtn", "ytd_bm", "ytd_delta",
        "y1_rtn", "y1_bm", "y1_delta",
        "y3_rtn", "y3_bm", "y3_delta",
        "inc_rtn", "inc_bm", "inc_delta"            

        keyLinks = {'NAV' : 'mm', '%':"alloc_pct", 0 : 'tgt_pct', 1 : 'alloc_delta', 
                    'MTD':'m_rtn',2 : 'm_bm', 3: 'm_delta',
                    'YTD':'ytd_rtn',4 : 'ytd_bm', 5: 'ytd_delta',
                    '1YR':'y1_rtn',6 : 'y1_bm', 7: 'y1_delta',
                    '3YR':'y3_rtn',8 : 'y3_bm', 9: 'y3_delta',
                    'ITD':'inc_rtn',10 : 'inc_bm', 11: 'inc_delta',
                    }
        rowHeaders = ['Direct Private Equity','Private Equity','Direct Real Assets','Real Assets','Public Equity','Long/Short','Absolute Return','Fixed Income','Cash  ','Total']
        searchDict = {item : item for item in rowHeaders} #dict for rowKey to excel table row name
        df = buildPerfDF(table,keyLinks,rowHeaders,searchDict,cashHeaders=['mm'],cornerTxt='asset_class', special = 'rvb', params = benchLinks)
        dataFrames['returns_vs_benchmark'] = df

    except:
        print(traceback.format_exc())
        rApp.setEnabled(True) #release the app back to the user
        blockMSG.destroy()
        raise
    rApp.populateReturnsTable(rApp.currentTableData,flagStruc = rApp.currentTableFlags)
    rApp.setEnabled(True) #release the app back to the user
    blockMSG.destroy()
    # Create an Excel writer using pandas (with openpyxl engine)
    # Since we don't know output filename requirement, use an in-memory object or a stub filename if needed
    # For this function, let's return the writer (or the Excel file path if saving), but here let's do minimal example

    # Create an empty DataFrame for each sheet and write to Excel
    try:
        with pd.ExcelWriter("portfolio_snapshot_TEST.xlsx", engine="openpyxl") as writer:
            for name in sheetNames:
                # Make an empty DataFrame for this sheet, so sheet exists
                df = dataFrames[name]
                # Sheet names in pandas/Excel can't be longer than 31 characters, so truncate if needed
                clean_name = str(name)[:31]
                df.to_excel(writer, sheet_name=clean_name, index=False)
    except Exception as e:
        print(f'     WARNING: The snapshot data failed to save to the test exel: {e.args}')
    return dataFrames

def buildPerfDF(table,keyLinks,rowHeaders,searchDict, cashHeaders = [], cornerTxt = '', special = None, params = None):
    perfDictTemplate = {val : 0.0 for _,val in keyLinks.items()}
    perfDict = {val : perfDictTemplate.copy() for _, val in searchDict.items()}
    if special == 'rvb':
        benchLinks = {b.get('asset') : b for b in params if b.get('assetLevel') == 2}
    else:
        benchLinks = {}
    currentAC = None
    for rowKey, rowDict in table.items(): #iterate through table with performance data
        row_name, _ = separateRowCode(rowKey)
        for orig, pr in keyLinks.items(): #for each datapoint, pull the table data
            benchBool = currentAC in benchLinks and currentAC in searchDict and row_name == benchLinks[currentAC].get('benchmark')
            if ('bm' in pr or 'delta' in pr or 'rtn' not in pr) and benchBool:
                continue
            if row_name in searchDict.keys(): #check if the row has the relevant data
                prName = searchDict[row_name]
            elif benchBool:
                prName = searchDict[currentAC] #use previous row header for linked benchmarks
            else:
                continue
            val = rowDict.get(orig)
            if val:
                if orig not in cashHeaders and pr not in cashHeaders:
                    val = f'{val:.2f}%'
                else:
                    val = val / 1000000
            else:
                val = ''
            if benchBool:
                pr = pr.replace('rtn','bm') #if benchmark, use the benchmark header
            perfDict[prName][pr] = val
            if benchBool and val and 'bm' in pr:
                portRtn = float(perfDict[prName].get(pr.replace('bm','rtn'),'').strip('%'))
                benchRtn = float(perfDict[prName].get(pr).strip('%'))
                if portRtn and benchRtn:
                    perfDict[prName][pr.replace('bm','delta')] = f'{(portRtn - benchRtn)}%'
        currentAC = row_name
    if special ==  'ofb':
        for aClass in perfDict:
            pDaC = perfDict[aClass]
            if aClass == 'Non-HFC':
                for header in ('CM $MM','LM $MM'):
                    for subGroup in ('Pilot','Sports','Gate City Energy'):
                        pDaC[header] = pDaC[header] - perfDict.get(subGroup,{}).get(header,0.0)
            pDaC['%'] = pDaC.get('CM $MM',0.0) / perfDict.get('Total',{}).get('CM $MM',0) * 100 if perfDict.get('Total',{}).get('CM $MM',0) != 0 else 0.0
            pDaC['delta_mm'] = pDaC.get('CM $MM',0.0) - pDaC.get('LM $MM',0.0)

    df = dictToExcelDf(perfDict,rowHeaders,cornerTxt=cornerTxt)
    return df
def dictToExcelDf(dict,rowHeaders,cornerTxt = ''):
    df = pd.DataFrame.from_dict(dict, orient='index')
    df = df.loc[rowHeaders].copy()  # Ensure the order matches row_order
    df.insert(0, cornerTxt, rowHeaders)  # Keep the row headers in a visible column
    df.reset_index(drop=True, inplace=True)
    return df