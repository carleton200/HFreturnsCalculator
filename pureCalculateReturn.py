import sys
import os
import json
import subprocess
import sqlite3
import requests
import calendar
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import queue
from dateutil.relativedelta import relativedelta
from PyQt5.QtWidgets import (
    QApplication, QWidget, QStackedWidget, QVBoxLayout,
    QLabel, QLineEdit, QPushButton, QFormLayout,
    QRadioButton, QButtonGroup, QComboBox, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QProgressBar
)
from PyQt5.QtGui import QBrush, QColor
from PyQt5.QtCore import Qt, QTimer

testDataMode = False

executor = ThreadPoolExecutor()
gui_queue = queue.Queue()

def poll_queue():
    try:
        while True:
            callback = gui_queue.get_nowait()
            if callback:
                try:
                    callback()  # Run the GUI update in the main thread
                except Exception as e:
                    print(f"Error occured while attempting to run background gui update: {e}")
    except queue.Empty:
        pass
# Determine assets path, works in PyInstaller bundle or script
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(BASE_DIR, 'assets')
if testDataMode:
    DATABASE_PATH = os.path.join(ASSETS_DIR, 'Acc_Tran_Test.db')
else:
    DATABASE_PATH = os.path.join(ASSETS_DIR, 'Acc_Tran.db')

if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)


mainURL = "https://api.dynamosoftware.com/api/v2.2"

class MyWindow(QWidget):
    def __init__(self, start_index=0):
        super().__init__()
        self.setWindowTitle('Returns Calculator')
        self.setGeometry(100, 100, 1000, 600)

        os.makedirs(ASSETS_DIR, exist_ok=True)
        self.api_key = None
        # main stack
        self.stack = QStackedWidget()
        self.init_api_key_page()
        self.init_form_page()
        self.init_results_page()
        self.stack.setCurrentIndex(start_index)

        main_layout = QVBoxLayout()
        main_layout.addWidget(self.stack)
        self.setLayout(main_layout)

    def init_api_key_page(self):
        page = QWidget()
        layout = QVBoxLayout()
        self.api_label = QLabel('Enter Dynamo API Key:')
        self.api_input = QLineEdit()
        btn = QPushButton('Submit')
        btn.clicked.connect(self.check_api_key)
        layout.addWidget(self.api_label)
        layout.addWidget(self.api_input)
        layout.addWidget(btn)
        page.setLayout(layout)
        self.stack.addWidget(page)

    def init_form_page(self):
        page = QWidget()
        form = QFormLayout()
    

        # navigation to results (loads from DB)
        btn_to_results = QPushButton('Go to Results')
        btn_to_results.clicked.connect(self.show_results)
        form.addRow(btn_to_results)

        # form inputs (submit disabled)
        self.investor_input = QComboBox()
        form.addRow('Investor:', self.investor_input)
        self.radio_group = QButtonGroup()
        self.radio_total = QRadioButton('Total Portfolio')
        self.radio_asset = QRadioButton('Asset')
        self.radio_subasset = QRadioButton('Sub-Asset')
        for rb in (self.radio_total, self.radio_asset, self.radio_subasset):
            self.radio_group.addButton(rb)
        self.radio_total.setChecked(True)
        tl = QHBoxLayout()
        tl.addWidget(self.radio_total)
        tl.addWidget(self.radio_asset)
        tl.addWidget(self.radio_subasset)
        form.addRow('Select Type:', tl)

        self.asset_input = QLineEdit(); self.asset_input.setEnabled(False)
        form.addRow('Asset:', self.asset_input)
        self.subasset_input = QLineEdit(); self.subasset_input.setEnabled(False)
        form.addRow('Sub-asset:', self.subasset_input)
        for rb in (self.radio_total, self.radio_asset, self.radio_subasset):
            rb.toggled.connect(self.update_fields)

        self.month_combo = QComboBox()
        months = ['January','February','March','April','May','June','July','August','September','October','November','December']
        self.month_combo.addItems(months)
        self.month_combo.setCurrentIndex((datetime.now()-relativedelta(months=1)).month-1)
        form.addRow('Month:', self.month_combo)

        self.year_combo = QComboBox()
        years = [str(y) for y in range(datetime.now().year-10, datetime.now().year+1)]
        self.year_combo.addItems(years)
        self.year_combo.setCurrentText(str(datetime.now().year))
        form.addRow('Year:', self.year_combo)

        self.importButton = QPushButton('Import Data')
        self.importButton.clicked.connect(self.beginImport)
        if testDataMode:
            self.importButton.setEnabled(False)
        form.addRow(self.importButton)

        self.clearButton = QPushButton('Clear All Cached Data')
        self.clearButton.clicked.connect(self.resetData)
        form.addRow(self.clearButton)

        self.apiLoadingBarBox = QWidget()
        t2 = QVBoxLayout()
        t2.addWidget(QLabel("Pulling transaction and account data from server..."))
        self.apiLoadingBar = QProgressBar()
        self.apiLoadingBar.setRange(0,100)
        t2.addWidget(self.apiLoadingBar)
        self.apiLoadingBarBox.setLayout(t2)
        self.apiLoadingBarBox.setVisible(False)
        form.addRow(self.apiLoadingBarBox)

        page.setLayout(form)
        self.stack.addWidget(page)
        self.pullInvestorNames()
    
        

    def init_results_page(self):
        page = QWidget()
        layout = QVBoxLayout()
        self.info_label = QLabel('Results')
        layout.addWidget(self.info_label)
        btn_to_form = QPushButton('Go to Form')
        btn_to_form.clicked.connect(lambda: self.stack.setCurrentIndex(1))
        layout.addWidget(btn_to_form)
        self.calculateButton = QPushButton("Recalculate Data")
        self.calculateButton.clicked.connect(lambda: self.calculatePushed())
        layout.addWidget(self.calculateButton)
        self.calculationLoadingBox = QWidget()
        loadLay = QVBoxLayout()
        self.calculationLoadingBar = QProgressBar()
        self.calculationLoadingBar.setRange(0,100)
        self.calculationLabel = QLabel()
        loadLay.addWidget(self.calculationLabel)
        loadLay.addWidget(self.calculationLoadingBar)
        self.calculationLoadingBox.setLayout(loadLay)
        self.calculationLoadingBox.setVisible(False)
        layout.addWidget(self.calculationLoadingBox)

        hl = QHBoxLayout()
        self.resultTable = QTableWidget(); self.resultTable.setSortingEnabled(True)
        hl.addWidget(self.resultTable)
        layout.addLayout(hl)

        

        page.setLayout(layout)
        self.stack.addWidget(page)
    def resetData(self):
        self.save_to_db("calculations",None,action="reset") #reset calculations so new data will be freshly calculated
    def calculatePushed(self):
        executor.submit(self.calculateReturn)
    def beginImport(self):
        self.updateFormVars()
        print(f"'{self.investor}'", self.classType, self.month, self.year)
        executor.submit(self.pullData)
    def updateFormVars(self):
        self.investor = self.investor_input.currentText()
        btn = self.radio_group.checkedButton()
        self.classType = btn.text()
        self.classString = None
        if self.classType == "Asset":
            self.classString = self.asset_input.text()
        elif self.classType == "Sub-asset":
            self.classString = self.subasset_input.text()
        self.month = self.month_combo.currentText()
        self.year = self.year_combo.currentText()
        self.updateMonths()
        

    def updateMonths(self):
        startMonth = int(datetime.strptime(self.month, "%B").month)
        year = str(self.year)
        start = datetime(int(year),int(startMonth),1)
        end = datetime.now()
        index = start
        monthList = []
        while index < end:
            monthList.append(index)
            index += relativedelta(months=1)
        dbDates = []
        firstRun = True
        for monthDT in monthList:
            month = int(monthDT.month)
            year = int(monthDT.year)
            lastDayCurrent = calendar.monthrange(int(year),month)[1]
            lastDayCurrent   = str(lastDayCurrent).zfill(2)
            if month - 1 > 0:
                prevMonth =  month - 1
                prevMyear = year
            else:
                prevMonth = 12
                prevMyear = str(int(year) - 1)
            lastDayPrev = calendar.monthrange(int(prevMyear),prevMonth)[1]
            lastDayPrev   = str(lastDayPrev).zfill(2)
            prevMonth = str(prevMonth).zfill(2)
            month = str(month).zfill(2)
            
            tranStart = f"{year}-{month}-01T00:00:00.000Z"
            bothEnd = f"{year}-{month}-{lastDayCurrent}T00:00:00.000Z"
            accountStart = f"{prevMyear}-{prevMonth}-{lastDayPrev}T00:00:00.000Z"

            if firstRun:
                self.startDate = accountStart
                firstRun = False
            
            dateString = monthDT.strftime("%B %Y")

            monthEntry = {"dateTime" : monthDT, "Month" : dateString, "tranStart" : tranStart.removesuffix(".000Z"), "endDay" : bothEnd.removesuffix(".000Z"), "accountStart" : accountStart.removesuffix(".000Z")}
            dbDates.append(monthEntry)
        self.endDate = bothEnd
        self.save_to_db("Months",dbDates)

    def pullInvestorNames(self):
        accountsHigh = self.load_from_db('positions_high')
        if accountsHigh is not None:
            investors = []
            for account in accountsHigh:
                if account["Source name"] not in investors:
                    investors.append(account["Source name"])
            investors.sort()
            self.allInvestors = investors
            self.investor_input.addItems(investors)
        else:
            self.allInvestors = []
    def update_fields(self):
        self.asset_input.setEnabled(self.radio_asset.isChecked() or self.radio_subasset.isChecked())
        self.subasset_input.setEnabled(self.radio_subasset.isChecked())

    def check_api_key(self):
        key = self.api_input.text().strip()
        if key:
            subprocess.run(['setx','Dynamo_API',key], check=True)
            os.environ['Dynamo_API'] = key
            self.api_key = key
            self.stack.setCurrentIndex(1)
        else:
            self.api_label.setText('API key cannot be empty')

    def show_results(self):
        self.stack.setCurrentIndex(2)

    def pullData(self):
        gui_queue.put(lambda: self.apiLoadingBarBox.setVisible(True))
        gui_queue.put(lambda: self.importButton.setEnabled(False))
        
        startDate = f"2021-01-01T00:00:00.000Z" #around first day for most records
        startDate = self.startDate
        endDate = self.endDate
        self.pullInvestorNames()
        apiData = {
            "tranCols": "Investment in, Investing Entity, Transaction Type, Effective date, Cash flow change",
            "tranName": "InvestmentTransaction",
            "tranSort": "Effective date:desc",
            "accountCols": "As of Date, Balance Type, Asset Class, Sub-asset class, Value of Investments, Investing entity, Investment in",
            "accountName": "InvestmentPosition",
            "accountSort": "As of Date:desc",
        }
        loadingIdx = 0
        for i in range(2):
            cols_key = 'accountCols' if i == 1 else 'tranCols'
            name_key = 'accountName' if i == 1 else 'tranName'
            sort_key = 'accountSort' if i == 1 else 'tranSort'
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "x-columns": apiData[cols_key],
                "x-sort": apiData[sort_key]
            }
            for j in range(2): #0: fund level, 1: pool to high investor level
                gui_queue.put(lambda: self.apiLoadingBar.setValue(int((loadingIdx)/4 * 100)))
                loadingIdx += 1
                investmentLevel = "Investing entity" if j == 0 else "Investment in"
                if i == 0: #transaction
                    if j == 0:
                        payload = {
                        "advf": {
                            "e": [
                                {
                                    "_name": "InvestmentTransaction",
                                    "rule": [
                                        {
                                            "_op": "not_null",
                                            "_prop": "Cash flow change"
                                        },
                                        {
                                            "_op": "all",
                                            "_prop": f"{investmentLevel}",
                                            "values": [
                                                "pool, llc"
                                            ]
                                        },
                                        {
                                            "_op": "between_date",
                                            "_prop": "Effective date",
                                            "values": [
                                                f"{startDate}",
                                                f"{endDate}"
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                        "mode": "compact"
                    }
                    else:
                        payload = {
                        "advf": {
                            "e": [
                                {
                                    "_name": "InvestmentTransaction",
                                    "rule": [
                                        {
                                            "_op": "not_null",
                                            "_prop": "Cash flow change"
                                        },
                                        {
                                            "_op": "all",
                                            "_prop": f"{investmentLevel}",
                                            "values": [
                                                "pool, llc"
                                            ]
                                        },
                                        {
                                            "_op": "between_date",
                                            "_prop": "Effective date",
                                            "values": [
                                                f"{startDate}",
                                                f"{endDate}"
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                        "mode": "compact"
                    }
                    
                else: #account (position)
                    if j == 0:
                        payload = {
                            "advf": {
                                "e": [
                                    {
                                        "_name": "InvestmentPosition",
                                        "rule": [
                                            {
                                                "_op": "any_item",
                                                "_prop": "Balance type",
                                                "values": [
                                                    [
                                                        {
                                                            "id": "23d15ba6-2743-4a32-bce1-5f6a6125e132",
                                                            "es": "L_BalanceType",
                                                            "name": "Actual"
                                                        },
                                                        {
                                                            "id": "e37f6be0-6972-4f48-8228-102ea0e75a67",
                                                            "es": "L_BalanceType",
                                                            "name": "Internal Valuation"
                                                        },
                                                        {
                                                            "id": "eecf766d-4941-451f-b88b-67eb9cd1b7ff",
                                                            "es": "L_BalanceType",
                                                            "name": "Manager Estimate"
                                                        },
                                                        {
                                                            "id": "dc5c0527-94c0-4c28-8895-34bfa73b77a0",
                                                            "es": "L_BalanceType",
                                                            "name": "Custodian Estimate"
                                                        }
                                                    ]
                                                ]
                                            },
                                            {
                                                "_op": "all",
                                                "_prop": f"{investmentLevel}",
                                                "values": [
                                                    "pool, llc"
                                                ]
                                            },
                                            {
                                                "_op": "between_date",
                                                "_prop": "As of date",
                                                "values": [
                                                    f"{startDate}",
                                                    f"{endDate}"
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            },
                            "mode": "compact"
                        }
                    else:
                        payload = {
                                    "advf": {
                                        "e": [
                                            {
                                                "_name": "InvestmentPosition",
                                                "rule": [
                                                    {
                                                        "_op": "all",
                                                        "_prop": "Investment in",
                                                        "values": [
                                                            "pool, llc"
                                                        ]
                                                    },
                                                    {
                                                        "_op": "between_date",
                                                        "_prop": "As of date",
                                                        "values": [
                                                            f"{startDate}",
                                                            f"{endDate}"
                                                        ]
                                                    }
                                                ]
                                            }
                                        ]
                                    },
                                    "mode": "compact"
                                }
                response = requests.post(f"{mainURL}/Search", headers=headers, data=json.dumps(payload))
                if response.status_code == 200:
                    try:
                        data = response.json()
                    except ValueError:
                        continue
                    if isinstance(data, dict):
                        rows = data.get('data', data.get('rows', []))
                    elif isinstance(data, list):
                        rows = data
                    else:
                        rows = []

                    keys_to_remove = {'_id', '_es'}
                    rows = [
                        {k: v for k, v in row.items() if k not in keys_to_remove}
                        for row in rows
                    ]

                else:
                    print(f"Error in API call. Code: {response.status_code}. {response}")
                    try:
                        print(f"Error: {response.json()}")
                        print(f"Headers used:  \n {headers}, \n payload used: \n {payload}")
                    except:
                        pass
                if i == 1:
                    if j == 0:
                        gui_queue.put(lambda: self.save_to_db('positions_low', rows))
                    else:
                        gui_queue.put(lambda:self.save_to_db('positions_high', rows))
                else:
                    if j == 0:
                        gui_queue.put(lambda:self.save_to_db('transactions_low', rows))
                    else:
                        gui_queue.put(lambda:self.save_to_db('transactions_high', rows))
        gui_queue.put(lambda: self.stack.setCurrentIndex(2))
        gui_queue.put(lambda: self.apiLoadingBarBox.setVisible(False))
        while not gui_queue.empty(): #wait to assure database has been updated in main thread before continuing
            time.sleep(0.2)
        self.calculateReturn()
        if not testDataMode:
            gui_queue.put(lambda: self.importButton.setEnabled(True))

    def calculateReturn(self):
        try:
            gui_queue.put(lambda: self.calculateButton.setEnabled(False))
            gui_queue.put(lambda: self.importButton.setEnabled(False))
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(True))
            self.updateFormVars()
            gui_queue.put(lambda : self.stack.setCurrentIndex(2))
            print("Calculating return....")
            highAccounts = self.load_from_db("positions_high")
            pools = []
            poolNames = []
            for item in highAccounts:
                if item["Target name"] not in poolNames:
                    pools.append({"poolName" : item["Target name"], "assetClass" : item["ExposureAssetClass"], "subAssetClass" : item["ExposureAssetClassSub-assetClass(E)"]})
                    poolNames.append(item["Target name"])
            months = self.load_from_db("Months", f"ORDER BY [dateTime] ASC")
            calculations = []
            loadingIdx = 0
            loadingTotal = len(months)
            if self.load_from_db("calculations") == []:
                noCalculations = True
            else:
                noCalculations = False
            for month in months:
                totalNAV = 0
                totalGain = 0
                totalMDdenominator = 0
                monthCalculations = []
                perc = int(loadingIdx/loadingTotal * 100)
                if perc < 0 or perc > 100:
                    print(f"Warning: percentage failure. loading idx: {loadingIdx}, loading total : {loadingTotal}, percentage: {perc}")
                    perc = 0.9 #should not happen but I'm putting a safeguard
                gui_queue.put(lambda: self.calculationLoadingBar.setValue(perc))
                
                #if the calculations for the month have already been complete, pull the old data
                #only checks for more than 2 months ago so newer data may be accounted for
                twoMonthAhead = datetime.strptime(month["dateTime"], "%Y-%m-%d %H:%M:%S") + relativedelta(months=2)
                if datetime.now() > twoMonthAhead and not noCalculations:
                    previousCalculations = self.load_from_db("calculations",f"WHERE [dateTime] = ?", (month["dateTime"],))
                    if len(previousCalculations) > 0:
                        for calc in previousCalculations:
                            calculations.append(calc)
                        gui_queue.put(lambda: self.calculationLabel.setText(f"Using cached data for {month['Month']}"))
                        loadingTotal -= 1
                        continue
                gui_queue.put(lambda: self.calculationLabel.setText(f"Calculating Financial Data for : {month['Month']}"))
                totalDays = int(datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S").day  - datetime.strptime(month["tranStart"], "%Y-%m-%dT%H:%M:%S").day) + 1
                loadingPoolIdx = 0
                totalPoolNum = len(pools)
                for poolDict in pools:
                    try:
                        perc = int((loadingIdx/loadingTotal + loadingPoolIdx/totalPoolNum *1/loadingTotal ) * 100)
                    except:
                        #guard against divide by zero
                        print("Warning: divide by zero in loading bar calculations.")
                        pass
                    loadingPoolIdx += 1
                    if perc < 0 or perc > 100:
                        print(f"Warning: percentage failure. loading idx: {loadingIdx}, loading total : {loadingTotal}, percentage: {perc}")
                        perc = int(90) #should not happen but I'm putting a safeguard
                    gui_queue.put(lambda: self.calculationLoadingBar.setValue(perc))
                    pool = poolDict["poolName"]
                    poolFunds = self.load_from_db("positions_low", f"WHERE [Source name] = ? AND [Date] BETWEEN ? AND ?",(pool,month["accountStart"],month["endDay"]))
                    #find MD denominator for each investor
                    #find total gain per pool
                    funds = []
                    for account in poolFunds:
                        if account["Target name"] not in funds:
                            funds.append(account["Target name"])
                    poolGainSum = 0
                    poolNAV = 0
                    poolMDdenominator = 0
                    poolWeightedCashFlow = 0
                    fundEntryList = []
                    for fund in funds:
                        startEntry = self.load_from_db("positions_low", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?",(pool, fund,month["accountStart"]))
                        endEntry = self.load_from_db("positions_low", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?",(pool, fund,month["endDay"]))
                        if len(startEntry) < 1 or len(endEntry) < 1: #skips if missing the values
                            continue
                        elif len(startEntry) > 1 and len(endEntry) > 1: #combines the values for fund sub classes
                            for entry in startEntry[1:]:
                                startEntry[0]["Value"] = str(float(startEntry[0]["Value"]) + float(entry["Value"])) #adds values to the first index
                            for entry in endEntry[1:]:
                                endEntry[0]["Value"] = str(float(endEntry[0]["Value"]) + float(entry["Value"])) #adds values to the first index
                        startEntry = startEntry[0]
                        endEntry = endEntry[0]
                        poolTransactions = self.load_from_db("transactions_low", f"WHERE [Target name] = ? AND [Date] BETWEEN ? AND ?", (fund,month["tranStart"],month["endDay"]))
                        cashFlowSum = 0
                        weightedCashFlow = 0
                        for transaction in poolTransactions:
                            cashFlowSum -= float(transaction["CashFlow"])
                            weightedCashFlow -= float(transaction["CashFlow"])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day))/totalDays
                        try:
                            fundGain = (float(endEntry["Value"]) - float(startEntry["Value"]) - cashFlowSum)
                            fundMDdenominator = float(startEntry["Value"]) + weightedCashFlow
                            fundNAV = float(endEntry["Value"])
                            fundReturn = fundGain/fundMDdenominator * 100 if fundMDdenominator != 0 else 0
                            poolGainSum += fundGain
                            poolMDdenominator += fundMDdenominator
                            poolNAV += fundNAV
                            poolWeightedCashFlow += weightedCashFlow
                            monthFundEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Fund", "Pool" : pool, "Fund" : fund ,
                                              "assetClass" : poolDict["assetClass"], "subAssetClass" : poolDict["subAssetClass"],
                                              "NAV" : fundNAV, "Gain" : fundGain, "Return" : fundReturn , 
                                              "MDdenominator" : fundMDdenominator}
                            calculations.append(monthFundEntry)
                            monthCalculations.append(monthFundEntry)
                            fundEntryList.append(monthFundEntry)


                        except Exception as e:
                            print(f"Skipped fund {fund} for {pool} in {month["Month"]} because: {e}")
                            #skips fund if the values are zero and cause an error
                    if poolNAV == 0 and poolWeightedCashFlow == 0:
                        #skips the pool if there is no cash flow or value in the pool
                        continue
                    totalNAV += poolNAV
                    totalGain += poolGainSum
                    totalMDdenominator += poolMDdenominator
                    if poolMDdenominator == 0:
                        poolReturn = 0
                    else:
                        poolReturn = poolGainSum/poolMDdenominator * 100
                    monthPoolEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Pool", "Pool" : pool, "Fund" : None ,"assetClass" : poolDict["assetClass"], "subAssetClass" : poolDict["subAssetClass"] ,"NAV" : poolNAV, "Gain" : poolGainSum, "Return" : poolReturn , "MDdenominator" : poolMDdenominator, "Ownership" : None}
                    investorMDdenominatorSum = 0
                    tempInvestorDicts = {}
                    poolOwnershipSum = 0
                    for investor in self.allInvestors:
                        investorWeightedCashFlow = 0
                        investorCashFlowSum = 0
                        tempInvestorDict = {}
                        try:
                            startEntry = self.load_from_db("positions_high", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?",(investor, pool,month["accountStart"]))[0]
                            tempInvestorDict["Active"] = True
                        except Exception as e:
                            #skip month for this investor if there is no starting balance
                            tempInvestorDict["Active"] = False
                            continue
                        investorTransactions = self.load_from_db("transactions_high",f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] BETWEEN ? AND ?", (investor,pool,month["tranStart"],month["endDay"]))
                        for transaction in investorTransactions:
                            investorCashFlowSum -= float(transaction["CashFlow"])
                            investorWeightedCashFlow -= float(transaction["CashFlow"])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day))/totalDays
                        investorMDdenominator = float(startEntry["Value"]) + investorWeightedCashFlow
                        tempInvestorDict["MDden"] = investorMDdenominator
                        tempInvestorDict["cashFlow"] = investorCashFlowSum
                        tempInvestorDict["startVal"] = float(startEntry["Value"])
                        investorMDdenominatorSum += investorMDdenominator
                        tempInvestorDicts[investor] = tempInvestorDict
                    investorEOMsum = 0
                    monthPoolEntryInvestorList = []
                    for investor in tempInvestorDicts.keys():
                        if tempInvestorDicts[investor]["Active"]:
                            investorMDdenominator = tempInvestorDicts[investor]["MDden"]
                            if investorMDdenominatorSum == 0:
                                investorGain = 0 #0 if no true value in the pool. avoids error
                            else:
                                investorGain = poolGainSum * investorMDdenominator / investorMDdenominatorSum
                            if investorMDdenominator == 0:
                                investorReturn = 0 #0 if investor has no value in pool. avoids error
                            else:
                                investorReturn = investorGain / investorMDdenominator
                            investorEOM = tempInvestorDicts[investor]["startVal"] + tempInvestorDicts[investor]["cashFlow"] + investorGain
                            investorEOMsum += investorEOM
                            monthPoolEntryInvestor = monthPoolEntry.copy()
                            monthPoolEntryInvestor["Investor"] = investor
                            monthPoolEntryInvestor["NAV"] = investorEOM
                            monthPoolEntryInvestor["Gain"] = investorGain
                            monthPoolEntryInvestor["Return"] = investorReturn * 100
                            monthPoolEntryInvestor["MDdenominator"] = investorMDdenominator
                            monthPoolEntryInvestorList.append(monthPoolEntryInvestor)
                            inputs = (investorEOM, investor,pool, month["endDay"])
                            EOMcheck = self.load_from_db("positions_high", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?",inputs[1:])
                            if len(EOMcheck) < 1:
                                EOMentry = {"Date" : month["endDay"], "Source name" : investor, "Target name" : pool, "Value" : investorEOM}
                                self.save_to_db("positions_high",EOMentry, action="add")
                            else:
                                query = "UPDATE positions_high SET Value = ? WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?"
                                self.save_to_db("positions_high",None, action = "replace", query=query, inputs = inputs)
                    for investorEntry in monthPoolEntryInvestorList:
                        if investorEOMsum != 0:
                            investorEOM = investorEntry["NAV"]
                            ownershipPerc = investorEOM/investorEOMsum * 100
                            investorEntry["Ownership"] = ownershipPerc
                            poolOwnershipSum += ownershipPerc
                        calculations.append(investorEntry)
                        monthCalculations.append(investorEntry)
                    monthPoolEntry["Ownership"] = poolOwnershipSum
                    
                    
                    for investorEntry in monthPoolEntryInvestorList:
                        for fundEntry in fundEntryList:
                            fundInvestorNAV = investorEntry["Ownership"] * fundEntry["NAV"]
                            fundInvestorGain = fundEntry["Gain"] / monthPoolEntry["Gain"] * investorEntry["Gain"] if monthPoolEntry["Gain"] != 0 else None
                            fundInvestorMDdenominator = investorEntry["MDdenominator"] / monthPoolEntry["MDdenominator"] * fundEntry["MDdenominator"] if monthPoolEntry["MDdenominator"] != 0 else None
                            fundInvestorReturn = fundInvestorGain / fundInvestorMDdenominator if fundInvestorMDdenominator != 0 else None
                            fundInvestorOwnership = fundInvestorNAV /  fundEntry["NAV"] if fundEntry["NAV"] != 0 else None
                            monthFundInvestorEntry = {"dateTime" : month["dateTime"], "Investor" : investorEntry["Investor"], "Pool" : pool, "Fund" : fundEntry["Fund"] ,
                                              "assetClass" : poolDict["assetClass"], "subAssetClass" : poolDict["subAssetClass"],
                                              "NAV" : fundInvestorNAV, "Gain" : fundInvestorGain , "Return" :  fundInvestorReturn, 
                                              "MDdenominator" : fundInvestorMDdenominator, "Ownership" : fundInvestorOwnership}
                            calculations.append(monthFundInvestorEntry)
                            monthCalculations.append(monthFundInvestorEntry)
                    calculations.append(monthPoolEntry)
                    monthCalculations.append(monthPoolEntry)
                    #End of pools loop
                    
                
                if totalNAV != 0 and totalMDdenominator != 0:
                    monthTotalEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Portfolio", "Pool" : None, "Fund" : None ,"assetClass" : None, "subAssetClass" : None ,"NAV" : totalNAV, "Gain" : totalGain, "Return" : totalGain/totalMDdenominator * 100 , "MDdenominator" : totalMDdenominator}
                    calculations.append(monthTotalEntry)
                    monthCalculations.append(monthTotalEntry)
                if datetime.now() > twoMonthAhead and not noCalculations:
                    self.save_to_db("calculations",monthCalculations, inputs=(month["dateTime"],), action="calculationUpdate")
                loadingIdx += 1
                #end of months loop
            keys = []
            for row in calculations:
                for key in row.keys():
                    if key not in keys:
                        keys.append(key)
            if noCalculations:
                self.save_to_db("calculations",calculations, keys=keys)
            gui_queue.put(lambda: self.populate(self.resultTable,calculations,keys = keys))
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
            gui_queue.put(lambda: self.calculateButton.setEnabled(True))
            if not testDataMode:
                gui_queue.put(lambda: self.importButton.setEnabled(True))
            print("Calculations complete.")
        except Exception as e:
            print(f"Error occured running calculations: {e}")
        

    def save_to_db(self, table, rows, action = "", query = "",inputs = None, keys = None):
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        if action == "reset":
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        elif action == "add":
            try:
                cols = list(rows.keys())
                quoted_cols = ','.join(f'"{c}"' for c in cols)
                placeholders = ','.join('?' for _ in cols)
                sql = f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
                vals = tuple(str(rows.get(c, '')) for c in cols)
                cur.execute(sql,vals)
                conn.commit()
            except Exception as e:
                print(f"Error inserting row into database: {e}")
                print("e.args:", e.args)
                # maybe also:
                import traceback
                print(traceback.format_exc())
        elif action == "calculationUpdate":
            try:
                cur.execute("DELETE FROM calculations WHERE [dateTime] = ?", inputs) #inputs input should be the date for deletion
                for row in rows:
                    cols = list(row.keys())
                    quoted_cols = ','.join(f'"{c}"' for c in cols)
                    placeholders = ','.join('?' for _ in cols)
                    sql = (f"INSERT INTO calculations ({quoted_cols}) VALUES ({placeholders})")
                    vals = tuple(str(row.get(c, '')) for c in cols)
                    cur.execute(sql,vals)
                conn.commit()
            except Exception as e:
                print(f"Error updating calculations in database: {e}")
                print("e.args:", e.args)
                # maybe also:
                import traceback
                print(traceback.format_exc())
        elif action == "replace":
            cur.execute(query,inputs)
            conn.commit()
        elif rows:
            if keys is None:
                cols = list(rows[0].keys())
            else:
                cols = list(keys)
            quoted_cols = ','.join(f'"{c}"' for c in cols)
            col_defs = ','.join(f'"{c}" TEXT' for c in cols)
            if True:
                cur.execute(f'DROP TABLE IF EXISTS "{table}";')
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')
            cur.execute(f'DELETE FROM "{table}"')
            placeholders = ','.join('?' for _ in cols)
            sql = f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
            vals = [tuple(str(row.get(c, '')) for c in cols) for row in rows]
            cur.executemany(sql, vals)
            conn.commit()
        else:
            print(f"No rows found for data input to '{table}'")
        conn.close()

    def populate(self, table, rows, keys = None):
        if not rows:
            return
        if keys is None:
            headers = list(rows[0].keys())
        else:
            headers = list(keys)
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(rows))

        green = QColor(181, 235, 135)
        lightGreen = QColor(213, 236, 193)

        for r, row in enumerate(rows):
            is_total_pool = (row.get('Investor', '') == "Total Pool")
            is_total_portfolio = (row.get('Investor', '') == "Total Portfolio")
            is_total_fund = (row.get('Fund', '') is not None) and (row.get('Investor', '') == "Total Fund")
            is_fund = (row.get('Fund', '') is not None) and (row.get('Investor', '') != "Total Fund")
            for c, h in enumerate(headers):
                item = QTableWidgetItem(str(row.get(h, '')))
                if is_total_pool:
                    item.setBackground(QBrush(Qt.lightGray))
                elif is_total_portfolio:
                    item.setBackground(QBrush(Qt.darkGray))
                elif is_fund:
                    item.setBackground(QBrush(lightGreen))
                elif is_total_fund:
                    item.setBackground(QBrush(green))
                table.setItem(r, c, item)

    def load_from_db(self,table, condStatement = "",parameters = None):
        # Transactions
        if os.path.exists(DATABASE_PATH):
            conn = sqlite3.connect(DATABASE_PATH)
            cur = conn.cursor()
            try:
                if condStatement != "" and parameters is not None:
                    cur.execute(f'SELECT * FROM {table} {condStatement}',parameters)
                elif condStatement != "" and parameters is None:
                    cur.execute(f'SELECT * FROM {table} {condStatement}')
                else:
                    cur.execute(f'SELECT * FROM {table}')
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                conn.close()
                return rows
            except Exception as e:
                try:
                    if parameters is not None and table != "calculations":
                        print(f"Error loading from database: {e}, table: {table} condStatment: {condStatement}, parameters: {parameters}")
                    elif table != "calculations":
                        print(f"Error loading from database: {e}, table: {table} condStatment: {condStatement}")
                    else:
                        print("Info: no previous calculations table found")
                    conn.close()
                except:
                    pass
                return []
            

if __name__ == '__main__':
    key = os.environ.get('Dynamo_API')
    ok = key and key != 'value'
    app = QApplication(sys.argv)
    timer = QTimer()
    timer.timeout.connect(poll_queue)
    timer.start(500)
    w = MyWindow(start_index=0 if not ok else 1)
    if ok: w.api_key = key
    w.show()
    sys.exit(app.exec_())
