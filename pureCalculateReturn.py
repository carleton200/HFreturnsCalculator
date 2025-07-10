import sys
import os
import json
import subprocess
import sqlite3
import requests
import calendar
import time
import copy
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import queue
from dateutil.relativedelta import relativedelta
from PyQt5.QtWidgets import (
    QApplication, QWidget, QStackedWidget, QVBoxLayout,
    QLabel, QLineEdit, QPushButton, QFormLayout,
    QRadioButton, QButtonGroup, QComboBox, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QProgressBar, QTableView, QCheckBox, QMessageBox
)
from PyQt5.QtGui import QBrush, QColor, QStandardItem, QStandardItemModel
from PyQt5.QtCore import Qt, QTimer, QAbstractTableModel, QModelIndex, QObject, pyqtSignal, QThread

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

class returnsApp(QWidget):
    def __init__(self, start_index=0):
        super().__init__()
        self.setWindowTitle('Returns Calculator')
        self.setGeometry(100, 100, 1000, 600)

        os.makedirs(ASSETS_DIR, exist_ok=True)
        self.api_key = None
        self.filterCallLock = False
        self.cancel = False
        self.tableWindows = {}
        self.earliestChangeDate = datetime(datetime.now().year,datetime.now().month + 1,datetime.now().day)
        # main stack
        self.main_layout = QVBoxLayout()
        self.stack = QStackedWidget()
        self.init_global_widgets()

        self.init_api_key_page() #1
        self.init_returns_page() #2
        self.init_calculation_page() #3

        self.stack.setCurrentIndex(start_index)
        self.main_layout.addWidget(self.stack)
        self.setLayout(self.main_layout)
    def init_global_widgets(self):
        self.lastImportLabel = QLabel("Last Data Import: ")
        self.main_layout.addWidget(self.lastImportLabel)
        self.apiLoadingBarBox = QWidget()
        t2 = QVBoxLayout()
        t2.addWidget(QLabel("Pulling transaction and account data from server..."))
        self.apiLoadingBar = QProgressBar()
        self.apiLoadingBar.setRange(0,100)
        t2.addWidget(self.apiLoadingBar)
        self.apiLoadingBarBox.setLayout(t2)
        self.apiLoadingBarBox.setVisible(False)
        self.main_layout.addWidget(self.apiLoadingBarBox)
        loadLay = QVBoxLayout()
        self.calculationLoadingBar = QProgressBar()
        self.calculationLoadingBar.setRange(0,100)
        self.calculationLabel = QLabel()
        self.cancelCalcBtn = QPushButton("Cancel Calculations")
        self.cancelCalcBtn.clicked.connect(self.cancelCalc)
        loadLay.addWidget(self.calculationLabel)
        loadLay.addWidget(self.calculationLoadingBar)
        loadLay.addWidget(self.cancelCalcBtn)
        self.calculationLoadingBox = QWidget()
        self.calculationLoadingBox.setLayout(loadLay)
        self.calculationLoadingBox.setVisible(False)
        self.main_layout.addWidget(self.calculationLoadingBox)

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

    def init_calculation_page(self):
        page = QWidget()
        layout = QVBoxLayout()
        self.info_label = QLabel('Results')
        layout.addWidget(self.info_label)
        btn_to_form = QPushButton('Go to Results')
        btn_to_form.clicked.connect(lambda: self.stack.setCurrentIndex(1))
        layout.addWidget(btn_to_form)
        

        hl = QHBoxLayout()
        self.calculationTable = QTableView(); self.calculationTable.setSortingEnabled(True)
        hl.addWidget(self.calculationTable)
        layout.addLayout(hl)

        

        page.setLayout(layout)
        self.stack.addWidget(page)

    def init_returns_page(self):
        page = QWidget()
        layout = QVBoxLayout()

        controlsBox = QWidget()
        controlsLayout = QHBoxLayout()
        self.importButton = QPushButton('Reimport Data')
        self.importButton.clicked.connect(self.beginImport)
        if testDataMode:
            self.importButton.setEnabled(False)
        clearButton = QPushButton('Clear All Cached Data')
        clearButton.clicked.connect(self.resetData)
        controlsLayout.addWidget(clearButton)
        controlsLayout.addWidget(self.importButton)
        btn_to_results = QPushButton('See Calculation Database')
        btn_to_results.clicked.connect(self.show_results)
        controlsLayout.addWidget(btn_to_results)
        controlsBox.setLayout(controlsLayout)
        layout.addWidget(controlsBox)

        fullFilterBox = QWidget()
        filterLayout = QHBoxLayout()

        tableSelectorBox = QWidget()
        tableSelectorLayout = QVBoxLayout()
        self.tableBtnGroup = QButtonGroup()
        self.complexTableBtn = QRadioButton("Complex Table")
        self.monthlyTableBtn = QRadioButton("Monthly Table")
        for rb in (self.complexTableBtn,self.monthlyTableBtn):
            self.tableBtnGroup.addButton(rb)
            #rb.toggled.connect(self.updateTableType)
            tableSelectorLayout.addWidget(rb)
        self.tableBtnGroup.buttonClicked.connect(self.buildReturnTable)
        self.complexTableBtn.setChecked(True)
        self.returnOutputType = QComboBox()
        self.returnOutputType.addItems(["Return","NAV", "Gain", "Ownership" , "MDdenominator"])
        self.returnOutputType.currentTextChanged.connect(self.buildReturnTable)
        tableSelectorLayout.addWidget(self.returnOutputType)
        tableSelectorLayout.addWidget(QLabel("Sort by: "))
        self.sortSelection = QComboBox()
        self.sortSelection.addItems(["Asset","Pool"])
        self.sortSelection.currentTextChanged.connect(self.buildReturnTable)
        tableSelectorLayout.addWidget(self.sortSelection)
        tableSelectorBox.setLayout(tableSelectorLayout)
        filterLayout.addWidget(tableSelectorBox)

        self.filterOptions = [
                            {"key": "Investor",       "name": "Investor", "dataType" : "Investor", "dynNameLow" : None},
                            {"key": "assetClass",     "name": "Asset Class", "dataType" : "Total Asset", "dynNameLow" : "ExposureAssetClass"},
                            {"key": "subAssetClass",  "name": "Sub-Asset Class", "dataType" : "Total subAsset", "dynNameLow" : "ExposureAssetClassSub-assetClass(E)"},
                            {"key": "Pool",           "name": "Pool", "dataType" : "Total Pool" , "dynNameLow" : "Source name"},
                            {"key": "Fund",           "name": "Fund/Investment", "dataType" : "Total Fund" , "dynNameLow" : "Target name"},
                        ]
        self.filterDict = {}
        self.filterRadioBtnDict = {}
        self.filterBtnGroup = QButtonGroup()
        self.filterBtnGroup.setExclusive(False)
        for filter in self.filterOptions:
            filterBox = QWidget()
            filterBoxLayout = QVBoxLayout()
            if filter["key"] != "Investor":
                #investor level is not filterable. It is total portfolio or shows the investors data
                self.filterRadioBtnDict[filter["key"]] = QCheckBox(f"{filter["name"]}:")
                self.filterRadioBtnDict[filter["key"]].setChecked(True)
                self.filterBtnGroup.addButton(self.filterRadioBtnDict[filter["key"]])
                filterBoxLayout.addWidget(self.filterRadioBtnDict[filter["key"]])
            else:
                filterBoxLayout.addWidget(QLabel("Investor:"))
            self.filterDict[filter["key"]] = QComboBox()
            self.filterDict[filter["key"]].addItems([""])
            self.filterDict[filter["key"]].currentTextChanged.connect(lambda text, level = filter["key"]: self.filterUpdate(text,level))
            
            filterBoxLayout.addWidget(self.filterDict[filter["key"]])
            filterBox.setLayout(filterBoxLayout)
            filterLayout.addWidget(filterBox)
        self.filterBtnGroup.buttonToggled.connect(self.filterBtnUpdate)
        fullFilterBox.setLayout(filterLayout)
        layout.addWidget(fullFilterBox)
        self.returnsTable = QTableWidget()
        layout.addWidget(self.returnsTable)
        self.viewUnderlyingDataBtn = QPushButton("View Underlying Data")
        self.viewUnderlyingDataBtn.clicked.connect(self.viewUnderlyingData)
        layout.addWidget(self.viewUnderlyingDataBtn)
        


        page.setLayout(layout)
        self.stack.addWidget(page)

        self.pullInvestorNames()
        self.pullLevelNames()
        
    def init_data_processing(self):
        self.calcSubmitted = False
        lastImport = self.load_from_db("history") if len(self.load_from_db("history")) == 1 else None
        if not testDataMode and lastImport is None:
            #pull data is there is no data pulled yet
            executor.submit(lambda: self.pullData())
        elif not testDataMode:
            lastImportString = lastImport[0]["lastImport"]
            lastImport = datetime.strptime(lastImportString, "%B %d, %Y @ %I:%M %p")  
            self.lastImportLabel.setText(f"Last Data Import: {lastImportString}")
            now = datetime.now()
            if lastImport.month != now.month or now > lastImport + relativedelta(days=5):
                #pull data if in a new month or 5 days have elapsed
                executor.submit(self.pullData)
            else:
                calculations = self.load_from_db("calculations")
                if calculations != []:
                    self.populate(self.calculationTable,calculations)
                    self.buildReturnTable()
                else:
                    executor.submit(self.calculateReturn)
        else:
            calculations = self.load_from_db("calculations")
            if calculations != []:
                self.populate(self.calculationTable,calculations)
                self.buildReturnTable()
            else:
                executor.submit(self.calculateReturn)
    def cancelCalc(self):
        self.cancel = True
    def viewUnderlyingData(self):
        row = self.returnsTable.currentRow()
        col = self.returnsTable.currentColumn()

        vh_item = self.returnsTable.verticalHeaderItem(row)
        row = vh_item.text() if vh_item else f"Row {row}"

        # Get the horizontal (column) header text
        hh_item = self.returnsTable.horizontalHeaderItem(col)
        col = hh_item.text() if hh_item else f"Column {col}"
        self.selectedCell = {"entity": row, "month" : col}
        try:
            window = underlyingDataWindow(parentSource=self)
            self.udWindow = window
            window.show()
        except Exception as e:
            print(f"Error in data viewing window: {e}")
    def filterBtnUpdate(self, button, checked):
        if not self.filterCallLock:
            self.filterCallLock = True
            reloadRequired = False
            for filter in self.filterOptions:
                if filter["key"] != "Investor":
                    if not self.filterRadioBtnDict[filter["key"]].isChecked():
                        self.filterDict[filter["key"]].setCurrentText("")
                        self.filterDict[filter["key"]].setVisible(False)
                        reloadRequired = True
                    else:
                        self.filterDict[filter["key"]].setVisible(True)
            self.filterCallLock = False
            if reloadRequired:
                self.buildReturnTable()
            else:
                self.populateReturnsTable(self.investorCalculations)
    def resetData(self):
        self.save_to_db("calculations",None,action="reset") #reset calculations so new data will be freshly calculated
        if testDataMode:
            executor.submit(self.calculateReturn)
        else:
            executor.submit(self.pullData)
    def beginImport(self):
        executor.submit(self.pullData)
    def buildReturnTable(self):
        print("Building return table...")
        self.stack.setCurrentIndex(1)
        if self.tableBtnGroup.checkedButton().text() == "Complex Table":
            self.returnOutputType.setCurrentText("Return")
            self.returnOutputType.setVisible(False)
            self.viewUnderlyingDataBtn.setVisible(False)
        else:
            self.returnOutputType.setVisible(True)
            self.viewUnderlyingDataBtn.setVisible(True)
        condStatement = " WHERE [Investor] = ? "
        if self.filterDict["Investor"].currentText() == "":
            parameters = ["Total Fund"]
        else:
            parameters = [self.filterDict["Investor"].currentText()]
        for filter in self.filterOptions:
            if filter["key"] != "Investor":
                if self.filterDict[filter["key"]].currentText() != "":
                    condStatement += f" AND [{filter["key"]}] = ?"
                    parameters.append(self.filterDict[filter["key"]].currentText())
        data = self.load_from_db("calculations",condStatement, tuple(parameters))
        output = {"Total" : {}}
        output , data = self.calculateUpperLevels(output,data)
        complexOutput = copy.deepcopy(output)
        for entry in data:
            if entry["Fund"] is not None and entry["Fund"] != "None":
                Dtype = "Total Fund"
                level = entry["Fund"]
            elif entry["Pool"] is not None and entry["Pool"] != "None":
                Dtype = "Total Pool"
                level = entry["Pool"]
            elif entry["subAssetClass"] is not None and entry["subAssetClass"] != "None":
                Dtype = "Total subAsset"
                level = entry["subAssetClass"]
            elif entry["assetClass"] is not None and entry["assetClass"] != "None":
                Dtype = "Total Asset"
                level = entry["assetClass"]
            else:
                Dtype = "Total"
                level = "Total"
            date = datetime.strftime(datetime.strptime(entry["dateTime"], "%Y-%m-%d %H:%M:%S"), "%B %Y")
            dataOutputType = self.returnOutputType.currentText()
            if level in output.keys():
                output[level][date] = entry[dataOutputType]
            else:
                output[level] = {}
            if "dataType" not in output[level].keys():
                output[level]["dataType"] = Dtype
            if self.tableBtnGroup.checkedButton().text() == "Complex Table" and date == datetime.strftime(datetime.now() - relativedelta(months=1),"%B %Y"):
                if level not in complexOutput.keys():
                    complexOutput[level] = {}
                if "dataType" not in complexOutput[level].keys():
                    complexOutput[level]["dataType"] = Dtype
                complexOutput[level][f"NAV"] = entry["NAV"]
                complexOutput[level][f"Gain"] = entry["Gain"]
                if self.filterDict["Investor"].currentText() != "":
                    complexOutput[level]["Ownership (%)"] = entry["Ownership"]
        if self.tableBtnGroup.checkedButton().text() == "Complex Table":
            output = self.calculateComplexTable(output,complexOutput)
        outputKeys = output.keys()
        deleteKeys = []
        for key in outputKeys:
            if len(output[key].keys()) == 0:
                deleteKeys.append(key)
        for key in deleteKeys:
            output.pop(key)
        self.investorCalculations = output
        self.populateReturnsTable(output)
    def calculateComplexTable(self,monthOutput,complexOutput):
        MTDtime = datetime.strftime(datetime.now() - relativedelta(months=1),"%B %Y")
        QTDtimes = [datetime.strftime(datetime.now() - relativedelta(months=i + 1),"%B %Y") for i in range(int((datetime.now().month) - 1) % 3 if (int(datetime.now().month) - 1) % 3 != 0 else 3)]
        YTDtimes = [datetime.strftime(datetime.now() - relativedelta(months=i + 1),"%B %Y") for i in range(int((datetime.now().month) - 1) % 12 if (int(datetime.now().month) - 1) % 12 != 0 else 12)]
        YR_times = {}
        for yr in (1,3,5,7,10,12,15,20):
            YR_times[yr] = [datetime.strftime(datetime.now() - relativedelta(months=i + 1),"%B %Y") for i in range(12 * yr)]
        for level in monthOutput.keys():
            if MTDtime in monthOutput[level].keys():
                complexOutput[level]["MTD"] = monthOutput[level][MTDtime]
            if all(month in monthOutput[level].keys() for month in QTDtimes):
                complexOutput[level]["QTD"] = 1
                for month in QTDtimes:
                    complexOutput[level]["QTD"] *= (1 + float(monthOutput[level][month]) / 100)
                complexOutput[level]["QTD"] = (complexOutput[level]["QTD"] -1) * 100
            if all(month in monthOutput[level].keys() for month in YTDtimes):
                complexOutput[level]["YTD"] = 1
                for month in YTDtimes:
                    complexOutput[level]["YTD"] *= (1 + float(monthOutput[level][month]) / 100)
                complexOutput[level]["YTD"] = (complexOutput[level]["YTD"] -1) * 100
            for yearKey in YR_times.keys():
                if all(month in monthOutput[level].keys() for month in YR_times[yearKey]):
                    headerKey = f"{yearKey}YR"
                    complexOutput[level][headerKey] = 1
                    for month in YR_times[yearKey]:
                        complexOutput[level][headerKey] *= (1 + float(monthOutput[level][month]) / 100 )
                    complexOutput[level][headerKey] = ((complexOutput[level][headerKey] ** (1/int(yearKey)) ) - 1 ) * 100 if complexOutput[level][headerKey] > 0 else -1 * ((abs(complexOutput[level][headerKey]) ** (1/int(yearKey)) ) - 1)* 100
            try:
                monthCount = 0
                if MTDtime in monthOutput[level].keys():
                    #only runs ITD if it is a current fund (MTD month exists)
                    ITDmonths = list(monthOutput[level].keys())
                    ITDmonths = [m for m in ITDmonths if m != "dataType"]
                    ITDmonths = sorted([datetime.strptime(date,"%B %Y") for date in ITDmonths])
                    
                    ITDmonths = [datetime.strftime(date,"%B %Y") for date in ITDmonths]
                    if len(ITDmonths) >= 2:
                        #only calculates if more than previous month
                        #ITDmonths = ITDmonths[1:] #remove first month?? 
                        complexOutput[level]["ITD"] = 1
                        for month in ITDmonths:
                            if month != "dataType" and month != datetime.strftime(datetime.now(),"%B %Y"):
                                monthCount += 1
                                complexOutput[level]["ITD"] *= (1 + float(monthOutput[level][month]) / 100 )
                        complexOutput[level]["ITD"] = ((complexOutput[level]["ITD"] ** (12/int(monthCount)) ) - 1 ) * 100 if complexOutput[level]["ITD"] > 0 else -1 * ((abs(complexOutput[level]["ITD"]) ** (1/int(monthCount)) ) - 1)* 100
                    else:
                        #ITD is just the previous month if no more months are found
                        complexOutput[level]["ITD"] = monthOutput[level][MTDtime]
            except Exception as e:
                pass





        return complexOutput

    def calculateUpperLevels(self, tableStructure,data):
        if self.sortSelection.currentText() == "Pool":
            poolDict = {}
            for idx, row in enumerate(data):
                #builds dict of pools with their data indexes
                if row["Pool"] not in poolDict.keys():
                    poolDict[row["Pool"]] = [idx]
                else:
                    poolDict[row["Pool"]].append(idx)
            totalEntries = {}
            for pool in sorted(poolDict.keys()):
                poolEntries = {}
                #strucuters table dict to have a pool and all its funds beneath
                tableStructure[pool] = {}
                poolFundNames = []
                for fundIdx in poolDict[pool]:
                    poolFundNames.append(data[fundIdx]["Fund"])
                    if data[fundIdx]["dateTime"] not in poolEntries.keys():
                        #creates and sums the pool level data from the fund entries
                        poolEntries[data[fundIdx]["dateTime"]] = {"dateTime" : data[fundIdx]["dateTime"], "Investor" : "Total Pool", "Pool" : pool, "Fund" : None ,
                                            "assetClass" : None, "subAssetClass" : None,
                                            "NAV" : float(data[fundIdx]["NAV"]), "Gain" : float(data[fundIdx]["Gain"]), "Return" : None , 
                                            "MDdenominator" : float(data[fundIdx]["MDdenominator"]), "Ownership" : data[fundIdx]["Ownership"]}
                    else:
                        poolEntries[data[fundIdx]["dateTime"]]["NAV"] += float(data[fundIdx]["NAV"])
                        poolEntries[data[fundIdx]["dateTime"]]["Gain"] += float(data[fundIdx]["Gain"])
                        poolEntries[data[fundIdx]["dateTime"]]["MDdenominator"] += float(data[fundIdx]["MDdenominator"])
                    if data[fundIdx]["dateTime"] not in totalEntries.keys():
                        #creates and sums the pool level data from the fund entries
                        totalEntries[data[fundIdx]["dateTime"]] = {"dateTime" : data[fundIdx]["dateTime"], "Investor" : "Total", "Pool" : None, "Fund" : None ,
                                            "assetClass" : None, "subAssetClass" : None,
                                            "NAV" : float(data[fundIdx]["NAV"]), "Gain" : float(data[fundIdx]["Gain"]), "Return" : None , 
                                            "MDdenominator" : float(data[fundIdx]["MDdenominator"]), "Ownership" : None}
                    else:
                        totalEntries[data[fundIdx]["dateTime"]]["NAV"] += float(data[fundIdx]["NAV"])
                        totalEntries[data[fundIdx]["dateTime"]]["Gain"] += float(data[fundIdx]["Gain"])
                        totalEntries[data[fundIdx]["dateTime"]]["MDdenominator"] += float(data[fundIdx]["MDdenominator"])
                for fundName in sorted(poolFundNames):
                    tableStructure[fundName] = {}
                for month in poolEntries.keys():
                    poolEntries[month]["Return"] = poolEntries[month]["Gain"] / poolEntries[month]["MDdenominator"] * 100 if poolEntries[month]["MDdenominator"] != 0 else 0
                    data.append(poolEntries[month])
            for month in totalEntries.keys():
                totalEntries[month]["Return"] = totalEntries[month]["Gain"] / totalEntries[month]["MDdenominator"] * 100 if totalEntries[month]["MDdenominator"] != 0 else 0
                data.append(totalEntries[month])
        elif self.sortSelection.currentText() == "Asset":
            assetDict = {}
            for idx, row in enumerate(data):
                #builds dict of pools with their data indexes
                asset = row["assetClass"] if row["assetClass"] != "Cash" else "Cash "
                if asset not in assetDict.keys():
                    assetDict[asset] = {row["subAssetClass"] : [idx]}
                elif row["subAssetClass"] not in assetDict[asset].keys():
                    assetDict[asset][row["subAssetClass"]] = [idx]
                else:
                    assetDict[asset][row["subAssetClass"]].append(idx)
            totalEntries = {}
            for asset in sorted(assetDict.keys()):
                tableStructure[asset] = {}
                assetEntries = {}
                for subAsset in sorted(assetDict[asset].keys()):
                    tableStructure[subAsset] = {}
                    subAssetEntries = {}
                    poolFundNames = []
                    for fundIdx in assetDict[asset][subAsset]:
                        poolFundNames.append(data[fundIdx]["Fund"])
                        if data[fundIdx]["dateTime"] not in assetEntries.keys():
                            assetEntries[data[fundIdx]["dateTime"]] = {"dateTime" : data[fundIdx]["dateTime"], "Investor" : "Total Asset", "Pool" : None, "Fund" : None ,
                                                "assetClass" : asset, "subAssetClass" : None,
                                                "NAV" : float(data[fundIdx]["NAV"]), "Gain" : float(data[fundIdx]["Gain"]), "Return" : None , 
                                                "MDdenominator" : float(data[fundIdx]["MDdenominator"]), "Ownership" : data[fundIdx]["Ownership"]}
                        else:
                            assetEntries[data[fundIdx]["dateTime"]]["NAV"] += float(data[fundIdx]["NAV"])
                            assetEntries[data[fundIdx]["dateTime"]]["Gain"] += float(data[fundIdx]["Gain"])
                            assetEntries[data[fundIdx]["dateTime"]]["MDdenominator"] += float(data[fundIdx]["MDdenominator"])
                        if data[fundIdx]["dateTime"] not in subAssetEntries.keys():
                            #creates and sums the pool level data from the fund entries
                            subAssetEntries[data[fundIdx]["dateTime"]] = {"dateTime" : data[fundIdx]["dateTime"], "Investor" : "Total subAsset", "Pool" : None, "Fund" : None ,
                                                "assetClass" : asset, "subAssetClass" : subAsset,
                                                "NAV" : float(data[fundIdx]["NAV"]), "Gain" : float(data[fundIdx]["Gain"]), "Return" : None , 
                                                "MDdenominator" : float(data[fundIdx]["MDdenominator"]), "Ownership" : data[fundIdx]["Ownership"]}
                        else:
                            subAssetEntries[data[fundIdx]["dateTime"]]["NAV"] += float(data[fundIdx]["NAV"])
                            subAssetEntries[data[fundIdx]["dateTime"]]["Gain"] += float(data[fundIdx]["Gain"])
                            subAssetEntries[data[fundIdx]["dateTime"]]["MDdenominator"] += float(data[fundIdx]["MDdenominator"])
                        if data[fundIdx]["dateTime"] not in totalEntries.keys():
                            #creates and sums the pool level data from the fund entries
                            totalEntries[data[fundIdx]["dateTime"]] = {"dateTime" : data[fundIdx]["dateTime"], "Investor" : "Total", "Pool" : None, "Fund" : None ,
                                                "assetClass" : None, "subAssetClass" : None,
                                                "NAV" : float(data[fundIdx]["NAV"]), "Gain" : float(data[fundIdx]["Gain"]), "Return" : None , 
                                                "MDdenominator" : float(data[fundIdx]["MDdenominator"]), "Ownership" : None}
                        else:
                            totalEntries[data[fundIdx]["dateTime"]]["NAV"] += float(data[fundIdx]["NAV"])
                            totalEntries[data[fundIdx]["dateTime"]]["Gain"] += float(data[fundIdx]["Gain"])
                            totalEntries[data[fundIdx]["dateTime"]]["MDdenominator"] += float(data[fundIdx]["MDdenominator"])
                    for fundName in sorted(poolFundNames):
                        tableStructure[fundName] = {}
                    for month in subAssetEntries.keys():
                        subAssetEntries[month]["Return"] = subAssetEntries[month]["Gain"] / subAssetEntries[month]["MDdenominator"] * 100 if subAssetEntries[month]["MDdenominator"] != 0 else 0
                        data.append(subAssetEntries[month])
                for month in assetEntries.keys():
                    assetEntries[month]["Return"] = assetEntries[month]["Gain"] / assetEntries[month]["MDdenominator"] * 100 if assetEntries[month]["MDdenominator"] != 0 else 0
                    data.append(assetEntries[month])
            for month in totalEntries.keys():
                totalEntries[month]["Return"] = totalEntries[month]["Gain"] / totalEntries[month]["MDdenominator"] * 100 if totalEntries[month]["MDdenominator"] != 0 else 0
                data.append(totalEntries[month])
        return tableStructure,data
                    
    def filterUpdate(self, filterText, level):
        def resetOptions(key,options):
            currentText = self.filterDict[key].currentText()
            comboBox = self.filterDict[key]
            comboBox.clear()
            comboBox.addItems([""])
            comboBox.addItems(sorted(options))
            if currentText in options:
                comboBox.setCurrentText(currentText)
        def exitFunc():
            self.filterCallLock = False
            self.buildReturnTable()
        if not self.filterCallLock:
            #prevents recursion on calls from comboboxes being updated
            self.filterCallLock = True
            currentAsset = self.filterDict["assetClass"].currentText()
            currentSubAsset = self.filterDict["subAssetClass"].currentText()
            currentPool = self.filterDict["Pool"].currentText()
            currentFund = self.filterDict["Fund"].currentText()

            if currentAsset == "" and currentSubAsset == "" and currentPool == "" and currentFund == "":
                #if no filters, open all options and exit
                resetOptions("Fund",self.fullLevelOptions["Fund"])
                resetOptions("Pool",self.fullLevelOptions["Pool"])
                resetOptions("subAssetClass",self.fullLevelOptions["subAssetClass"])
                resetOptions("assetClass",self.fullLevelOptions["assetClass"])
                exitFunc()
                return

            condStatement = ""
            first = True
            parameters = []
            for filter in self.filterOptions:
                if filter["key"] != "Investor":
                    if self.filterDict[filter["key"]].currentText() != "":
                        if first:
                            condStatement = f"WHERE [{filter["dynNameLow"]}] = ?"
                            first = False
                        else:
                            condStatement += f" AND [{filter["dynNameLow"]}] = ?"
                        parameters.append(self.filterDict[filter["key"]].currentText())
            lowAccounts = self.load_from_db("positions_low", condStatement,tuple(parameters))
            options = {}
            for filter in self.filterOptions:
                options[filter["key"]] = []
            for account in lowAccounts:
                for filter in self.filterOptions:
                    if filter["key"] != "Investor":
                        if account[filter["dynNameLow"]] not in options[filter["key"]]:
                            options[filter["key"]].append(account[filter["dynNameLow"]])
            for filter in self.filterOptions:
                #resets options for everything but recently updated parameter
                if filter["key"] != "Investor" and filter["key"] != level:
                    resetOptions(filter["key"],options[filter["key"]])
            exitFunc()
            return
    def updateMonths(self):
        startMonth = int(1)
        year = str(2021)
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
        self.endDate =  bothEnd
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
            self.filterDict["Investor"].addItems(investors)
        else:
            self.allInvestors = []
    def pullLevelNames(self):
        assets = []
        subAssets = []
        pools = []
        funds = []
        accountsHigh = self.load_from_db("positions_high")
        if accountsHigh is not None:
            for account in accountsHigh:
                assetClass = account["ExposureAssetClass"]
                subAssetClass = account["ExposureAssetClassSub-assetClass(E)"]
                pool = account["Target name"]
                if assetClass is not None and assetClass not in assets:
                    assets.append(assetClass)
                if subAssetClass is not None and subAssetClass not in subAssets:
                    subAssets.append(subAssetClass)
                if pool is not None and pool not in pools:
                    pools.append(pool)
        else:
            print("no investor to pool accounts found")
        accountsLow = self.load_from_db("positions_low")
        if accountsLow is not None:
            for lowAccount in accountsLow:
                assetClass = lowAccount["ExposureAssetClass"]
                subAssetClass = lowAccount["ExposureAssetClassSub-assetClass(E)"]
                pool = lowAccount["Source name"]
                fund = lowAccount["Target name"]
                if fund is not None and fund not in funds:
                    funds.append(fund)
                if assetClass is not None and assetClass not in assets:
                    assets.append(assetClass)
                if subAssetClass is not None and subAssetClass not in subAssets:
                    subAssets.append(subAssetClass)
                if pool is not None and pool not in pools:
                    pools.append(pool)
        else:
            print("no pool to fund accounts found")
        assets.sort()
        subAssets.sort()
        pools.sort()
        funds.sort()
        self.filterDict["assetClass"].addItems(assets)
        self.filterDict["subAssetClass"].addItems(subAssets)
        self.filterDict["Pool"].addItems(pools)
        self.filterDict["Fund"].addItems(funds)
        self.fullLevelOptions = {"Fund" : funds, "Pool" : pools, "subAssetClass" : subAssets, "assetClass" : assets}


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
        def checkNewestData(table, rows):
            try:
                diffCount = 0
                differences = []
                previous = self.load_from_db(table) or []

                # Build a set of tuple‐keys for the old data
                seen = set()
                for rec in previous:
                    value = rec['Value' if "position" in table else "CashFlow"]
                    value = 0 if value is None or value == "None" else value
                    seen.add((
                        rec['Source name'] if rec['Source name'] is not None else "None",
                        rec['Target name'] if rec['Target name'] is not None else "None",
                        round(float(value),-2),               # normalize to float
                        rec['Date'].replace(' ', 'T')      # normalize format if needed
                    ))

                earliest = None
                for rec in rows:
                    value = rec['Value' if "position" in table else "CashFlow"]
                    value = 0 if value is None or value == "None" else value
                    key = (
                        rec['Source name'] if rec['Source name'] is not None else "None",
                        rec['Target name'] if rec['Target name'] is not None else "None",
                        round(float(value),-2),               
                        rec['Date'].replace(' ', 'T')
                    )
                    if key in seen:
                        continue
                    diffCount += 1
                    differences.append(rec)
                    differences.append({"Source name" : key[0],"Target name" : key[1],"Value" : key[2],"Date" : key[3]})
                    # parse the date for comparison
                    dt = datetime.strptime(rec['Date'], "%Y-%m-%dT%H:%M:%S")
                    if earliest is None or dt < earliest:
                        earliest = dt
                
                if earliest:
                    if earliest < self.earliestChangeDate:
                        self.earliestChangeDate = earliest
                print(f"Differences in {table} : {diffCount} of {len(rows)}")
                if diffCount > 0:
                    def openWindow():
                        window = tableWindow(parentSource=self,all_rows=differences,table=table)
                        self.tableWindows[table] = window
                        window.show()
                    gui_queue.put(lambda: openWindow())
            except Exception as e:
                print(f"Error searching old data: {e}")

        self.earliestChangeDate = datetime(datetime.now().year,datetime.now().month + 1,datetime.now().day)
        gui_queue.put(lambda: self.apiLoadingBarBox.setVisible(True))
        gui_queue.put(lambda: self.importButton.setEnabled(False))
        self.updateMonths()
        startDate = f"2021-01-01T00:00:00.000Z" #around first day for most records
        startDate = self.startDate
        endDate = self.endDate
        self.pullInvestorNames()
        apiData = {
            "tranCols": "Investment in, Investing Entity, Transaction Type, Effective date, Cash flow change, Asset Class (E), Sub-asset class (E)",
            "tranName": "InvestmentTransaction",
            "tranSort": "Effective date:desc",
            "accountCols": "As of Date, Balance Type, Asset Class, Sub-asset class, Value of Investments, Investing entity, Investment in",
            "accountName": "InvestmentPosition",
            "accountSort": "As of Date:desc",
        }
        calculationsTest = self.load_from_db("calculations")
        if calculationsTest != []:
            skipCalculations = True
        else:
            skipCalculations = False
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
                        if skipCalculations:
                            checkNewestData('positions_low',rows)
                        gui_queue.put(lambda: self.save_to_db('positions_low', rows))
                    else:
                        #positions_high is not checked for new data, as the calculations overwrite Dynamo's calculations
                        gui_queue.put(lambda:self.save_to_db('positions_high', rows))
                else:
                    if j == 0:
                        if skipCalculations:
                            checkNewestData('transactions_low',rows)
                        gui_queue.put(lambda:self.save_to_db('transactions_low', rows))
                    else:
                        if skipCalculations:
                            checkNewestData('transactions_high',rows)
                        gui_queue.put(lambda:self.save_to_db('transactions_high', rows))
        if skipCalculations:
            print("Earliest change: ", self.earliestChangeDate)
        gui_queue.put(lambda: self.stack.setCurrentIndex(2))
        gui_queue.put(lambda: self.apiLoadingBarBox.setVisible(False))
        while not gui_queue.empty(): #wait to assure database has been updated in main thread before continuing
            time.sleep(0.2)
        


        self.save_to_db("history",None,action="reset") #clears history then updates most recent import
        currentTime = datetime.now().strftime("%B %d, %Y @ %I:%M %p")
        self.save_to_db("history",[{"lastImport" : currentTime}])
        self.lastImportLabel.setText(f"Last Data Import: {currentTime}")

        self.calculateReturn()
        if not testDataMode:
            gui_queue.put(lambda: self.importButton.setEnabled(True))

    def calculateReturn(self):
        try:
            gui_queue.put(lambda: self.importButton.setEnabled(False))
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(True))
            self.updateMonths()
            gui_queue.put(lambda: self.pullInvestorNames())
            gui_queue.put(lambda: self.pullLevelNames())
            gui_queue.put(lambda : self.stack.setCurrentIndex(2))
            print("Calculating return....")
            months = self.load_from_db("Months", f"ORDER BY [dateTime] ASC")
            calculations = []
            monthIdx = 0
            if self.load_from_db("calculations") == []:
                noCalculations = True
            else:
                noCalculations = False

            if self.earliestChangeDate > datetime.now() and not noCalculations:
                #if no new data exists, use old calculations
                calculations = self.load_from_db("calculations")
                keys = []
                for row in calculations:
                    for key in row.keys():
                        if key not in keys:
                            keys.append(key)
                gui_queue.put( lambda: self.populate(self.calculationTable,calculations,keys = keys))
                gui_queue.put( lambda: self.buildReturnTable())
                gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
                if not testDataMode:
                    gui_queue.put(lambda: self.importButton.setEnabled(True))
                print("Calculations skipped.")
                return
            skippedMonths = 0
            for monthIdx, month in enumerate(months):
                monthCalculations = []
                
                #if the calculations for the month have already been complete, pull the old data
                if self.earliestChangeDate > datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S") and not noCalculations:
                    previousCalculations = self.load_from_db("calculations",f"WHERE [dateTime] = ?", (month["dateTime"],))
                    if len(previousCalculations) > 0:
                        for calc in previousCalculations:
                            calculations.append(calc)
                        gui_queue.put(lambda: self.calculationLabel.setText(f"Using cached data for {month['Month']}"))
                        skippedMonths += 1
                        continue
                gui_queue.put(lambda: self.calculationLabel.setText(f"Calculating Financial Data for : {month['Month']}"))
                totalDays = int(datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S").day  - datetime.strptime(month["tranStart"], "%Y-%m-%dT%H:%M:%S").day) + 1
                monthLowAccounts = self.load_from_db("positions_low", f"WHERE [Date] BETWEEN ? AND ?",(month["accountStart"],month["endDay"]))
                pools = []
                poolNames = []
                for item in monthLowAccounts:
                    if item["Source name"] not in poolNames:
                        pools.append({"poolName" : item["Source name"], "assetClass" : item["ExposureAssetClass"], "subAssetClass" : item["ExposureAssetClassSub-assetClass(E)"]})
                        poolNames.append(item["Source name"])
                monthLowTransactions = self.load_from_db("transactions_low", f"WHERE [Date] BETWEEN ? AND ?",(month["tranStart"],month["endDay"]))
                for item in monthLowTransactions:
                    if item["Source name"] not in poolNames:
                        pools.append({"poolName" : item["Source name"], "assetClass" : item["SysProp_FundTargetNameAssetClass(E)"], "subAssetClass" : item["SysProp_FundTargetNameSub-assetClass(E)"]})
                        poolNames.append(item["Source name"])
                for poolIdx, poolDict in enumerate(pools):
                    if self.cancel:
                        gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
                        gui_queue.put(lambda: self.importButton.setEnabled(True))
                        gui_queue.put(lambda: QMessageBox.information(self,"Cancelling","Cancelling Calculation Process"))
                        self.cancel = False
                        return
                    countedMonths = len(months) - skippedMonths
                    loadingFraction = (monthIdx - skippedMonths)/countedMonths + poolIdx/len(pools)/countedMonths
                    perc = int(loadingFraction * 100) if int(loadingFraction * 100) >= 0 and int(loadingFraction * 100) <= 100 else 50
                    gui_queue.put(lambda: self.calculationLoadingBar.setValue(perc))
                    pool = poolDict["poolName"]
                    poolFunds = self.load_from_db("positions_low", f"WHERE [Source name] = ? AND [Date] BETWEEN ? AND ?",(pool,month["accountStart"],month["endDay"]))
                    #find MD denominator for each investor
                    #find total gain per pool
                    funds = []
                    fundNames = []
                    for account in poolFunds:
                        if account["Target name"] not in fundNames:
                            fundNames.append(account["Target name"])
                            funds.append({"fundName" : account["Target name"], "hidden" : False})

                    hiddenFunds = self.load_from_db("transactions_low", f"WHERE [Source name] = ? AND [Date] BETWEEN ? AND ?",(pool,month["accountStart"],month["endDay"]))
                    #funds that do not have account positions. Just transactions that should not appear as a fund (ex: deferred liabilities)
                    for account in hiddenFunds:
                        if account["Target name"] not in fundNames:
                            fundNames.append(account["Target name"])
                            funds.append({"fundName" : account["Target name"], "hidden" : True})

                    poolGainSum = 0
                    poolNAV = 0
                    poolMDdenominator = 0
                    poolWeightedCashFlow = 0
                    fundEntryList = []
                    for fundDict in funds:
                        fund = fundDict["fundName"]
                        hidden = fundDict["hidden"]
                        assetClass = None
                        subAssetClass = None
                        startEntry = self.load_from_db("positions_low", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?",(pool, fund,month["accountStart"]))
                        endEntry = self.load_from_db("positions_low", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?",(pool, fund,month["endDay"]))
                        createFinalValue = False
                        if len(startEntry) < 1:
                            startEntry = [{"Value" : 0}]
                        else:
                            assetClass = startEntry[0]["ExposureAssetClass"]
                            subAssetClass = startEntry[0]["ExposureAssetClassSub-assetClass(E)"]
                        if len(startEntry) > 1: #combines the values for fund sub classes
                            for entry in startEntry[1:]:
                                startEntry[0]["Value"] = str(float(startEntry[0]["Value"]) + float(entry["Value"])) #adds values to the first index
                        if len(endEntry) < 1:
                            createFinalValue = True
                            endEntry = [{"Value" : 0}]
                        else:
                            assetClass = endEntry[0]["ExposureAssetClass"]
                            subAssetClass = endEntry[0]["ExposureAssetClassSub-assetClass(E)"]
                        if len(endEntry) > 1:
                            for entry in endEntry[1:]:
                                endEntry[0]["Value"] = str(float(endEntry[0]["Value"]) + float(entry["Value"])) #adds values to the first index
                        startEntry = startEntry[0]
                        endEntry = endEntry[0]
                        poolTransactions = self.load_from_db("transactions_low", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] BETWEEN ? AND ?", (pool,fund,month["tranStart"],month["endDay"]))
                        cashFlowSum = 0
                        weightedCashFlow = 0
                        for transaction in poolTransactions:
                            if assetClass is None or assetClass == "None":
                                assetClass = transaction["SysProp_FundTargetNameAssetClass(E)"]
                            if subAssetClass is None or subAssetClass == "None":
                                subAssetClass = transaction["SysProp_FundTargetNameSub-assetClass(E)"]
                            cashFlowSum -= float(transaction["CashFlow"])
                            weightedCashFlow -= float(transaction["CashFlow"])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day))/totalDays
                        try:
                            if startEntry["Value"] is None or startEntry["Value"] == "None":
                                startEntry["Value"] = 0
                            if endEntry["Value"] is None or endEntry["Value"] == "None":
                                endEntry["Value"] = 0
                            if createFinalValue:
                                #implies there is no gain (cash account)
                                endEntry["Value"] = float(startEntry["Value"]) + cashFlowSum    
                            fundGain = (float(endEntry["Value"]) - float(startEntry["Value"]) - cashFlowSum)
                            fundMDdenominator = float(startEntry["Value"]) + weightedCashFlow
                            fundNAV = float(endEntry["Value"])
                            fundReturn = fundGain/fundMDdenominator * 100 if fundMDdenominator != 0 else 0
                            if fundNAV == 0 and fundMDdenominator == 0:
                                #skip if there is no value and no change in value
                                continue
                            elif createFinalValue:
                                fundEOMentry = {"Date" : month["endDay"], "Source name" : pool, "Target name" : fund , "Value" : endEntry["Value"],
                                                    "Balancetype" : "Calculated_R", "ExposureAssetClass" : assetClass, "ExposureAssetClassSub-assetClass(E)" : subAssetClass}
                                self.save_to_db("positions_low",fundEOMentry, action="add")
                            poolGainSum += fundGain
                            poolMDdenominator += fundMDdenominator
                            poolNAV += fundNAV
                            poolWeightedCashFlow += weightedCashFlow
                            monthFundEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Fund", "Pool" : pool, "Fund" : fund ,
                                            "assetClass" : assetClass, "subAssetClass" : subAssetClass,
                                            "NAV" : fundNAV, "Gain" : fundGain, "Return" : fundReturn , 
                                            "MDdenominator" : fundMDdenominator, "Ownership" : ""}
                            calculations.append(monthFundEntry)
                            monthCalculations.append(monthFundEntry)
                            fundEntryList.append(monthFundEntry)


                        except Exception as e:
                            print(f"Skipped fund {fund} for {pool} in {month["Month"]} because: {e}")
                            #skips fund if the values are zero and cause an error
                    if poolNAV == 0 and poolWeightedCashFlow == 0:
                        #skips the pool if there is no cash flow or value in the pool
                        continue
                    poolReturn = poolGainSum/poolMDdenominator * 100 if poolMDdenominator != 0 else 0
                    monthPoolEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Pool", "Pool" : pool, "Fund" : None ,
                                      "assetClass" : poolDict["assetClass"], "subAssetClass" : poolDict["subAssetClass"] ,
                                      "NAV" : poolNAV, "Gain" : poolGainSum, "Return" : poolReturn , "MDdenominator" : poolMDdenominator,
                                        "Ownership" : None}
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
                        tempInvestorDict["ExposureAssetClass"] = startEntry["ExposureAssetClass"]
                        tempInvestorDict["ExposureAssetClassSub-assetClass(E)"] = startEntry["ExposureAssetClassSub-assetClass(E)"]
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
                            ownershipPerc = investorEOM/poolNAV * 100 if poolNAV != 0 else 0
                            monthPoolEntryInvestor["Ownership"] = ownershipPerc
                            poolOwnershipSum += ownershipPerc
                            # calculations.append(monthPoolEntryInvestor)
                            # monthCalculations.append(monthPoolEntryInvestor)
                            monthPoolEntryInvestorList.append(monthPoolEntryInvestor)
                            inputs = (investorEOM, investor,pool, month["endDay"])
                            EOMcheck = self.load_from_db("positions_high", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?",inputs[1:])
                            if len(EOMcheck) < 1:
                                EOMentry = {"Date" : month["endDay"], "Source name" : investor, "Target name" : pool, "Value" : investorEOM,
                                             "Balancetype" : "Calculated_R", "ExposureAssetClass" : tempInvestorDicts[investor]["ExposureAssetClass"],
                                               "ExposureAssetClassSub-assetClass(E)" : tempInvestorDicts[investor]["ExposureAssetClassSub-assetClass(E)"]}
                                self.save_to_db("positions_high",EOMentry, action="add")
                            else:
                                query = "UPDATE positions_high SET Value = ? WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?"
                                self.save_to_db("positions_high",None, action = "replace", query=query, inputs = inputs)
                    monthPoolEntry["Ownership"] = poolOwnershipSum
                    for investorEntry in monthPoolEntryInvestorList:
                        for fundEntry in fundEntryList:
                            fundInvestorNAV = investorEntry["Ownership"] / 100 * fundEntry["NAV"]
                            fundInvestorGain = fundEntry["Gain"] / monthPoolEntry["Gain"] * investorEntry["Gain"] if monthPoolEntry["Gain"] != 0 else 0
                            fundInvestorMDdenominator = investorEntry["MDdenominator"] / monthPoolEntry["MDdenominator"] * fundEntry["MDdenominator"] if monthPoolEntry["MDdenominator"] != 0 else 0
                            fundInvestorReturn = fundInvestorGain / fundInvestorMDdenominator if fundInvestorMDdenominator != 0 else 0
                            fundInvestorOwnership = fundInvestorNAV /  fundEntry["NAV"] if fundEntry["NAV"] != 0 else 0
                            monthFundInvestorEntry = {"dateTime" : month["dateTime"], "Investor" : investorEntry["Investor"], "Pool" : pool, "Fund" : fundEntry["Fund"] ,
                                            "assetClass" : poolDict["assetClass"], "subAssetClass" : poolDict["subAssetClass"],
                                            "NAV" : fundInvestorNAV, "Gain" : fundInvestorGain , "Return" :  fundInvestorReturn * 100, 
                                            "MDdenominator" : fundInvestorMDdenominator, "Ownership" : fundInvestorOwnership * 100}
                            calculations.append(monthFundInvestorEntry)
                            monthCalculations.append(monthFundInvestorEntry)
                    #End of pools loop
                #end of months loop
            keys = []
            for row in calculations:
                for key in row.keys():
                    if key not in keys:
                        keys.append(key)
            self.save_to_db("calculations",calculations, keys=keys)
            gui_queue.put( lambda: self.populate(self.calculationTable,calculations,keys = keys))
            gui_queue.put( lambda: self.buildReturnTable())
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
            if not testDataMode:
                gui_queue.put(lambda: self.importButton.setEnabled(True))
            print("Calculations complete.")
        except Exception as e:
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
            print(f"Error occured running calculations: {e}")
            print("e.args:", e.args)
            # maybe also:
            import traceback
            print(traceback.format_exc())
        

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
    def populateReturnsTable(self, origRows: dict):
        if not origRows:
            # nothing to show
            self.returnsTable.clear()
            self.returnsTable.setRowCount(0)
            self.returnsTable.setColumnCount(0)
            return
        rows = copy.deepcopy(origRows)
        #prevents alteration of orignal data
        for filter in self.filterOptions:
            #removes the rows that the user has selected not to see
            if filter["key"] != "Investor":
                if not self.filterRadioBtnDict[filter["key"]].isChecked():
                    keys = rows.keys()
                    deleteKeys = []
                    for key in keys:
                        if rows[key]["dataType"] == filter["dataType"]:
                            deleteKeys.append(key)
                    for deleteKey in deleteKeys:
                        rows.pop(deleteKey)
        # 1) Determine the full set of columns
        cleanedRows = copy.deepcopy(rows)
        keys = rows.keys()
        for key in keys:
            cleanedRows[key].pop("dataType")
        col_keys = set()
        for row_dict in cleanedRows.values():
            col_keys.update(row_dict.keys())
        col_keys = list(col_keys)
        if self.tableBtnGroup.checkedButton().text() == "Monthly Table":
            for idx in range(len(col_keys)):
                col_keys[idx] = datetime.strptime(col_keys[idx],"%B %Y")
            col_keys = sorted(col_keys)
            for idx in range(len(col_keys)):
                col_keys[idx] = datetime.strftime(col_keys[idx],"%B %Y")
        elif self.tableBtnGroup.checkedButton().text() == "Complex Table":
            newColKeys = []
            headerOrder = ["NAV","Gain", "Ownership (%)","MTD","QTD","YTD"]
            for i in (1,3,5,7,10,12,15,20):
                headerOrder.append(f"{i}YR")
            for header in headerOrder:
                if header in col_keys:
                    newColKeys.append(header)
            for header in col_keys:
                #in case changes result in extra headers not listed, they will still appear
                if header not in headerOrder:
                    newColKeys.append(header)
            col_keys = newColKeys
        # 2) Configure table size and headers
        self.returnsTable.setRowCount(len(rows))
        self.returnsTable.setColumnCount(len(col_keys))
        self.returnsTable.setVerticalHeaderLabels(list(cleanedRows.keys()))
        self.returnsTable.setHorizontalHeaderLabels(col_keys)

        # 3) Populate each cell
        for r, (row_label, row_dict) in enumerate(rows.items()):
            if "dataType" in row_dict.keys():
                dataType = row_dict["dataType"]
                row_dict.pop("dataType")
            else:
                dataType = ""
        # decide if this row should be grey
            if dataType == "Total Portfolio" or dataType == "Total":
                row_color = QColor(Qt.darkGray)
            elif dataType == "Total Pool":
                row_color = QColor(Qt.lightGray)
            elif dataType == "Total Fund":
                row_color = QColor(213, 236, 193)
            elif dataType == "Total Asset":
                row_color = QColor(181, 135, 235)
            elif dataType == "Total subAsset":
                row_color = QColor(213, 193, 236)
            else:
                row_color = None

            # 1) create (or override) the vertical header item for this row
            header_item = QTableWidgetItem(row_label)
            if row_color is not None:
                header_item.setBackground(QBrush(row_color))
            self.returnsTable.setVerticalHeaderItem(r, header_item)

            # 2) fill in the row’s cells
            for c, col in enumerate(col_keys):
                val = row_dict.get(col, "")
                val = round(float(val), 2) if val is not None and val != "" and val != "None" else ""
                if val != "":
                    val = f"{val:,.2f}"
                item = QTableWidgetItem(val)
                if val != "":
                    item.setData(Qt.UserRole,val)
                if row_color is not None:
                    item.setBackground(QBrush(row_color))
                self.returnsTable.setItem(r, c, item)
    def populate(self, table, rows, keys = None):
        if not rows:
            return
        if keys is None:
            headers = list(rows[0].keys())
        else:
            headers = list(keys)

        calcTableModel = DictListModel(rows,headers, self)
        table.setModel(calcTableModel)

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
        else:
            return []

class DictListModel(QAbstractTableModel):
    """
    Simple table model over a list of dicts.
    """
    def __init__(self, rows, headers, parent=None):
        super().__init__(parent)
        self._rows = rows
        self._headers = headers

    def rowCount(self, parent=QModelIndex()):
        return len(self._rows)

    def columnCount(self, parent=QModelIndex()):
        return len(self._headers)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        row = self._rows[index.row()]
        key = self._headers[index.column()]
        # Display the cell text
        if role == Qt.DisplayRole:
            return str(row.get(key, ''))

        # Conditional background coloring
        if role == Qt.BackgroundRole:
            # Example: color 'Value' column green if above threshold
            try:
                Investor = str(row.get('Investor', ''))
                Fund = row.get('Fund', '')
                if Investor == "Total Portfolio":
                    return QBrush(Qt.darkGray)  
                elif Investor == "Total Asset":
                    return QBrush(QColor(181, 135, 235)) 
                elif Investor == "Total subAsset":
                    return QBrush(QColor(213, 193, 236)) 
                elif Investor == "Total Pool":
                    return QBrush(Qt.lightGray) 
                elif Fund is not None and Fund != "None" and Investor == "Total Fund":
                    return QBrush(QColor(181, 235, 135))  
                elif Fund is not None and Fund != "None": #Fund
                    return QBrush(QColor(213, 236, 193)) 
            except (ValueError, TypeError):
                pass

        # Alignment for numbers
        if role == Qt.TextAlignmentRole:
            try:
                float(row.get(key))
                return Qt.AlignVCenter | Qt.AlignRight
            except (ValueError, TypeError):
                return Qt.AlignVCenter | Qt.AlignLeft

        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self._headers[section]
        return None            

class underlyingDataWindow(QWidget):
    """
    A window that loads data from four database sources in the parent,
    merges and sorts it by dateTime, and displays it in a QTableWidget
    with a unified set of columns.
    """
    def __init__(self, parent=None, flags=Qt.WindowFlags(), parentSource = None):
        super().__init__(parent, flags)
        self.parent = parentSource
        self.setWindowTitle("Underlying Data Viewer")
        self.resize(1000, 600)

        # Layout and table
        layout = QVBoxLayout(self)
        self.table = QTableWidget(self)
        layout.addWidget(self.table)

        selectedMonth = datetime.strptime(self.parent.selectedCell["month"], "%B %Y")
        tranStart = selectedMonth.replace(day = 1)
        accountStart = tranStart - relativedelta(days= 1)
        allEnd = (tranStart + relativedelta(months=1)) - relativedelta(days=1)
        tranStart = datetime.strftime(tranStart,"%Y-%m-%dT00:00:00")
        accountStart = datetime.strftime(accountStart,"%Y-%m-%dT00:00:00")
        allEnd = datetime.strftime(allEnd,"%Y-%m-%dT00:00:00")
        entity = self.parent.selectedCell["entity"]
        # 1) Define your four data sources (table names or identifiers)
        assetLook = False
        if entity in self.parent.fullLevelOptions["Pool"]:
            highLook = "[Target name]"
            lowLook = "[Source name]"
        elif entity in self.parent.fullLevelOptions["Fund"]:
            highLook = None
            lowLook = "[Target name]"
        elif entity in self.parent.fullLevelOptions["subAssetClass"]:
            #account, transaction level name 
            highLook = ["[ExposureAssetClassSub-assetClass(E)]", "[SysProp_FundTargetNameSub-assetClass(E)]"]
            lowLook = highLook
            assetLook = True
        elif entity in self.parent.fullLevelOptions["assetClass"]:
            #account, transaction level name 
            highLook = ["[ExposureAssetClass]","[SysProp_FundTargetNameAssetClass(E)]"]
            lowLook = highLook
            assetLook = True
        else:
            QMessageBox.warning(self,"No data found","Selection could not be found in the funds,pools,subAssets, or assets (Selecting total portfolio is not an option)")
            self.close()

        highTables = {"positions_high": accountStart,"transactions_high" : tranStart}
        lowTables = {"positions_low": accountStart,"transactions_low": tranStart}

        all_rows = []
        if self.parent.filterDict["Investor"].currentText() != "":
            for idx, table in enumerate(highTables.keys()):
                try:
                    rows = self.parent.load_from_db(table, f"WHERE {highLook if not assetLook else highLook[idx]} = ? AND [Date] BETWEEN ? AND ?", (entity, highTables[table],allEnd))
                except Exception as e:
                    print(f"Error in call : {e} ; {e.args}")
                    rows = []
                for row in rows or []:
                    row['_source'] = table
                    all_rows.append(row)
        for idx, table in enumerate(lowTables.keys()):
            try:
                rows = self.parent.load_from_db(table, f"WHERE {lowLook if not assetLook else lowLook[idx]} = ? AND [Date] BETWEEN ? AND ?", (entity, lowTables[table],allEnd))
                
            except Exception as e:
                print(f"Error in call : {e}; {e.args}")
                rows = []
            for row in rows or []:
                row['_source'] = table
                all_rows.append(row)

        # 3) Sort by dateTime column (handles ISO or space-separated)
        def parse_dt(s):
            return datetime.strptime(s, "%Y-%m-%dT00:00:00")

        all_rows.sort(key=lambda r: parse_dt(r.get('Date', '')))

        # 4) Collect the union of all column keys
        all_cols = set()
        for row in all_rows:
            all_cols.update(row.keys())
        all_cols = list(all_cols)

        # 5) Configure the table widget
        self.table.setRowCount(len(all_rows))
        self.table.setColumnCount(len(all_cols))
        self.table.setHorizontalHeaderLabels(all_cols)

        # 6) Populate each cell
        for r, row in enumerate(all_rows):
            for c, key in enumerate(all_cols):
                raw = row.get(key,"")
                try:
                    num = float(raw)
                    text = f"{num:,.2f}"
                    item = QTableWidgetItem(text)
                    item.setData(Qt.UserRole,num)
                except:
                    item = QTableWidgetItem(str(raw))
                self.table.setItem(r, c, item)

class tableWindow(QWidget):
    """
    A window that loads data from four database sources in the parent,
    merges and sorts it by dateTime, and displays it in a QTableWidget
    with a unified set of columns.
    """
    def __init__(self, parent=None, flags=Qt.WindowFlags(), parentSource = None, all_rows = [], table = ""):
        super().__init__(parent, flags)
        self.parent = parentSource
        self.setWindowTitle(f"New data in {table}")
        self.resize(1000, 600)

        # Layout and table
        layout = QVBoxLayout(self)
        self.table = QTableWidget(self)
        layout.addWidget(self.table)

        

        # 4) Collect the union of all column keys
        all_cols = set()
        for row in all_rows:
            all_cols.update(row.keys())
        all_cols = list(all_cols)

        # 5) Configure the table widget
        self.table.setRowCount(len(all_rows))
        self.table.setColumnCount(len(all_cols))
        self.table.setHorizontalHeaderLabels(all_cols)

        # 6) Populate each cell
        for r, row in enumerate(all_rows):
            for c, key in enumerate(all_cols):
                raw = row.get(key,"")
                try:
                    num = float(raw)
                    text = f"{num:,.2f}"
                    item = QTableWidgetItem(text)
                    item.setData(Qt.UserRole,num)
                except:
                    item = QTableWidgetItem(str(raw))
                self.table.setItem(r, c, item)

class MultiSelectCombo(QComboBox):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setEditable(True)
        self.lineEdit().setReadOnly(True)
        model = QStandardItemModel(self)
        self.setModel(model)
        self.view().setSelectionMode(Qt.MultiSelection)
        self.view().selectionModel().selectionChanged.connect(self._update_text)

    def addItem(self, text, userData=None):
        item = QStandardItem(text)
        item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        if userData is not None:
            item.setData(userData, Qt.UserRole)
        self.model().appendRow(item)

    def _update_text(self, *args):
        checked = [idx.data() for idx in self.view().selectionModel().selectedIndexes()]
        self.lineEdit().setText(", ".join(checked))

    def checkedItems(self):
        return [idx.data(Qt.UserRole) 
                for idx in self.view().selectionModel().selectedIndexes()]

if __name__ == '__main__':
    key = os.environ.get('Dynamo_API')
    ok = key and key != 'value'
    app = QApplication(sys.argv)
    timer = QTimer()
    timer.timeout.connect(poll_queue)
    timer.start(500)
    w = returnsApp(start_index=0 if not ok else 1)
    if ok: w.api_key = key
    w.show()
    w.init_data_processing()
    sys.exit(app.exec_())
