import sys
import os
import json
import subprocess
import traceback
import sqlite3
import requests
import calendar
import time
import copy
import re
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Alignment
from openpyxl.utils import get_column_letter
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import queue
import threading
from dateutil.relativedelta import relativedelta
from PyQt5.QtWidgets import (
    QApplication, QWidget, QStackedWidget, QVBoxLayout,
    QLabel, QLineEdit, QPushButton, QFormLayout,
    QRadioButton, QButtonGroup, QComboBox, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QProgressBar, QTableView, QCheckBox, QMessageBox,
    QScrollArea, QFileDialog
)
from PyQt5.QtGui import QBrush, QColor, QDesktopServices
from PyQt5.QtCore import Qt, QTimer, QAbstractTableModel, QModelIndex, pyqtSignal, QPoint, QUrl

testDataMode = False
demoMode = True

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
                    trace = traceback.format_exc()
                    print(f"Error occured while attempting to run background gui update: {e}. \n traceback: \n {trace}")
    except queue.Empty:
        pass
# Determine assets path, works in PyInstaller bundle or script
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(BASE_DIR, 'assets')
if testDataMode:
    DATABASE_PATH = os.path.join(ASSETS_DIR, 'Acc_Tran_Test.db')
else:
    DATABASE_PATH = os.path.join(ASSETS_DIR, 'Acc_Tran.db')

if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)

dynamoAPIenvName = "Dynamo_API"
mainURL = "https://api.dynamosoftware.com/api/v2.2"

nameHier = {
                "Family Branch" : {"api" : "Parent investor", "dynHigh" : "Parentinvestor", "local" : "Family Branch"},
                "Unfunded" : {"api" : "Remaining commitment change", "dynLow" : "RemainingCommitmentChange", "local" : "Unfunded", "value" : "CashFlowSys"},
                "Commitment" : {"api" : "Amount" , "dynLow" : "ValueInSystemCurrency", "local" : "Commitment"},
                "Transaction Time" : {"dynLow" : "TradeDate"},
                "sleeve" : {"sleeve" : "sleeve", "fund" : "Name", "local" : "subAssetSleeve"},
                "CashFlow" : {"dynLow" : "CashFlowSys", "dynHigh" : "CashFlowSys"}, 
                "Value" : {"local" : "NAV", "api" : "Value in system currency", "dynLow" : "ValueInSystemCurrency", "dynHigh" : "Value"},
            }

commitmentChangeTransactionTypes = ["Commitment", "Transfer of commitment", "Transfer of commitment (out)", "Secondary - Original commitment (by secondary seller)"]
headerOptions = ["Return","NAV", "Gain", "Ownership" , "MDdenominator", "Commitment", "Unfunded"]
dataOptions = ["Investor","Family Branch","Classification", "dateTime"]
yearOptions = (1,2,3,5,7,10,12,15,20)

options = ["MTD","QTD","YTD", "Ownership", "Return", "ITD"] + [f"{y}YR" for y in yearOptions]
percent_headers = {option for option in options}

class returnsApp(QWidget):
    def __init__(self, start_index=0):
        super().__init__()
        self.setWindowTitle('Returns Calculator')
        self.setGeometry(100, 100, 1000, 600)

        os.makedirs(ASSETS_DIR, exist_ok=True)
        self.start_index = start_index
        self.api_key = None
        self.filterCallLock = False
        self.cancel = False
        self.tableWindows = {}
        self.dataTimeStart = datetime(2021,1,1)
        self.earliestChangeDate = datetime(datetime.now().year,datetime.now().month + 1,datetime.now().day)
        self.currentTableData = None
        self.fullLevelOptions = {}
        self.buildTableCancel = None
        self.buildTableFuture = None
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
        if not demoMode:
            controlsLayout.addWidget(clearButton)
        controlsLayout.addWidget(self.importButton)
        btn_to_results = QPushButton('See Calculation Database')
        btn_to_results.clicked.connect(self.show_results)
        controlsLayout.addWidget(btn_to_results)
        self.exportBtn = QPushButton("Export Current Table to Excel")
        self.exportBtn.clicked.connect(self.exportCurrentTable)
        self.exportBtn.setStyleSheet("""
                            QPushButton {
                                background-color: #51AE2B;
                                color: white;
                                border: 1px solid #33721B;
                                border-radius: 6px;
                                padding: 4px 12px;
                            }
                            QPushButton:hover {
                                background-color: #429321;
                            }
                            QPushButton:pressed {
                                background-color: #33721B;
                            }
                        """)
        controlsLayout.addWidget(self.exportBtn)
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
        self.returnOutputType.addItems(headerOptions)
        self.returnOutputType.currentTextChanged.connect(self.buildReturnTable)
        tableSelectorLayout.addWidget(self.returnOutputType)
        self.dataStartSelect = QComboBox()
        self.dataEndSelect = QComboBox()
        for text, CB in (["Start: ", self.dataStartSelect], ["End: ", self.dataEndSelect]):
            lineBox = QWidget()
            lineLay = QHBoxLayout()
            lineLay.addWidget(QLabel(text))
            lineLay.addWidget(CB)
            lineBox.setLayout(lineLay)
            tableSelectorLayout.addWidget(lineBox)
        tableSelectorLayout.addWidget(QLabel("Sort by: "))
        self.sortHierarchy = MultiSelectBox()
        self.sortHierarchy.hierarchyMode()
        self.sortHierarchy.popup.closed.connect(self.buildReturnTable)
        tableSelectorLayout.addWidget(self.sortHierarchy)
        self.consolidateFundsBtn = QRadioButton("Consolidate Funds")
        self.consolidateFundsBtn.setChecked(True)
        self.consolidateFundsBtn.clicked.connect(self.buildReturnTable)
        tableSelectorLayout.addWidget(self.consolidateFundsBtn)
        self.benchmarkSelection = MultiSelectBox()
        self.benchmarkSelection.popup.closed.connect(self.buildReturnTable)
        tableSelectorLayout.addWidget(self.benchmarkSelection)
        tableSelectorBox.setLayout(tableSelectorLayout)
        filterLayout.addWidget(tableSelectorBox)

        self.filterOptions = [
                            {"key": "Classification", "name": "HF Classification", "dataType" : None, "dynNameLow" : "Target nameExposureHFClassificationLevel2"},
                            {"key" : nameHier["Family Branch"]["local"], "name" : nameHier["Family Branch"]["local"], "dataType" : None, "dynNameLow" : None, "dynNameHigh" : nameHier["Family Branch"]["dynHigh"]},
                            {"key": "Investor",       "name": "Investor", "dataType" : "Investor", "dynNameLow" : None},
                            {"key": "assetClass",     "name": "Asset Class", "dataType" : "Total Asset", "dynNameLow" : "ExposureAssetClass", "dynNameHigh" : "ExposureAssetClass"},
                            {"key": "subAssetClass",  "name": "Sub-Asset Class", "dataType" : "Total subAsset", "dynNameLow" : "ExposureAssetClassSub-assetClass(E)", "dynNameHigh" : "ExposureAssetClassSub-assetClass(E)"},
                            {"key" : nameHier["sleeve"]["local"], "name" : "Sub-Asset Class Sleeve", "dataType" : "Total sleeve", "dynNameLow" : nameHier["sleeve"]["local"]},
                            {"key": "Pool",           "name": "Pool", "dataType" : "Total Pool" , "dynNameLow" : "Source name", "dynNameHigh" : "Target name"},
                            {"key": "Fund",           "name": "Fund/Investment", "dataType" : "Total Fund" , "dynNameLow" : "Target name"},
                            
                        ]
        self.filterBtnExclusions = ["Investor","Classification", nameHier["Family Branch"]["local"]]
        self.highOnlyFilters = ["Investor", nameHier["Family Branch"]["local"]]
        self.filterDict = {}
        self.filterRadioBtnDict = {}
        self.filterBtnGroup = QButtonGroup()
        self.filterBtnGroup.setExclusive(False)
        for filter in self.filterOptions:
            filterBox = QWidget()
            filterBoxLayout = QVBoxLayout()
            if filter["key"] not in self.filterBtnExclusions:
                #investor level is not filterable. It is total portfolio or shows the investors data
                self.filterRadioBtnDict[filter["key"]] = QCheckBox(f"{filter["name"]}:")
                self.filterRadioBtnDict[filter["key"]].setChecked(True)
                self.filterBtnGroup.addButton(self.filterRadioBtnDict[filter["key"]])
                filterBoxLayout.addWidget(self.filterRadioBtnDict[filter["key"]])
            else:
                filterBoxLayout.addWidget(QLabel(f"{filter["name"]}:"))
            if filter["key"] != "Fund":
                self.sortHierarchy.addItem(filter["key"])
            self.filterDict[filter["key"]] = MultiSelectBox()
            self.filterDict[filter["key"]].popup.closed.connect(lambda: self.filterUpdate())
            
            filterBoxLayout.addWidget(self.filterDict[filter["key"]])
            filterBox.setLayout(filterBoxLayout)
            filterLayout.addWidget(filterBox)
        self.filterBtnGroup.buttonToggled.connect(self.filterBtnUpdate)
        fullFilterBox.setLayout(filterLayout)
        layout.addWidget(fullFilterBox)
        t1 = QVBoxLayout() #build table loading bar
        self.buildTableLoadingBox = QWidget()
        t1.addWidget(QLabel("Building returns table..."))
        self.buildTableLoadingBar = QProgressBar()
        self.buildTableLoadingBar.setRange(0,8)
        t1.addWidget(self.buildTableLoadingBar)
        self.buildTableLoadingBox.setLayout(t1)
        self.buildTableLoadingBox.setVisible(False)
        layout.addWidget(self.buildTableLoadingBox)
        self.returnsTable = QTableWidget() #table
        layout.addWidget(self.returnsTable)
        self.viewUnderlyingDataBtn = QPushButton("View Underlying Data")
        self.viewUnderlyingDataBtn.clicked.connect(self.viewUnderlyingData)
        layout.addWidget(self.viewUnderlyingDataBtn)
        


        page.setLayout(layout)
        self.stack.addWidget(page)

        self.pullLevelNames()
        self.updateMonthOptions()
        if self.start_index != 0:
            self.filterUpdate()
        self.dataEndSelect.currentTextChanged.connect(self.buildReturnTable)
        self.dataStartSelect.currentTextChanged.connect(self.buildReturnTable)
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
            if lastImport.month != now.month or now > lastImport + relativedelta(days=1):
                #pull data if in a new month or 5 days have elapsed
                executor.submit(self.pullData)
            else:
                calculations = self.load_from_db("calculations")
                self.findConsolidatedFunds()
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
    def exportCurrentTable(self):
        # helper to darken a 6-digit hex color by a given factor
        def darken_color(hex_color, factor=0.01):
            h = hex_color.strip("#")
            r = int(h[0:2], 16)
            g = int(h[2:4], 16)
            b = int(h[4:6], 16)
            dr = max(0, int(r * factor))
            dg = max(0, int(g * factor))
            db = max(0, int(b * factor))
            return f"{dr:02X}{dg:02X}{db:02X}"
        # 1) prompt user
        path, _ = QFileDialog.getSaveFileName(
            self, "Save as…", "", "Excel Files (*.xlsx)"
        )
        if not path:
            return
        if not path.lower().endswith(".xlsx"):
            path += ".xlsx"

        def processExport():
            data = self.currentTableData  # dict of dicts

            # 2) determine hierarchy levels present
            all_types = {row.get("dataType") for row in data.values()}
            if self.sortHierarchy.checkedItems() != []:
                full_hierarchy = ["Total"] + ["Total " + level for level in self.sortHierarchy.checkedItems()] + ["Total Fund"]
            else:
                full_hierarchy = ["Total", "Total assetClass", "Total Fund"]
            hierarchy_levels = [lvl for lvl in full_hierarchy if lvl in all_types]
            num_hier = len(hierarchy_levels)

            # 3) dynamic data columns minus "dataType"
            all_cols = {
                k for row in data.values() for k in row.keys()
                if k != "dataType"
            }

            sorted_cols = self.orderColumns(all_cols)

            # 4) create workbook
            wb = Workbook()
            ws = wb.active

            appliedFilters = {}
            for filter in self.filterOptions:
                if self.filterDict[filter["key"]].checkedItems() != []:
                    appliedFilters[filter["key"]] = self.filterDict[filter["key"]].checkedItems()

            rowStart = 3
            # 5) header row
            for idx, lvl in enumerate(hierarchy_levels, start=1):
                ws.cell(row=rowStart, column=idx, value=lvl)
            for idx, colname in enumerate(sorted_cols, start=num_hier+1):
                ws.cell(row=rowStart, column=idx, value=colname)

            split_cell = f"{get_column_letter(num_hier+1)}4"
            ws.freeze_panes = split_cell

            # 7) populate rows
            for r, (row_name, row_dict) in enumerate(data.items(), start=rowStart + 1):
                row_name, code = self.separateRowCode(row_name)
                dtype = row_dict.get("dataType")
                level = hierarchy_levels.index(dtype) if dtype in hierarchy_levels else 0

                # fills
                data_color = "FFFFFF"
                if dtype != "Total Fund":
                    depth      = code.count("::") if dtype != "Total" else code.count("::") - 1
                    maxDepth   = max(len(self.sortHierarchy.checkedItems()),1) + 1
                    data_color = darken_color(data_color,depth/maxDepth/3 + 2/3)

                if r % 2 == 1:
                    data_color = darken_color(data_color,0.93)
                header_color = darken_color(data_color, 0.9)
                data_fill   = PatternFill("solid", data_color, data_color)
                header_fill = PatternFill("solid", header_color, header_color)

                # spread header fill across hierarchy cols
                data_start = num_hier + 1
                for col in range(level+1, data_start):
                    cell = ws.cell(row=r, column=col, value=row_name if col==level+1 else None)
                    cell.fill = header_fill
                    if col == level+1:
                        cell.alignment = Alignment(indent=level)

                # data cells with proper formatting
                for c, colname in enumerate(sorted_cols, start=data_start):
                    val = row_dict.get(colname, None)
                    cell = ws.cell(row=r, column=c, value=val)
                    cell.fill = data_fill
                    if isinstance(val, (int, float)):
                        if colname not in percent_headers:
                            # show with commas, two decimals
                            cell.number_format = "#,##0.00"
                        else:
                            # interpret val as percentage (e.g. 10.5 → 10.5%)
                            cell.value = val / 100.0
                            cell.number_format = "0.00%"

            # 8) autofit column widths
            for idx, col_cells in enumerate(ws.columns, start=1):
                max_len = 0
                for cell in col_cells:
                    if cell.value is not None:
                        text = str(cell.value)
                        max_len = max(max_len, len(text))
                ws.column_dimensions[get_column_letter(idx)].width = max_len + 2

            # 9) save and notify
            try:
                wb.save(path)
            except Exception as e:
                gui_queue.put(lambda: QMessageBox.critical(self, "Save error", str(e)))
            else:
                gui_queue.put(lambda: QMessageBox.information(self, "Saved", f"Excel saved to:\n{path}"))
                gui_queue.put(lambda: QDesktopServices.openUrl(QUrl.fromLocalFile(path)))
        executor.submit(processExport)
    def findConsolidatedFunds(self):
        funds = self.load_from_db("funds")
        if funds != []:
            consolidatorFunds = {}
            for row in funds: #find sleeve values and consolidated funds
                if row.get("Fundpipelinestatus") is not None and "Z - Placeholder" in row.get("Fundpipelinestatus"):
                    consolidatorFunds[row["Name"]] = {"cFund" : row["Name"], "assetClass" : assetClass, "subAssetClass" : subAssetClass, "sleeve" : sleeve}
                assetClass = row["assetClass"]
                subAssetClass = row["subAssetClass"]
                sleeve = row["sleeve"]
            self.consolidatedFunds = {}
            for row in funds: #assign funds to their consolidators
                if row.get("Parentfund") in consolidatorFunds:
                    self.consolidatedFunds[row["Name"]] = consolidatorFunds.get(row.get("Parentfund"))
        else:
            self.consolidatedFunds = {}
    def filterBtnUpdate(self, button, checked):
        if not self.filterCallLock:
            self.buildTableLoadingBox.setVisible(True)
            self.buildTableLoadingBar.setValue(1)
            self.filterCallLock = True
            reloadRequired = False
            for filter in self.filterOptions:
                if filter["key"] not in self.filterBtnExclusions:
                    if not self.filterRadioBtnDict[filter["key"]].isChecked():
                        if self.filterDict[filter["key"]].checkedItems() != []:
                            reloadRequired = True #rebuild the table only if filter selections are being removed
                        self.filterDict[filter["key"]].clearSelection()
                        self.filterDict[filter["key"]].setVisible(False)
                    else:
                        self.filterDict[filter["key"]].setVisible(True)
            self.filterCallLock = False
            if reloadRequired or self.currentTableData is None:
                self.buildReturnTable()
            else:
                self.populateReturnsTable(self.currentTableData)
    def resetData(self):
        self.save_to_db("calculations",None,action="reset") #reset calculations so new data will be freshly calculated
        if testDataMode:
            executor.submit(self.calculateReturn)
        else:
            executor.submit(self.pullData)
    def beginImport(self):
        executor.submit(self.pullData)
    def updateMonthOptions(self):
        start = self.dataTimeStart
        end = datetime.now() - relativedelta(months=1) + relativedelta(hours=8)
        #ends on the previous month. Adds a few hours so index will still be before it and count as a month on the 1st
        index = start
        monthList = []
        while index < end:
            monthList.append(datetime.strftime(index,"%B %Y"))
            index += relativedelta(months=1)
        self.dataEndSelect.addItems(monthList)
        self.dataEndSelect.setCurrentText(monthList[-1])
        self.dataStartSelect.addItems(monthList)
        self.dataStartSelect.setCurrentText(monthList[0])
    def buildReturnTable(self):
        self.buildTableLoadingBox.setVisible(True)
        self.buildTableLoadingBar.setValue(2)
        def buildTable(cancelEvent):
            try:
                print("Building return table...")
                self.currentTableData = None #resets so a failed build won't be used
                
                if self.tableBtnGroup.checkedButton().text() == "Complex Table":
                    gui_queue.put(lambda: self.returnOutputType.setCurrentText("Return"))
                    gui_queue.put(lambda: self.returnOutputType.setVisible(False))
                    gui_queue.put(lambda: self.viewUnderlyingDataBtn.setVisible(False))
                else:
                    gui_queue.put(lambda: self.returnOutputType.setVisible(True))
                    gui_queue.put(lambda: self.viewUnderlyingDataBtn.setVisible(True))
                if self.filterDict["Investor"].checkedItems() == [] and self.filterDict[nameHier["Family Branch"]["local"]].checkedItems() == []:
                    #if no investor level selections,show full portfolio information
                    parameters = ["Total Fund"]
                    condStatement = " WHERE [Investor] = ? "
                else:
                    #show investor level fund data
                    condStatement = " WHERE"
                    parameters = []
                    if self.filterDict["Investor"].checkedItems() != []:
                        paramTemp = self.filterDict["Investor"].checkedItems()
                        placeholders = ','.join('?' for _ in paramTemp) 
                        condStatement += f" [Investor] IN ({placeholders}) "
                        for param in paramTemp:
                            parameters.append(param)
                    if self.filterDict[nameHier["Family Branch"]["local"]].checkedItems() != []:
                        paramTemp = self.filterDict[nameHier["Family Branch"]["local"]].checkedItems()
                        placeholders = ','.join('?' for _ in paramTemp)
                        if condStatement == " WHERE":
                            condStatement += f" [{nameHier["Family Branch"]["local"]}] IN ({placeholders}) "
                        else:
                            condStatement += f" AND [{nameHier["Family Branch"]["local"]}] IN ({placeholders}) "
                        for param in paramTemp:
                            parameters.append(param)
                for filter in self.filterOptions:
                    if filter["key"] != "Investor" and filter["key"] != nameHier["Family Branch"]["local"]:
                        if self.filterDict[filter["key"]].checkedItems() != []:
                            paramTemp = self.filterDict[filter["key"]].checkedItems()
                            for param in paramTemp:
                                parameters.append(param)
                            placeholders = ','.join('?' for _ in paramTemp)
                            condStatement += f" AND [{filter["key"]}] IN ({placeholders})"
                gui_queue.put(lambda: self.buildTableLoadingBar.setValue(3))
                if cancelEvent.is_set(): #exit if new table build request is made
                    return
                data = self.load_from_db("calculations",condStatement, tuple(parameters))
                output = {"Total##()##" : {}}
                if self.benchmarkSelection.checkedItems() != []:
                    output = self.applyBenchmarks(output)
                output , data = self.calculateUpperLevels(output,data)
                gui_queue.put(lambda: self.buildTableLoadingBar.setValue(4))
                if cancelEvent.is_set(): #exit if new table build request is made
                    return
                complexOutput = copy.deepcopy(output)
                for entry in data:
                    if (datetime.strptime(entry["dateTime"], "%Y-%m-%d %H:%M:%S") >  datetime.strptime(self.dataEndSelect.currentText(),"%B %Y") or 
                        datetime.strptime(entry["dateTime"], "%Y-%m-%d %H:%M:%S") <  datetime.strptime(self.dataStartSelect.currentText(),"%B %Y")):
                        #don't build in data outside the selection
                        continue
                    date = datetime.strftime(datetime.strptime(entry["dateTime"], "%Y-%m-%d %H:%M:%S"), "%B %Y")
                    Dtype = entry["Calculation Type"]
                    level = entry["rowKey"]

                    dataOutputType = self.returnOutputType.currentText()
                    if level in output.keys():
                        if date not in output[level].keys():
                            #creates value if not exists. If it is not return percent, sums the values
                            output[level][date] = float(entry[dataOutputType])
                        elif dataOutputType != "Return":
                            output[level][date] += float(entry[dataOutputType])
                    else:
                        output[level] = {}
                    if "dataType" not in output[level].keys():
                        output[level]["dataType"] = Dtype
                    if self.tableBtnGroup.checkedButton().text() == "Complex Table" and date == self.dataEndSelect.currentText():
                        if level not in complexOutput.keys():
                            complexOutput[level] = {}
                        if "dataType" not in complexOutput[level].keys():
                            complexOutput[level]["dataType"] = Dtype
                        if headerOptions[0] not in complexOutput[level].keys() and headerOptions:
                            for option in headerOptions:
                                complexOutput[level][option] = float(entry[option] if entry[option] is not None and entry[option] != '' else 0)
                        else:
                            for option in headerOptions:
                                if option != "Ownership":
                                    complexOutput[level][option] += float(entry[option] if entry[option] is not None and entry[option] != '' else 0)
                        if self.filterDict["Investor"].checkedItems() != [] or self.filterDict["Family Branch"].checkedItems() != []:
                            if "Ownership (%)" not in complexOutput[level].keys():
                                complexOutput[level]["Ownership (%)"] = entry["Ownership"]
                            else:
                                complexOutput[level]["Ownership (%)"] += entry["Ownership"]
                gui_queue.put(lambda: self.buildTableLoadingBar.setValue(5))
                if cancelEvent.is_set(): #exit if new table build request is made
                    return
                if self.tableBtnGroup.checkedButton().text() == "Complex Table":
                    output = self.calculateComplexTable(output,complexOutput)
                gui_queue.put(lambda: self.buildTableLoadingBar.setValue(6))
                if cancelEvent.is_set(): #exit if new table build request is made
                    return
                outputKeys = output.keys()
                deleteKeys = []
                for key in outputKeys:
                    if len(output[key].keys()) == 0:
                        deleteKeys.append(key)
                for key in deleteKeys:
                    output.pop(key)
                gui_queue.put(lambda: self.populateReturnsTable(output))
                self.currentTableData = output
            except Exception as e:
                tracebackMsg = traceback.format_exc()
                gui_queue.put(lambda error = e: QMessageBox.warning(self, "Error building returns table", f"Error: {error}. {error.args}. Data entry: \n  \n Traceback:  \n {tracebackMsg}"))
                gui_queue.put(lambda: self.buildTableLoadingBox.setVisible(False))
        if self.buildTableCancel:
            self.buildTableCancel.set()
        if self.buildTableFuture and not self.buildTableFuture.done():
            self.buildTableFuture.cancel()

        cancelEvent = threading.Event()
        self.buildTableCancel = cancelEvent
        self.stack.setCurrentIndex(1)
        future = executor.submit(buildTable, cancelEvent)
        self.buildTableFuture = future
    def calculateComplexTable(self,monthOutput,complexOutput):
        endTime = datetime.strptime(self.dataEndSelect.currentText(),"%B %Y")
        MTDtime = datetime.strftime(endTime,"%B %Y")
        QTDtimes = [datetime.strftime(endTime - relativedelta(months=i),"%B %Y") for i in range(int((endTime.month)) % 3 if (int(endTime.month)) % 3 != 0 else 3)]
        YTDtimes = [datetime.strftime(endTime - relativedelta(months=i),"%B %Y") for i in range(int((endTime.month)) % 12 if (int(endTime.month)) % 12 != 0 else 12)]
        YR_times = {}
        for yr in yearOptions:
            YR_times[yr] = [datetime.strftime(endTime - relativedelta(months=i),"%B %Y") for i in range(12 * yr)]
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
                if monthOutput[level].get("dataType","") != "benchmark":
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
                                if month != "dataType" and datetime.strptime(month,"%B %Y") <= datetime.strptime(self.dataEndSelect.currentText(),"%B %Y"):
                                    monthCount += 1
                                    complexOutput[level]["ITD"] *= (1 + float(monthOutput[level][month]) / 100 )
                            complexOutput[level]["ITD"] = ((complexOutput[level]["ITD"] ** (12/int(monthCount)) ) - 1 ) * 100 if complexOutput[level]["ITD"] > 0 else -1 * ((abs(complexOutput[level]["ITD"]) ** (1/int(monthCount)) ) - 1)* 100
                        else:
                            #ITD is just the previous month if no more months are found
                            complexOutput[level]["ITD"] = monthOutput[level][MTDtime]
                else:
                    complexOutput[level]["ITD"] = monthOutput[level]["ITD"]
            except Exception as e:
                pass
        # for level in monthOutput.keys():
        #     if "dataType" not in complexOutput[level].keys():
        #         complexOutput.pop(level)





        return complexOutput
    def applyBenchmarks(self, output):
        benchmarkChoices = self.benchmarkSelection.checkedItems()
        code = self.buildCode([])
        placeholders = ','.join('?' for _ in benchmarkChoices)
        benchmarks = self.load_from_db("benchmarks",f"WHERE [Index] IN ({placeholders})",tuple(benchmarkChoices))
        for idx, bench in enumerate(benchmarks):
            name = bench["Index"] + code
            if datetime.strptime(bench["Asofdate"], "%Y-%m-%dT%H:%M:%S") < datetime.strptime(self.dataStartSelect.currentText(), "%B %Y"):
                continue #skip if before start time
            date = datetime.strftime(datetime.strptime(bench["Asofdate"], "%Y-%m-%dT%H:%M:%S"), "%B %Y")
            if output.get(name) is None:
                output[name] =  {}
            if self.tableBtnGroup.checkedButton().text() != "Complex Table" and self.returnOutputType.currentText() == "Return": #show monthly return benchmarks
                output[name][date] = float(bench.get("MTDnet",0) if bench.get("MTDnet",0) !=  "None" else 0) * 100
                if output[name].get("dataType") is None:
                    output[name]["dataType"] = "benchmark"
            elif self.tableBtnGroup.checkedButton().text() == "Complex Table" and date == self.dataEndSelect.currentText():
                #populate the complex fields
                if output[name].get("dataType") is None:
                    output[name]["dataType"] = "benchmark"
                for time in ("MTD","QTD","YTD"):
                    if bench.get(f"{time}net", "None") != "None":
                        output[name][time] = float(bench.get(f"{time}net")) * 100
                if bench.get("ITDTWR","None") != "None":
                    output[name]["ITD"] = float(bench.get("ITDTWR")) * 100
                for year in yearOptions:
                    if bench.get(f"Last{year}yrnet","None") not in ("None",None):
                        output[name][f"{year}YR"] = float(bench.get(f"Last{year}yrnet")) * 100
        return output
    def buildCode(self, path):
            code = f"##({"::".join(path)})##"
            return code
    def calculateUpperLevels(self, tableStructure,data):
        
        def buildLevel(levelName,levelIdx, struc,data,path : list):
            levelIdx += 1
            entryTemplate = {"dateTime" : None, "Calculation Type" : "Total " + levelName, "Pool" : None, "Fund" : None ,
                                            "assetClass" : None, "subAssetClass" : None, "Investor" : None,
                                            "Return" : None , nameHier["sleeve"]["local"] : None,
                                            "Ownership" : None}
            for header in headerOptions:
                if header != "Ownership":
                    entryTemplate[header] = 0

            #check for filtering. If none, use all options
            options = []
            for entry in data: #all available data
                if entry[levelName] not in options: #
                    options.append(entry[levelName])
            options.sort()
            newTotalEntries = []
            if len(sortHierarchy) > levelIdx: #more hierarchy levels to parse
                highTotals = [] #all total values made on the level
                for option in options:
                    tempPath = path.copy()
                    tempPath.append(option)
                    
                    highEntries = {}
                    name = option if levelName != "assetClass" or option != "Cash" else "Cash "
                    code = self.buildCode(tempPath)
                    struc[name + code] = {} #place table space for that level selection
                    levelData = []
                    for entry in data: #separates out only relevant data
                        if entry[levelName] == option:
                            levelData.append(entry)
                    struc, lowTotals, fullEntries = buildLevel(sortHierarchy[levelIdx],levelIdx,struc,levelData,tempPath)
                    newTotalEntries.extend(fullEntries)
                #gui_queue.put(lambda rows = highTotals, name = "Entries for: " + ",".join(options): self.openTableWindow(rows,f"{name} data"))
                    for total in lowTotals:
                        if total["dateTime"] not in highEntries.keys():
                            highEntries[total["dateTime"]] = copy.deepcopy(entryTemplate)
                            highEntries[total["dateTime"]]["rowKey"] = name + code
                            for label in dataOptions:
                                highEntries[total["dateTime"]][label] = total[label]
                            if levelName not in ("Investor","Family Branch"):
                                highEntries[total["dateTime"]][levelName] = total[levelName] if total[levelName] != "Cash" or levelName != "assetClass" else "Cash "
                                if levelName == "subAssetClass":
                                    highEntries[total["dateTime"]]["assetClass"] = total["assetClass"] if total["assetClass"] != "Cash" else "Cash "
                        for header in headerOptions:
                            if header != "Ownership":
                                highEntries[total["dateTime"]][header] += float(total[header])
                    for month in highEntries.keys():
                        highEntries[month]["Return"] = highEntries[month]["Gain"] / highEntries[month]["MDdenominator"] * 100 if highEntries[month]["MDdenominator"] != 0 else 0
                        highTotals.append(highEntries[month])
                newTotalEntries.extend(highTotals)       
                #high totals: all totals for the exact level
                #newTotalEntries: all totals for every level being tracked
                return struc, highTotals, newTotalEntries
            else: #occurs at level of fund parent
                newEntriesLow = []
                totalDataLow = []
                for option in options:
                    tempPath = path.copy()
                    tempPath.append(option)
                    totalEntriesLow = {}
                    name = option if levelName != "assetClass" or option != "Cash" else "Cash "
                    code = self.buildCode(tempPath)
                    struc[name + code] =  {}
                    levelData = []
                    for entry in data: #separates out only relevant data
                        if entry[levelName] == option:
                            levelData.append(entry)
                    nameList = []
                    for entry in levelData:
                        fundName = entry["Fund"] if not self.consolidateFundsBtn.isChecked() or entry["Fund"] not in self.consolidatedFunds or entry["Fund"] in self.filterDict["Fund"].checkedItems() else self.consolidatedFunds.get(entry["Fund"]).get("cFund")
                        nameList.append(fundName + code)
                        temp = entry.copy()
                        temp["rowKey"] = fundName + code
                        totalDataLow.append(temp)
                        if entry["dateTime"] not in totalEntriesLow:
                            totalEntriesLow[entry["dateTime"]] = copy.deepcopy(entryTemplate)
                            totalEntriesLow[entry["dateTime"]]["rowKey"] =name + code
                            for label in dataOptions:
                                totalEntriesLow[entry["dateTime"]][label] = entry[label]
                            if levelName not in ("Investor","Family Branch"):
                                totalEntriesLow[entry["dateTime"]][levelName] = entry[levelName] if entry[levelName] != "Cash" or levelName != "assetClass" else "Cash "
                                if levelName == "subAssetClass":
                                    totalEntriesLow[entry["dateTime"]]["assetClass"] = entry["assetClass"] if entry["assetClass"] != "Cash" else "Cash "
                        for header in headerOptions:
                            if header != "Ownership":
                                totalEntriesLow[entry["dateTime"]][header] += float(entry[header])
                    for name in sorted(nameList):
                        struc[name] = {}
                    for month in totalEntriesLow.keys():
                        totalEntriesLow[month]["Return"] = totalEntriesLow[month]["Gain"] / totalEntriesLow[month]["MDdenominator"] * 100 if totalEntriesLow[month]["MDdenominator"] != 0 else 0
                        newEntriesLow.append(totalEntriesLow[month])
                totalDataLow.extend(newEntriesLow)
                return struc, newEntriesLow, totalDataLow

        sortHierarchy = self.sortHierarchy.checkedItems()
        if len(sortHierarchy) < 1:
            sortHierarchy = ["assetClass"] #default option
        levelIdx = 0
        tableStructure, highestEntries, newEntries = buildLevel(sortHierarchy[levelIdx],levelIdx,tableStructure,data, [])
        trueTotalEntries = {}
        for total in highestEntries:
            if total["dateTime"] not in trueTotalEntries.keys():
                trueTotalEntries[total["dateTime"]] = {"dateTime" : None, "Calculation Type" : "Total", "Pool" : None, "Fund" : None ,
                                            "assetClass" : None, "subAssetClass" : None, "Investor" : None,
                                            "Return" : None , nameHier["sleeve"]["local"] : None,
                                            "Ownership" : None}
                trueTotalEntries[total["dateTime"]]["rowKey"] = "Total" + self.buildCode([])
                for header in headerOptions:
                    if header != "Ownership":
                        trueTotalEntries[total["dateTime"]][header] = 0
                for label in dataOptions:
                    trueTotalEntries[total["dateTime"]][label] = total[label]
            for header in headerOptions:
                if header != "Ownership":
                    trueTotalEntries[total["dateTime"]][header] += float(total[header])
        for month in trueTotalEntries.keys():
            trueTotalEntries[month]["Return"] = trueTotalEntries[month]["Gain"] / trueTotalEntries[month]["MDdenominator"] * 100 if trueTotalEntries[month]["MDdenominator"] != 0 else 0
            newEntries.append(trueTotalEntries[month])
        #data.extend(newEntries)
        return tableStructure,newEntries
                    
    def filterUpdate(self):
        def resetOptions(key,options):
            currentSelections = self.filterDict[key].checkedItems()
            multiBox = self.filterDict[key]
            multiBox.clearItems()
            multiBox.addItems(sorted(options))
            for currentText in currentSelections:
                if currentText in options:
                    multiBox.setCheckedItem(currentText)
        def exitFunc():
            self.filterCallLock = False
            gui_queue.put(lambda: self.buildReturnTable())
        if not self.filterCallLock:
            def processFilter():
                try:
                    #prevents recursion on calls from comboboxes being updated
                    self.filterCallLock = True
                    currentChoices = {}
                    for key in self.filterDict.keys():
                        if key not in self.highOnlyFilters:
                            currentChoices[key] = self.filterDict[key].checkedItems()
                    if all(choices == [] for _, choices in currentChoices.items()):
                        for key in currentChoices.keys():
                            gui_queue.put(lambda: resetOptions(key,self.fullLevelOptions[key]))
                        exitFunc()
                        return
                    for filterSwitch in self.filterOptions:
                        if filterSwitch["key"] not in self.highOnlyFilters:
                            condStatement = ""
                            first = True
                            parameters = []
                            for filter in self.filterOptions:
                                if filter["key"] != filterSwitch["key"] and filter["key"] not in self.highOnlyFilters:
                                    if self.filterDict[filter["key"]].checkedItems() != []:
                                        paramTemp = self.filterDict[filter["key"]].checkedItems()
                                        placeholders = ','.join('?' for _ in paramTemp)
                                        if first:
                                            condStatement = f"WHERE [{filter["dynNameLow"]}] IN ({placeholders})"
                                            first = False
                                        else:
                                            condStatement += f" AND [{filter["dynNameLow"]}] IN ({placeholders})"
                                        for param in paramTemp:
                                            parameters.append(param)
                            lowAccounts = self.load_from_db("positions_low", condStatement,tuple(parameters))
                            
                            options = {}
                            for filter in self.filterOptions:
                                options[filter["key"]] = []
                            for account in lowAccounts:
                                for filter in self.filterOptions:
                                    if filter["key"] not in self.highOnlyFilters:
                                        option = account[filter["dynNameLow"]]
                                        if option not in options[filter["key"]] and option is not None:
                                            options[filter["key"]].append(option)
                            gui_queue.put(lambda key = filterSwitch["key"], opts = options[filterSwitch["key"]]: resetOptions(key,opts))
                except:
                    gui_queue.put(lambda: QMessageBox.warning(self,"Filter Error", "Error occured updating filters"))
                exitFunc()
            executor.submit(processFilter)
            return
    def updateMonths(self):
        start = self.dataTimeStart
        end = datetime.now()
        index = start
        monthList = []
        while index < end:
            monthList.append(index)
            index += relativedelta(months=1)
        dbDates = []
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

            
            dateString = monthDT.strftime("%B %Y")

            monthEntry = {"dateTime" : monthDT, "Month" : dateString, "tranStart" : tranStart.removesuffix(".000Z"), "endDay" : bothEnd.removesuffix(".000Z"), "accountStart" : accountStart.removesuffix(".000Z")}
            dbDates.append(monthEntry)
        self.save_to_db("Months",dbDates)

    def pullInvestorNames(self):
        accountsHigh = self.load_from_db('positions_high')
        if accountsHigh is not None:
            investors = []
            familyBranches = []
            for account in accountsHigh:
                if account["Source name"] not in investors:
                    investors.append(account["Source name"])
                if account[nameHier["Family Branch"]["dynHigh"]] not in familyBranches:
                    familyBranches.append(account[nameHier["Family Branch"]["dynHigh"]])
            investors.sort()
            familyBranches.sort()
            self.allInvestors = investors
            self.filterDict["Investor"].addItems(investors)
            self.allFamilyBranches = familyBranches
            self.filterDict[nameHier["Family Branch"]["local"]].addItems(familyBranches)
            self.fullLevelOptions["Investor"] = self.allInvestors
            self.fullLevelOptions["Family Branch"] = self.allFamilyBranches
        else:
            self.allInvestors = []
            self.allFamilyBranches = []
    def pullLevelNames(self):
        allOptions = {}
        for filter in self.filterOptions:
            if filter["key"] not in self.highOnlyFilters:
                allOptions[filter["key"]] = []
        accountsHigh = self.load_from_db("positions_high")
        if accountsHigh is not None:
            for account in accountsHigh:
                for filter in self.filterOptions:
                    if (filter["key"] in allOptions and "dynNameHigh" in filter.keys() and
                        account[filter["dynNameHigh"]] is not None and
                        account[filter["dynNameHigh"]] not in allOptions[filter["key"]]):
                        allOptions[filter["key"]].append(account[filter["dynNameHigh"]])
        else:
            print("no investor to pool accounts found")
        accountsLow = self.load_from_db("positions_low")
        if accountsLow is not None:
            for lowAccount in accountsLow:
                for filter in self.filterOptions:
                    if (filter["key"] in allOptions and "dynNameLow" in filter.keys() and
                        lowAccount[filter["dynNameLow"]] is not None and
                        lowAccount[filter["dynNameLow"]] not in allOptions[filter["key"]]):
                        allOptions[filter["key"]].append(lowAccount[filter["dynNameLow"]])
        else:
            print("no pool to fund accounts found")
        self.fullLevelOptions = {}
        for filter in self.filterOptions:
            if filter["key"] in allOptions:
                allOptions[filter["key"]].sort()
                self.filterDict[filter["key"]].addItems(allOptions[filter["key"]])
                self.fullLevelOptions[filter["key"]] = allOptions[filter["key"]]
        self.filterDict["Classification"].setCheckedItem("HFC")
        self.pullInvestorNames()
        self.pullBenchmarks()

    def pullBenchmarks(self):
        benchmarks = self.load_from_db("benchmarks")
        benchNames = []
        for bench in benchmarks:
            if bench["Index"] not in benchNames:
                benchNames.append(bench["Index"])
        self.benchmarkSelection.addItems(benchNames)
    def check_api_key(self):
        key = self.api_input.text().strip()
        if key:
            headers = {
                "Authorization": f"Bearer {key}",
                "Content-Type":  "application/json"
            }
            payload = {
                "advf": [{ "_name": "Fund" }],
                "mode": "compact",
                "page": {"size": 0}
            }
            resp = requests.get(f"{mainURL}/Entity", headers=headers, json=payload)
            if resp.status_code == 200:
                self.api_label.setText('API key valid. Saving to system...')
                subprocess.run(['setx',dynamoAPIenvName,key], check=True)
                os.environ[dynamoAPIenvName] = key
                self.api_key = key
                self.stack.setCurrentIndex(1)
                self.init_data_processing()
            else:
                self.api_label.setText('Invalid API key')
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
                    value = rec[nameHier["Value"]["dynHigh"] if "position" in table else nameHier["CashFlow"]["dynLow"]]
                    value = 0 if value is None or value == "None" else value
                    seen.add((
                        rec['Source name'] if rec['Source name'] is not None else "None",
                        rec['Target name'] if rec['Target name'] is not None else "None",
                        round(float(value)),               # normalize to float
                        rec['Date'].replace(' ', 'T')      # normalize format if needed
                    ))

                earliest = None
                for rec in rows:
                    value = rec[nameHier["Value"]["dynHigh"] if "position" in table else nameHier["CashFlow"]["dynLow"]]
                    value = 0 if value is None or value == "None" else value
                    key = (
                        rec['Source name'] if rec['Source name'] is not None else "None",
                        rec['Target name'] if rec['Target name'] is not None else "None",
                        round(float(value)),               
                        rec['Date'].replace(' ', 'T')
                    )
                    if key in seen:
                        continue
                    diffCount += 1
                    differences.append(rec)
                    differences.append({"Source name" : key[0],"Target name" : key[1],nameHier["Value"]["dynLow"] : key[2],"Date" : key[3]})
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
        try:
            self.earliestChangeDate = datetime(datetime.now().year,datetime.now().month + 1,datetime.now().day)
            gui_queue.put(lambda: self.apiLoadingBarBox.setVisible(True))
            gui_queue.put(lambda: self.importButton.setEnabled(False))
            self.updateMonths()
            startDate = datetime.strftime(self.dataTimeStart, "%Y-%m-%dT00:00:00.000Z")
            endDate = datetime.strftime(datetime.now(), "%Y-%m-%dT00:00:00.000Z")
            self.pullInvestorNames()
            apiData = {
                "tranCols": "Investment in, Investing Entity, Transaction Type, Effective date, Asset Class (E), Sub-asset class (E), HF Classification, Remaining commitment change, Trade date/time, Amount in system currency, Cash flow change (USD)",
                "tranName": "InvestmentTransaction",
                "tranSort": "Effective date:desc",
                "accountCols": "As of Date, Balance Type, Asset Class, Sub-asset class, Value of Investments, Investing entity, Investment in, HF Classification, Parent investor, Value in system currency",
                "accountName": "InvestmentPosition",
                "accountSort": "As of Date:desc",
                "fundCols" : "Fund Name, Asset class category, Parent fund, Fund Pipeline Status",
                "benchCols" : (f"Index, As of date, MTD %, QTD %, YTD %, ITD cumulative %, ITD TWRR %, "
                               f"{', '.join(f'Last {y} yr %' for y in yearOptions)}"), 
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
                    gui_queue.put(lambda val = loadingIdx: self.apiLoadingBar.setValue(int((val)/6 * 100)))
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
                                                        "_prop": "Investing entity",
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
                                            },
                                            {
                                                "_name": "InvestmentTransaction",
                                                "rule": [
                                                    {
                                                        "_op": "any_item",
                                                        "_prop": "Transaction type",
                                                        "values": [
                                                            [
                                                                {
                                                                    "id": "5327639c-8160-4d85-9b23-8c6bf60c5406",
                                                                    "es": "L_TransactionType",
                                                                    "name": "Commitment"
                                                                },
                                                                {
                                                                    "id": "37339e7c-1c24-4d13-9d17-86d0efe079b3",
                                                                    "es": "L_TransactionType",
                                                                    "name": "Transfer of commitment"
                                                                },
                                                                {
                                                                    "id": "0f8f8671-8579-49d7-b604-05300b6a3990",
                                                                    "es": "L_TransactionType",
                                                                    "name": "Transfer of commitment (out)"
                                                                },
                                                                {
                                                                    "id": "5e098d83-70b0-4135-a629-aff19048fb1c",
                                                                    "es": "L_TransactionType",
                                                                    "name": "Secondary - Original commitment (by secondary seller)"
                                                                }
                                                            ]
                                                        ]
                                                    },
                                                    {
                                                        "_op": "all",
                                                        "_prop": "Investing entity",
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
                    rows = []
                    idx = 0
                    while rows in ([],None) and idx < 3: #if call fails, tries again
                        idx += 1
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
                            for row in rows:
                                row[nameHier["Unfunded"]["local"]] = 0
                                row[nameHier["Commitment"]["local"]] = 0
                                row[nameHier["sleeve"]["local"]] = None
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
            fundPayload = {
                            "advf": {
                                "e": [
                                    {
                                        "_name": "Fund"
                                    }
                                ]
                            },
                            "mode": "compact"
                        }
            fundHeaders = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "x-columns": apiData["fundCols"],
                }
            gui_queue.put(lambda: self.apiLoadingBar.setValue(int((4)/6 * 100)))
            response = requests.post(f"{mainURL}/Search", headers=fundHeaders, data=json.dumps(fundPayload))
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict):
                        rows = data.get('data', data.get('rows', []))
                    elif isinstance(data, list):
                        rows = data
                    else:
                        rows = []
                    keys_to_remove = {'_id', '_es'}
                    rows = [{k: v for k, v in row.items() if k not in keys_to_remove} for row in rows]
                    consolidatorFunds = {}
                    for idx, row in enumerate(rows): #find sleeve values and consolidated funds
                        assetCat = row["ExposureAssetClassCategory"]
                        if assetCat is not None and assetCat.count(" > ") == 3:
                            assetClass = assetCat.split(" > ")[1]
                            subAssetClass = assetCat.split(" > ")[2]
                            sleeve = assetCat.split(" > ")[3]
                        elif assetCat is not None and assetCat.count(" > ") == 2:
                            assetClass = assetCat.split(" > ")[1]
                            subAssetClass = assetCat.split(" > ")[2]
                            sleeve = None
                        elif assetCat is not None and assetCat.count(" > ") == 1:
                            assetClass = assetCat.split(" > ")[1]
                            subAssetClass = None
                            sleeve = None
                        else:
                            assetClass = None
                            subAssetClass = None
                            sleeve = None
                        if row.get("Fundpipelinestatus") is not None and "Z - Placeholder" in row.get("Fundpipelinestatus"):
                            consolidatorFunds[row["Name"]] = {"cFund" : row["Name"], "assetClass" : assetClass, "subAssetClass" : subAssetClass, "sleeve" : sleeve}
                        rows[idx][nameHier["sleeve"]["sleeve"]] =  sleeve
                        rows[idx]["assetClass"] = assetClass
                        rows[idx]["subAssetClass"] = subAssetClass
                    self.consolidatedFunds = {}
                    for row in rows: #assign funds to their consolidators
                        if row.get("Parentfund") in consolidatorFunds:
                            self.consolidatedFunds[row["Name"]] = consolidatorFunds.get(row.get("Parentfund"))
                    if rows != []:
                        self.save_to_db("funds",rows)
                except Exception as e:
                    print(f"Error proccessing fund API data : {e} {e.args}.  {traceback.format_exc()}")
                
            else:
                print(f"Error in API call for fund. Code: {response.status_code}. {response}. {traceback.format_exc()}")
            benchmarkPayload = {
                                    "advf": {
                                        "e": [
                                            {
                                                "_name": "IndexPerformance"
                                            }
                                        ]
                                    },
                                    "mode": "compact"
                                }
            benchmarkHeaders = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "x-columns": apiData["benchCols"],
                }
            gui_queue.put(lambda: self.apiLoadingBar.setValue(int((5)/6 * 100)))
            response = requests.post(f"{mainURL}/Search", headers=benchmarkHeaders, data=json.dumps(benchmarkPayload))
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict):
                        rows = data.get('data', data.get('rows', []))
                    elif isinstance(data, list):
                        rows = data
                    else:
                        rows = []
                    keys_to_remove = {'_id', '_es'}
                    rows = [{k: v for k, v in row.items() if k not in keys_to_remove} for row in rows]
                    self.save_to_db("benchmarks",rows)
                except Exception as e:
                    print(f"Error proccessing benchmark API data : {e} {e.args}.  {traceback.format_exc()}")
                
            else:
                print(f"Error in API call for benchmarks. Code: {response.status_code}. {response}. {traceback.format_exc()}")
            if skipCalculations:
                print("Earliest change: ", self.earliestChangeDate)
            gui_queue.put(lambda: self.apiLoadingBar.setValue(100))
            gui_queue.put(lambda: self.stack.setCurrentIndex(2))
            
            while not gui_queue.empty(): #wait to assure database has been updated in main thread before continuing
                time.sleep(0.2)
            


            self.save_to_db("history",None,action="reset") #clears history then updates most recent import
            currentTime = datetime.now().strftime("%B %d, %Y @ %I:%M %p")
            self.save_to_db("history",[{"lastImport" : currentTime}])
            self.lastImportLabel.setText(f"Last Data Import: {currentTime}")
            gui_queue.put(lambda: self.apiLoadingBarBox.setVisible(False))
            self.calculateReturn()
        except Exception as e:
            QMessageBox.warning(self,"Error Importing Data", f"Error pulling data from dynamo: {e} , {e.args}")
        if not testDataMode:
            gui_queue.put(lambda: self.importButton.setEnabled(True))
        gui_queue.put(lambda: self.apiLoadingBarBox.setVisible(False))
    def openTableWindow(self, rows, name = "Table"):
        window = tableWindow(parentSource=self,all_rows=rows,table=name)
        self.tableWindows[name] = window
        window.show()
    def calculateReturn(self):
        try:
            calculationStart = datetime.now()
            gui_queue.put(lambda: self.importButton.setEnabled(False))
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(True))
            self.updateMonths()
            gui_queue.put(lambda: self.pullLevelNames())
            gui_queue.put(lambda : self.stack.setCurrentIndex(2))
            print("Calculating return....")
            fundListDB = self.load_from_db("funds")
            fundList = {}
            for fund in fundListDB:
                fundList[fund["Name"]] = fund[nameHier["sleeve"]["sleeve"]]
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
                    loadingFraction = (
                                            (monthIdx - skippedMonths) + (poolIdx / len(pools))
                                        ) / countedMonths
                    perc = max(0, min(100, int(loadingFraction * 100)))
                    gui_queue.put(lambda: self.calculationLoadingBar.setValue(perc))
                    if loadingFraction > 0.25:
                        loadingFraction = (loadingFraction - 0.25) / 0.75
                        timeElapsed = datetime.now() - calculationStart
                        secsElapsed = timeElapsed.total_seconds()
                        if loadingFraction > 0:
                            est_total_secs = secsElapsed / loadingFraction
                            secs_remaining = est_total_secs - secsElapsed
                        else:
                            secs_remaining = 0
                        mins, secs = divmod(int(secs_remaining), 60)
                        time_str = f"{mins}m {secs}s" # format as “Xm Ys” or “MM:SS”
                        gui_queue.put(lambda: self.calculationLabel.setText(f"Calculating Financial Data for : {month['Month']} (Estimated time remaining: {time_str})"))
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
                        fundClassification = None
                        startEntry = self.load_from_db("positions_low", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?",(pool, fund,month["accountStart"]))
                        endEntry = self.load_from_db("positions_low", f"WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?",(pool, fund,month["endDay"]))
                        createFinalValue = False
                        noStartValue = False
                        if len(startEntry) < 1:
                            startEntry = [{nameHier["Value"]["dynLow"] : 0}]
                            noStartValue = True
                            commitment = 0
                            unfunded = 0
                        else:
                            assetClass = startEntry[0]["ExposureAssetClass"]
                            subAssetClass = startEntry[0]["ExposureAssetClassSub-assetClass(E)"]
                            fundClassification = startEntry[0]["Target nameExposureHFClassificationLevel2"]
                            if nameHier["Commitment"]["local"] in startEntry[0].keys() and nameHier["Unfunded"]["local"] in startEntry[0].keys():
                                commitment = float(startEntry[0][nameHier["Commitment"]["local"]])
                                unfunded = float(startEntry[0][nameHier["Unfunded"]["local"]])
                            else:
                                commitment = 0
                                unfunded = 0
                        if len(startEntry) > 1: #combines the values for fund sub classes
                            for entry in startEntry[1:]:
                                startEntry[0][nameHier["Value"]["dynLow"]] = str(float(startEntry[0][nameHier["Value"]["dynLow"]]) + float(entry[nameHier["Value"]["dynLow"]])) #adds values to the first index
                        if len(endEntry) < 1:
                            createFinalValue = True
                            endEntry = [{nameHier["Value"]["dynLow"] : 0}]
                        elif assetClass is None or subAssetClass is None or fundClassification is None:
                            assetClass = endEntry[0]["ExposureAssetClass"]
                            subAssetClass = endEntry[0]["ExposureAssetClassSub-assetClass(E)"]
                            fundClassification = endEntry[0]["Target nameExposureHFClassificationLevel2"]
                        if len(endEntry) > 1:
                            for entry in endEntry[1:]:
                                endEntry[0][nameHier["Value"]["dynLow"]] = str(float(endEntry[0][nameHier["Value"]["dynLow"]]) + float(entry[nameHier["Value"]["dynLow"]])) #adds values to the first index
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
                            if fundClassification is None or fundClassification == "None":
                                fundClassification = transaction["Target nameExposureHFClassificationLevel2"]
                            if transaction["TransactionType"] not in commitmentChangeTransactionTypes and transaction[nameHier["CashFlow"]["dynLow"]] not in (None, "None"):
                                cashFlowSum -= float(transaction[nameHier["CashFlow"]["dynLow"]])
                                backDate = self.calculateBackdate(transaction, noStartValue)
                                weightedCashFlow -= float(transaction[nameHier["CashFlow"]["dynLow"]])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day) + backDate)/totalDays
                                if transaction.get(nameHier["Unfunded"]["dynLow"]) not in (None,"None"):
                                    unfunded += float(transaction[nameHier["Unfunded"]["value"]])
                            elif transaction["TransactionType"] in commitmentChangeTransactionTypes:
                                commitment += float(transaction[nameHier["Commitment"]["dynLow"]])
                                unfunded += float(transaction[nameHier["Commitment"]["dynLow"]])
                        try:
                            if startEntry[nameHier["Value"]["dynLow"]] in (None, "None"):
                                startEntry[nameHier["Value"]["dynLow"]] = 0
                            if endEntry[nameHier["Value"]["dynLow"]] in (None, "None"):
                                endEntry[nameHier["Value"]["dynLow"]] = 0
                            if createFinalValue:
                                #implies there is no gain (cash account)
                                endEntry[nameHier["Value"]["dynLow"]] = float(startEntry[nameHier["Value"]["dynLow"]]) + cashFlowSum    
                            fundGain = (float(endEntry[nameHier["Value"]["dynLow"]]) - float(startEntry[nameHier["Value"]["dynLow"]]) - cashFlowSum)
                            fundMDdenominator = float(startEntry[nameHier["Value"]["dynLow"]]) + weightedCashFlow
                            fundNAV = float(endEntry[nameHier["Value"]["dynLow"]])
                            fundReturn = fundGain/fundMDdenominator * 100 if fundMDdenominator != 0 else 0
                            if fundNAV == 0 and fundMDdenominator == 0 and unfunded == 0:
                                #skip if there is no value and no change in value
                                continue
                            elif createFinalValue:
                                fundEOMentry = {"Date" : month["endDay"], "Source name" : pool, "Target name" : fund , nameHier["Value"]["dynLow"] : endEntry[nameHier["Value"]["dynLow"]],
                                                    "Balancetype" : "Calculated_R", "ExposureAssetClass" : assetClass, "ExposureAssetClassSub-assetClass(E)" : subAssetClass,
                                                    nameHier["Commitment"]["local"] : commitment, nameHier["Unfunded"]["local"] : unfunded,
                                                    nameHier["sleeve"]["local"] : fundList[fund]}
                                self.save_to_db("positions_low",fundEOMentry, action="add")
                            else:
                                query = f"UPDATE positions_low SET [{nameHier['Commitment']["local"]}] = ? , [{nameHier['Unfunded']["local"]}] = ?, [{nameHier["sleeve"]["local"]}] = ? WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?"
                                inputs = (commitment,unfunded,fundList[fund],pool,fund,month["endDay"])
                                self.save_to_db("positions_low",None, action = "replace", query=query, inputs = inputs)
                            poolGainSum += fundGain
                            poolMDdenominator += fundMDdenominator
                            poolNAV += fundNAV
                            poolWeightedCashFlow += weightedCashFlow
                            monthFundEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Fund", "Pool" : pool, "Fund" : fund ,
                                            "assetClass" : assetClass, "subAssetClass" : subAssetClass,
                                            "NAV" : fundNAV, "Gain" : fundGain, "Return" : fundReturn , 
                                            "MDdenominator" : fundMDdenominator, "Ownership" : "", "Classification" : fundClassification,
                                            "Calculation Type" : "Total Fund",
                                            nameHier["sleeve"]["local"] : fundList.get(fund),
                                            nameHier["Commitment"]["local"] : commitment,
                                            nameHier["Unfunded"]["local"] : unfunded}
                            if fund not in (None,"None"): #removing blank funds (found duplicate of Monogram in 'HF Direct Investments Pool, LLC - PE (2021)' with most None values)
                                calculations.append(monthFundEntry)
                                fundEntryList.append(monthFundEntry)


                        except Exception as e:
                            print(f"Skipped fund {fund} for {pool} in {month["Month"]} because: {e} {e.args}")
                            #skips fund if the values are zero and cause an error
                    if poolNAV == 0 and poolWeightedCashFlow == 0:
                        #skips the pool if there is no cash flow or value in the pool
                        continue
                    poolReturn = poolGainSum/poolMDdenominator * 100 if poolMDdenominator != 0 else 0
                    monthPoolEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Pool", "Pool" : pool, "Fund" : None ,
                                      "assetClass" : poolDict["assetClass"], "subAssetClass" : poolDict["subAssetClass"] ,
                                      "NAV" : poolNAV, "Gain" : poolGainSum, "Return" : poolReturn , "MDdenominator" : poolMDdenominator,
                                        "Ownership" : None, "Calculation Type" : "Total Fund"}
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
                            investorCashFlowSum -= float(transaction[nameHier["CashFlow"]["dynHigh"]])
                            backDate = self.calculateBackdate(transaction)
                            investorWeightedCashFlow -= float(transaction[nameHier["CashFlow"]["dynHigh"]])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day) + backDate)/totalDays
                        investorMDdenominator = float(startEntry[nameHier["Value"]["dynHigh"]]) + investorWeightedCashFlow
                        tempInvestorDict["MDden"] = investorMDdenominator
                        tempInvestorDict["cashFlow"] = investorCashFlowSum
                        tempInvestorDict["startVal"] = float(startEntry[nameHier["Value"]["dynHigh"]])
                        tempInvestorDict["ExposureAssetClass"] = startEntry["ExposureAssetClass"]
                        tempInvestorDict["ExposureAssetClassSub-assetClass(E)"] = startEntry["ExposureAssetClassSub-assetClass(E)"]
                        tempInvestorDict[nameHier["Family Branch"]["local"]] = startEntry[nameHier["Family Branch"]["dynHigh"]]
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
                            monthPoolEntryInvestor[nameHier["Family Branch"]["local"]] = tempInvestorDicts[investor][nameHier["Family Branch"]["local"]]
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
                                EOMentry = {"Date" : month["endDay"], "Source name" : investor, "Target name" : pool, nameHier["Value"]["dynHigh"] : investorEOM,
                                             "Balancetype" : "Calculated_R", "ExposureAssetClass" : tempInvestorDicts[investor]["ExposureAssetClass"],
                                               "ExposureAssetClassSub-assetClass(E)" : tempInvestorDicts[investor]["ExposureAssetClassSub-assetClass(E)"],
                                               nameHier["Family Branch"]["dynHigh"] : tempInvestorDicts[investor][nameHier["Family Branch"]["local"]]}
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
                            fundInvestorCommitment = fundEntry[nameHier["Commitment"]["local"]] * fundInvestorOwnership
                            fundInvestorUnfunded = fundEntry[nameHier["Unfunded"]["local"]] * fundInvestorOwnership
                            monthFundInvestorEntry = {"dateTime" : month["dateTime"], "Investor" : investorEntry["Investor"], "Pool" : pool, "Fund" : fundEntry["Fund"] ,
                                            "assetClass" : fundEntry["assetClass"], "subAssetClass" : fundEntry["subAssetClass"],
                                            "NAV" : fundInvestorNAV, "Gain" : fundInvestorGain , "Return" :  fundInvestorReturn * 100, 
                                            "MDdenominator" : fundInvestorMDdenominator, "Ownership" : fundInvestorOwnership * 100,
                                            "Classification" : fundEntry["Classification"], nameHier["Family Branch"]["local"] : investorEntry[nameHier["Family Branch"]["local"]],
                                            nameHier["Commitment"]["local"] : fundInvestorCommitment, nameHier["Unfunded"]["local"] : fundInvestorUnfunded, 
                                            "Calculation Type" : "Total Fund",
                                            nameHier["sleeve"]["local"] : fundList.get(fundEntry["Fund"])
                                            }
                            calculations.append(monthFundInvestorEntry)
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
            gui_queue.put(lambda: self.importButton.setEnabled(True))
            print(f"Error occured running calculations: {e}")
            print("e.args:", e.args)
            # maybe also:
            print(traceback.format_exc())
        
    def calculateBackdate(self,transaction,noStartValue = False):
        if transaction.get(nameHier["Transaction Time"]["dynLow"]) not in (None,"None"):
            time = datetime.strptime(transaction.get(nameHier["Transaction Time"]["dynLow"]), "%Y-%m-%dT%H:%M:%S")
            if time.hour == 23 and time.minute == 59:
                #don't backdate if transaction was at the end of the day
                backDate = 0
            else:
                backDate = 1 #backdate if beginning of day
        elif datetime.strptime(transaction.get("Date"), "%Y-%m-%dT%H:%M:%S").day == 1 and noStartValue:
            backDate = 1
        else:
            backDate = 0
        return backDate
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
    def separateRowCode(self, label):
        header = re.sub(r'##\(.*\)##', '', label, flags=re.DOTALL)
        code = re.findall(r'##\(.*\)##', label, flags=re.DOTALL)[0]
        return header, code
    def orderColumns(self,keys):
        mode = self.tableBtnGroup.checkedButton().text()
        if mode == "Monthly Table":
            dates = [datetime.strptime(k, "%B %Y") for k in keys]
            dates = sorted(dates, reverse=True)
            keys = [d.strftime("%B %Y") for d in dates]
        elif mode == "Complex Table":
            newOrder = ["NAV","Gain","Ownership (%)","MTD","QTD","YTD"] + [f"{y}YR" for y in yearOptions] + ["ITD"]
            ordered = [h for h in newOrder if h in keys]
            ordered += [h for h in keys if h not in newOrder]
            keys = ordered
        return keys
    def populateReturnsTable(self, origRows: dict):
        self.buildTableLoadingBar.setValue(7)
        mode = self.tableBtnGroup.checkedButton().text()
        if not origRows:
            # nothing to show
            self.returnsTable.clear()
            self.returnsTable.setRowCount(0)
            self.returnsTable.setColumnCount(0)
            self.buildTableLoadingBox.setVisible(False)
            return

        # 0) Deep-copy & filter as before
        rows = copy.deepcopy(origRows)
        for f in self.filterOptions:
            if f["key"] not in self.filterBtnExclusions and not self.filterRadioBtnDict[f["key"]].isChecked():
                to_delete = [k for k,v in rows.items() if v["dataType"] == f["dataType"]]
                for k in to_delete:
                    rows.pop(k)

        # 1) Build a flat list of row-entries:
        #    each entry = (fund_label, unique_code, row_dict)
        row_entries = []
        for fund_label, row_dict in rows.items():
            row_label, code = self.separateRowCode(fund_label)
            row_entries.append((row_label, code, row_dict))

        # 2) Determine columns exactly as before, using cleanedRows for header order
        cleaned = {fund: d.copy() for fund, _, d in row_entries}
        for d in cleaned.values():
            d.pop("dataType", None)

        col_keys = set()
        for d in cleaned.values():
            col_keys |= set(d.keys())
        col_keys = list(col_keys)

        col_keys = self.orderColumns(col_keys)

        # 3) Resize & set horizontal headers (we no longer call setVerticalHeaderLabels)
        self.returnsTable.setRowCount(len(row_entries))
        self.returnsTable.setColumnCount(len(col_keys))
        self.returnsTable.setHorizontalHeaderLabels(col_keys)

        # Which columns should show as percents?
        percent_cols = {
            ci for ci in range(len(col_keys))
            if col_keys[ci] in percent_headers
        }

        # 4) Populate each row
        for r, (fund_label, code, row_dict) in enumerate(row_entries):
            # pull & remove dataType for coloring
            dataType = row_dict.pop("dataType", "")

            startColor = (160, 160, 160)
            if dataType != "Total":
                depth      = code.count("::") if dataType != "Total Fund" else code.count("::") + 1
                # if len(re.findall(r'##\((.*?)\)##', code, flags=re.DOTALL)[0]) > 0:
                #     depth -= 1
                maxDepth   = max(len(self.sortHierarchy.checkedItems()),1)
                cRange     = 255 - startColor[0]
                ratio      = (depth / maxDepth) if maxDepth != 0 else 1
                color = tuple(
                    int(startColor[i] + cRange * ratio)
                    for i in range(3)
                )
                bg = QColor(*color)
            else:
                bg =  QColor(*tuple(
                    int(startColor[i] * 0.8)
                    for i in range(3)
                ))

            # — vertical header: only show the fund, stash the code —
            hdr = QTableWidgetItem(fund_label)
            hdr.setData(Qt.UserRole, code)
            if bg:
                hdr.setBackground(QBrush(bg))
            self.returnsTable.setVerticalHeaderItem(r, hdr)

            # — fill cells —
            for c, col in enumerate(col_keys):
                raw = row_dict.get(col, "")
                if raw not in (None, "", "None"):
                    try:
                        v = round(float(raw), 2)
                        if c in percent_cols or (mode == "Monthly Table" and self.returnOutputType.currentText() == "Return"):
                            text = f"{v:.2f}%"
                        else:
                            text = f"{v:,.2f}"
                    except:
                        text = str(raw)
                else:
                    text = ""

                item = QTableWidgetItem(text)
                if text:
                    # store raw number for sorting or later retrieval
                    item.setData(Qt.UserRole, v)
                if bg:
                    item.setBackground(QBrush(bg))
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.returnsTable.setItem(r, c, item)
        self.buildTableLoadingBox.setVisible(False)
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
                        print(f"Info: {e}, {e.args}")
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
        if self.parent.filterDict["Investor"].checkedItems() != []:
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
        self.table.setSortingEnabled(True)
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

class ClickableLineEdit(QLineEdit):
    clicked = pyqtSignal()
    def mousePressEvent(self, event):
        super().mousePressEvent(event)
        self.clicked.emit()

class PopupPanel(QWidget):
    closed = pyqtSignal()
    def __init__(self, parent=None):
        super().__init__(parent, flags=Qt.Popup)
        self.parent = parent
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4,4,4,4)
        # ClearFilter button
        self.clear_btn = QPushButton("Clear", self)
        layout.addWidget(self.clear_btn)
        self.all_btn = QPushButton("Select All", self)
        layout.addWidget(self.all_btn)
        # scrollable checkbox area
        self.scroll = QScrollArea(self)
        self.scroll.setWidgetResizable(True)
        container = QWidget()
        self.box_layout = QVBoxLayout(container)
        self.box_layout.addStretch()
        container.setLayout(self.box_layout)
        self.scroll.setWidget(container)
        self.scroll.setFixedHeight(150)
        layout.addWidget(self.scroll)

    def hideEvent(self, event):
        super().hideEvent(event)
        if self.parent.choiceChange: #only emit if there is changes
            self.closed.emit()


class MultiSelectBox(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        # ——— top line edit ———
        self.line_edit = ClickableLineEdit(self)
        self.line_edit.setReadOnly(True)
        self.line_edit.setPlaceholderText("Click to select…")
        self.line_edit.clicked.connect(self._togglePopup)
        self.hierarchy = False
        self.currentItems = []

        # ——— pop-up panel ———
        self.popup = PopupPanel(self)
        # wire up clear button
        self.popup.clear_btn.clicked.connect(self.clearSelection)
        self.popup.all_btn.clicked.connect(self.selectAll)
        # react when popup closes

        # keep track of checkboxes
        self._checkboxes = {}

        # overall layout
        main = QVBoxLayout(self)
        main.addWidget(self.line_edit)
        main.setContentsMargins(0,0,0,0)
        self.setLayout(main)
    def hierarchyMode(self):
        self.hierarchy = True
    def _togglePopup(self):
        if self.popup.isVisible():
            self.popup.hide()
        else:
            p = self.line_edit.mapToGlobal(QPoint(0, self.line_edit.height()))
            self.popup.move(p)
            self.popup.resize(self.line_edit.width(),
                              self.popup.sizeHint().height())
            self.popup.show()
            self.choiceChange = False
    def addItems(self,items):
        for item in items:
            self.addItem(item)
    def addItem(self, text):
        if text in self._checkboxes:
            return
        cb = QCheckBox(text, self.popup)
        cb.stateChanged.connect(self._updateLine)
        self.popup.box_layout.insertWidget(
            self.popup.box_layout.count() - 1, cb
        )
        self._checkboxes[text] = cb

    def clearItems(self):
        for cb in self._checkboxes.values():
            self.popup.box_layout.removeWidget(cb)
            cb.deleteLater()
        self._checkboxes.clear()
        self._updateLine()

    def setCheckedItems(self, items):
        for text, cb in self._checkboxes.items():
            if text in items:
                cb.setChecked(True)
        self._updateLine()
    def setCheckedItem(self, item):
        for text, cb in self._checkboxes.items():
            if text == item:
                cb.setChecked(True)
        self._updateLine()

    def checkedItems(self):
        if self.hierarchy:
            return self.currentItems
        else:
            return [t for t, cb in self._checkboxes.items() if cb.isChecked()]

    def clearSelection(self):
        for cb in self._checkboxes.values():
            cb.setChecked(False)
        self._updateLine()
    def selectAll(self):
        for cb in self._checkboxes.values():
            cb.setChecked(True)
        self._updateLine()

    def _updateLine(self):
        self.choiceChange = True
        temp = self.hierarchy
        self.hierarchy = False
        sel = self.checkedItems()
        self.hierarchy = temp
        if self.hierarchy:
            for idx, item in enumerate(self.currentItems): #check if item is removed.
                if item not in sel:
                    self.currentItems.pop(idx)
                    break
            for idx, item in enumerate(sel): #check if item is added
                if item not in self.currentItems:
                    self.currentItems.append(item)
                    break
            lines = [f"{i+1}: {text}" for i, text in enumerate(self.currentItems)]
            display = "\n".join(lines)
        else:
            # the old single-line, comma-separated format
            display = ", ".join(sel)
        self.line_edit.setText(display)

if __name__ == '__main__':
    key = os.environ.get(dynamoAPIenvName)
    ok = key
    app = QApplication(sys.argv)
    timer = QTimer()
    timer.timeout.connect(poll_queue)
    timer.start(500)
    w = returnsApp(start_index=0 if not ok else 1)
    if ok: w.api_key = key
    w.show()
    if ok:
        w.init_data_processing()
    else:
        w.stack.setCurrentIndex(0)
    sys.exit(app.exec_())
