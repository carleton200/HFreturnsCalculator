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
from openpyxl.styles import PatternFill, Alignment, Font
from openpyxl.utils import get_column_letter
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import queue
import threading
from dateutil.relativedelta import relativedelta
from multiprocessing import Pool, freeze_support, Manager
from PyQt5.QtWidgets import (
    QApplication, QWidget, QStackedWidget, QVBoxLayout,
    QLabel, QLineEdit, QPushButton, QFormLayout,
    QRadioButton, QButtonGroup, QComboBox, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QProgressBar, QTableView, QCheckBox, QMessageBox,
    QScrollArea, QFileDialog, QListWidgetItem, QListWidget, QDialog, QSizePolicy, QGridLayout,
    QFrame, QTextEdit, QHeaderView
)
from PyQt5.QtGui import QBrush, QColor, QDesktopServices
from PyQt5.QtCore import Qt, QTimer, QAbstractTableModel, QModelIndex, pyqtSignal, QPoint, QUrl

currentVersion = "1.0.2"
testDataMode = False
onlyCalcTestMode = True
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
                "Classification" : {"local" : "Classification" , "dynLow" : "Target nameExposureHFClassificationLevel2"}
            }

commitmentChangeTransactionTypes = ["Commitment", "Transfer of commitment", "Transfer of commitment (out)", "Secondary - Original commitment (by secondary seller)"]
headerOptions = ["Return","NAV", "Monthly Gain", "Ownership" , "MDdenominator", "Commitment", "Unfunded"]
dataOptions = ["Investor","Family Branch","Classification", "dateTime"]
yearOptions = (1,2,3,5,7,10,12,15,20)

timeOptions = ["MTD","QTD","YTD", "Ownership", "Return", "ITD"] + [f"{y}YR" for y in yearOptions]
percent_headers = {option for option in timeOptions}

calculationPingTime = 8

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
        self.cFundsCalculated = False

        self.timer = QTimer()
        self.timer.timeout.connect(self.update_from_queue)
        self.queue = []

        # main stack
        self.main_layout = QVBoxLayout()
        appStyle = ("""
                        QWidget#borderFrame {
                            border: 2px solid #3E85E9;
                            border-radius: 4px;
                            padding: 4px;
                        }
                        QWidget#titleBox {
                            border: 4px solid #0665EA;
                            border-radius: 5px;
                            padding: 4px;
                        }
                        QWidget#mainPage, QMessageBox, QDialog {
                            background-color: #383838
                        }
                        QPushButton {
                            background-color: #3E85E9;
                            border: 2px solid transparent;
                            border-radius: 12px;
                            padding: 4px
                        }
                        QPushButton:hover {
                                background-color: #1771EE;
                        }
                        QPushButton#exportBtn {
                            background-color: #51AE2B;
                        }
                        QPushButton#exportBtn:hover {
                            background-color: #429321;
                        }
                        QPushButton#cancelBtn {
                            background-color: #D63131;
                        }
                        QLabel, QRadioButton, QCheckBox, QProgressBar {
                            color: white
                        }
                        QTableWidget, QWidget#subPanel, QHeaderView::corner, QTableCornerButton::section {
                        background-color : #514F4F
                        }
                        QHeaderView::section {
                            background-color: #A8A2A2;
                        }
                        QListWidget {
                            background-color : #514F4F;
                            color: white
                        }
                        QLineEdit{
                            border: 2px solid transparent;
                            border-radius: 12px;
                            background-color: #514F4F;
                            color : white;
                        }
                        QComboBox {
                            background-color: #514F4F;
                            color : white;
                        }
                    """)
        self.setStyleSheet(appStyle)
        self.setObjectName("mainPage")
        self.checkVersion()
        self.stack = QStackedWidget()
        self.init_global_widgets()

        self.init_api_key_page() #0
        self.init_returns_page() #1
        self.init_calculation_page() #2

        self.stack.setCurrentIndex(start_index)
        self.main_layout.addWidget(self.stack)
        self.setLayout(self.main_layout)
    def init_global_widgets(self):
        headerBox = QWidget()
        headerLayout = QHBoxLayout()
        self.lastImportLabel = QLabel("Last Data Import: ")
        headerLayout.addWidget(self.lastImportLabel)
        headerLayout.addStretch()
        headerLayout.addWidget(QLabel(f"Version: {currentVersion}"))
        headerBox.setLayout(headerLayout)
        self.main_layout.addWidget(headerBox)
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
        self.cancelCalcBtn.setObjectName("cancelBtn")
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
        self.exportBtn.setObjectName("exportBtn")
        controlsLayout.addWidget(self.exportBtn)
        controlsBox.setLayout(controlsLayout)
        layout.addWidget(controlsBox)

        mainButtonBox = QWidget()
        mainButtonLayout = QVBoxLayout()
        optionsBox = QWidget()
        optionsBox.setObjectName("borderFrame")
        optionsLayout = QHBoxLayout()
        optionsTitle = QLabel("Options")
        optionsTitle.setObjectName("titleBox")
        optionsLayout.addWidget(optionsTitle)
        self.tableBtnGroup = QButtonGroup()
        self.complexTableBtn = QRadioButton("Complex Table")
        self.monthlyTableBtn = QRadioButton("Monthly Table")
        buttonBox = QWidget()
        buttonLayout = QVBoxLayout()
        for rb in (self.complexTableBtn,self.monthlyTableBtn):
            self.tableBtnGroup.addButton(rb)
            #rb.toggled.connect(self.updateTableType)
            buttonLayout.addWidget(rb)
        self.returnOutputType = QComboBox()
        self.returnOutputType.addItems(headerOptions)
        self.returnOutputType.currentTextChanged.connect(self.buildReturnTable)
        self.dataTypeBox = QWidget()
        dataTypeLayout = QHBoxLayout()
        dataTypeLayout.addWidget(QLabel("Data type:"))
        dataTypeLayout.addWidget(self.returnOutputType)
        self.dataTypeBox.setLayout(dataTypeLayout)
        buttonLayout.addWidget(self.dataTypeBox)
        buttonBox.setLayout(buttonLayout)
        optionsLayout.addWidget(buttonBox)
        self.tableBtnGroup.buttonClicked.connect(self.buildReturnTable)
        self.complexTableBtn.setChecked(True)
        
        self.dataStartSelect = QComboBox()
        self.dataEndSelect = QComboBox()
        dateBox = QWidget()
        dateLayout = QVBoxLayout()
        for text, CB in (["Start: ", self.dataStartSelect], ["End: ", self.dataEndSelect]):
            lineBox = QWidget()
            lineLay = QHBoxLayout()
            lineLay.addWidget(QLabel(text))
            lineLay.addWidget(CB)
            lineBox.setLayout(lineLay)
            dateLayout.addWidget(lineBox)
        dateBox.setLayout(dateLayout)
        optionsLayout.addWidget(dateBox)
        benchBox = QWidget()
        benchLayout = QVBoxLayout()
        benchLayout.addWidget(QLabel("Benchmarks:"))
        self.benchmarkSelection = MultiSelectBox()
        self.benchmarkSelection.popup.closed.connect(self.buildReturnTable)
        benchLayout.addWidget(self.benchmarkSelection)
        benchBox.setLayout(benchLayout)
        optionsLayout.addWidget(benchBox)
        sortBox = QWidget()
        sortLayout = QVBoxLayout()
        sortLayout.addWidget(QLabel("Group by: "))
        self.sortHierarchy = MultiSelectBox()
        self.sortHierarchy.hierarchyMode()
        self.sortHierarchy.popup.closed.connect(self.buildReturnTable)
        sortLayout.addWidget(self.sortHierarchy)
        sortBox.setLayout(sortLayout)
        optionsLayout.addWidget(sortBox)
        self.consolidateFundsBtn = QRadioButton("Consolidate Funds")
        self.consolidateFundsBtn.setChecked(True)
        self.consolidateFundsBtn.clicked.connect(self.buildReturnTable)
        optionsLayout.addWidget(self.consolidateFundsBtn)
        self.headerSort = SortButtonWidget()
        self.headerSort.popup.popup_closed.connect(self.headerSortClosed)
        optionsLayout.addWidget(self.headerSort)
        optionsBox.setLayout(optionsLayout)
        mainButtonLayout.addWidget(optionsBox)
        mainButtonBox.setLayout(mainButtonLayout)
        layout.addWidget(mainButtonBox)

        mainFilterBox = QWidget()
        mainFilterBox.setObjectName("borderFrame")
        mainFilterLayout = QGridLayout()
        filterTitle = QLabel("Filters")
        filterTitle.setObjectName("titleBox")
        mainFilterLayout.addWidget(filterTitle,0,0,2,1)

        self.filterOptions = [
                            {"key": "Classification", "name": "HF Classification", "dataType" : None, "dynNameLow" : "Target nameExposureHFClassificationLevel2"},
                            {"key" : nameHier["Family Branch"]["local"], "name" : nameHier["Family Branch"]["local"], "dataType" : None, "dynNameLow" : None, "dynNameHigh" : nameHier["Family Branch"]["dynHigh"]},
                            {"key": "Investor",       "name": "Investor", "dataType" : "Investor", "dynNameLow" : None, "dynNameHigh" : "Source name"},
                            {"key": "assetClass",     "name": "Asset Level 1", "dataType" : "Total Asset", "dynNameLow" : "ExposureAssetClass", "dynNameHigh" : "ExposureAssetClass"},
                            {"key": "subAssetClass",  "name": "Asset Level 2", "dataType" : "Total subAsset", "dynNameLow" : "ExposureAssetClassSub-assetClass(E)", "dynNameHigh" : "ExposureAssetClassSub-assetClass(E)"},
                            {"key" : nameHier["sleeve"]["local"], "name" : "Asset Level 3", "dataType" : "Total sleeve", "dynNameLow" : nameHier["sleeve"]["local"]},
                            {"key": "Pool",           "name": "Pool", "dataType" : "Total Pool" , "dynNameLow" : "Source name", "dynNameHigh" : "Target name"},
                            {"key": "Fund",           "name": "Fund/Investment", "dataType" : "Total Fund" , "dynNameLow" : "Target name"},
                            
                        ]
        self.filterBtnExclusions = ["Investor","Classification", nameHier["Family Branch"]["local"]]
        self.highOnlyFilters = ["Investor", nameHier["Family Branch"]["local"]]
        self.filterDict = {}
        self.filterRadioBtnDict = {}
        self.filterBtnGroup = QButtonGroup()
        self.filterBtnGroup.setExclusive(False)
        for col, filter in enumerate(self.filterOptions, start=1):
            if filter["key"] not in self.filterBtnExclusions:
                #investor level is not filterable. It is total portfolio or shows the investors data
                self.filterRadioBtnDict[filter["key"]] = QCheckBox(f"{filter["name"]}:")
                self.filterRadioBtnDict[filter["key"]].setChecked(True)
                self.filterBtnGroup.addButton(self.filterRadioBtnDict[filter["key"]])
                mainFilterLayout.addWidget(self.filterRadioBtnDict[filter["key"]],0, col)
            else:
                mainFilterLayout.addWidget(QLabel(f"{filter["name"]}:"), 0, col)
            if filter["key"] != "Fund":
                self.sortHierarchy.addItem(filter["key"])
            self.filterDict[filter["key"]] = MultiSelectBox()
            self.filterDict[filter["key"]].popup.closed.connect(lambda: self.filterUpdate())
            
            mainFilterLayout.addWidget(self.filterDict[filter["key"]],1,col)
        self.filterBtnGroup.buttonToggled.connect(self.filterBtnUpdate)
        mainFilterBox.setLayout(mainFilterLayout)
        layout.addWidget(mainFilterBox)
        t1 = QVBoxLayout() #build table loading bar
        self.buildTableLoadingBox = QWidget()
        t1.addWidget(QLabel("Building returns table..."))
        self.buildTableLoadingBar = QProgressBar()
        self.buildTableLoadingBar.setRange(0,8)
        t1.addWidget(self.buildTableLoadingBar)
        self.buildTableLoadingBox.setLayout(t1)
        self.buildTableLoadingBox.setVisible(False)
        layout.addWidget(self.buildTableLoadingBox)
        self.returnsTable = SmartStretchTable() #table
        self.returnsTable.setSelectionMode(QTableWidget.ContiguousSelection)  # Required
        self.returnsTable.setSelectionBehavior(QTableWidget.SelectItems)
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
        lastImportDB = load_from_db("history") if len(load_from_db("history")) == 1 else None
        if not testDataMode and lastImportDB is None:
            print("No previous import found")
            #pull data is there is no data pulled yet
            executor.submit(lambda: self.pullData())
        elif not testDataMode:
            lastImportString = lastImportDB[0]["lastImport"]
            lastImport = datetime.strptime(lastImportString, "%B %d, %Y @ %I:%M %p")  
            self.lastImportLabel.setText(f"Last Data Import: {lastImportString}")
            now = datetime.now()
            if lastImport.month != now.month or now > (lastImport + relativedelta(hours=2)):
                print(f"Reimporting due to two hour data gap. \n     Last import: {lastImport}\n    Current time: {now}")
                #pull data if in a new month or 1 days have elapsed
                executor.submit(self.pullData)
            elif lastImportDB[0]["lastImport"] != lastImportDB[0].get("lastCalculation", "None"):
                self.earliestChangeDate = datetime.strptime(lastImportDB[0].get("changeDate"), "%B %d, %Y @ %I:%M %p")
                self.processFunds()
                self.calculateReturn()
            else:
                calculations = load_from_db("calculations")
                self.processFunds()
                if calculations != []:
                    self.populate(self.calculationTable,calculations)
                    self.buildReturnTable()
                else:
                    self.calculateReturn()
        else:
            calculations = load_from_db("calculations")
            if calculations != []:
                self.populate(self.calculationTable,calculations)
                self.buildReturnTable()
            else:
                executor.submit(self.calculateReturn)
    def cancelCalc(self):
        if self.pool:
            self.pool.terminate()
        if self.timer:
            self.timer.stop()
        self.cancel = True
    def viewUnderlyingData(self):
        row = self.returnsTable.currentRow()
        col = self.returnsTable.currentColumn()
        key = list(self.filteredReturnsTableData.keys())[row]
        vh_item = self.returnsTable.verticalHeaderItem(row)
        row = vh_item.text() if vh_item else f"Row {row}"

        # Get the horizontal (column) header text
        hh_item = self.returnsTable.horizontalHeaderItem(col)
        col = hh_item.text() if hh_item else f"Column {col}"
        self.selectedCell = {"entity": row, "month" : col, "rowKey" : key, "dataType" : self.filteredReturnsTableData[key]["dataType"] }
        try:
            window = underlyingDataWindow(parentSource=self)
            self.udWindow = window
            if window.success:
                window.show()
        except Exception as e:
            print(f"Error in data viewing window: {e} {traceback.format_exc()}")
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
            try:
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

                rowStart = 4
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

                appliedFilters = {}
                for filter in self.filterOptions:
                    if self.filterDict[filter["key"]].checkedItems() != []:
                        appliedFilters[filter["key"]] = self.filterDict[filter["key"]].checkedItems()
                filterStart = num_hier
                if self.filterDict[filter["key"]].checkedItems() != []: #only write if there are filters applied
                    cell= ws.cell(row=1, column=filterStart, value="Filter:")
                    cell.font = Font(bold=True)
                    cell = ws.cell(row=2, column=filterStart, value="Selections:")
                    cell.font = Font(bold=True)
                    for idx, key in enumerate(appliedFilters, start=filterStart + 1):
                        cell = ws.cell(row=1, column=idx, value=key)
                        cell.alignment = Alignment(wrap_text=True)
                        cell = ws.cell(row=2, column=idx, value=", ".join(appliedFilters[key]))
                        cell.alignment = Alignment(wrap_text=True)

            
                wb.save(path)
            except Exception as e:
                gui_queue.put(lambda error=e, trace = traceback.format_exc(): QMessageBox.critical(self, "Save error", trace))
            else:
                gui_queue.put(lambda: QMessageBox.information(self, "Saved", f"Excel saved to:\n{path}"))
                gui_queue.put(lambda: QDesktopServices.openUrl(QUrl.fromLocalFile(path)))
        executor.submit(processExport)
    def processFunds(self):
        self.cFundsCalculated = True
        self.sleeveFundLinks = {}
        self.cFundToFundLinks = {}
        self.pools = []
        funds = load_from_db("funds")
        if funds != []:
            consolidatorFunds = {}
            for row in funds: #find sleeve values and consolidated funds
                assetClass = row["assetClass"]
                subAssetClass = row["subAssetClass"]
                sleeve = row["sleeve"]
                if row.get("Fundpipelinestatus") is not None and "Z - Placeholder" in row.get("Fundpipelinestatus"):
                    consolidatorFunds[row["Name"]] = {"cFund" : row["Name"], "assetClass" : assetClass, "subAssetClass" : subAssetClass, "sleeve" : sleeve}
                    self.cFundToFundLinks[row["Name"]] = []
                if row["sleeve"] not in self.sleeveFundLinks:
                    self.sleeveFundLinks[row["sleeve"]] = [row["Name"]]
                else:
                    self.sleeveFundLinks[row["sleeve"]].append(row["Name"])
                if row["Fundpipelinestatus"] == "I - Internal":
                    self.pools.append({"poolName" : row["Name"], "assetClass" : assetClass, "subAssetClass" : subAssetClass})
            self.consolidatedFunds = {}
            for row in funds: #assign funds to their consolidators
                if row.get("Parentfund") in consolidatorFunds:
                    self.consolidatedFunds[row["Name"]] = consolidatorFunds.get(row.get("Parentfund"))
                    self.cFundToFundLinks[row.get("Parentfund")].append(row["Name"])
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
                        self.filterDict[filter["key"]].setEnabled(False)
                    else:
                        self.filterDict[filter["key"]].setEnabled(True)
            self.filterCallLock = False
            if reloadRequired or self.currentTableData is None:
                self.buildReturnTable()
            else:
                self.populateReturnsTable(self.currentTableData)
    def resetData(self):
        save_to_db("calculations",None,action="reset") #reset calculations so new data will be freshly calculated
        if testDataMode or onlyCalcTestMode:
            if onlyCalcTestMode:
                self.earliestChangeDate = self.dataTimeStart
            self.calculateReturn()
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
        if not self.cFundsCalculated:
            self.processFunds()
        def buildTable(cancelEvent):
            try:
                print("Building return table...")
                self.currentTableData = None #resets so a failed build won't be used
                
                if self.tableBtnGroup.checkedButton().text() == "Complex Table":
                    gui_queue.put(lambda: self.returnOutputType.setCurrentText("Return"))
                    gui_queue.put(lambda: self.dataTypeBox.setVisible(False))
                else:
                    gui_queue.put(lambda: self.dataTypeBox.setVisible(True))
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
                data = load_from_db("calculations",condStatement, tuple(parameters))
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
        benchmarks = load_from_db("benchmarks",f"WHERE [Index] IN ({placeholders})",tuple(benchmarkChoices))
        for idx, bench in enumerate(benchmarks):
            name = bench["Index"] + code
            if (datetime.strptime(bench["Asofdate"], "%Y-%m-%dT%H:%M:%S") < datetime.strptime(self.dataStartSelect.currentText(), "%B %Y") or
                datetime.strptime(bench["Asofdate"], "%Y-%m-%dT%H:%M:%S") > datetime.strptime(self.dataEndSelect.currentText(), "%B %Y") + relativedelta(months=1) ) :
                continue #skip if outside selected range
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
                        highEntries[month]["Return"] = highEntries[month]["Monthly Gain"] / highEntries[month]["MDdenominator"] * 100 if highEntries[month]["MDdenominator"] != 0 else 0
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
                        totalEntriesLow[month]["Return"] = totalEntriesLow[month]["Monthly Gain"] / totalEntriesLow[month]["MDdenominator"] * 100 if totalEntriesLow[month]["MDdenominator"] != 0 else 0
                        newEntriesLow.append(totalEntriesLow[month])
                totalDataLow.extend(newEntriesLow)
                return struc, newEntriesLow, totalDataLow

        sortHierarchy = self.sortHierarchy.checkedItems()
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
            trueTotalEntries[month]["Return"] = trueTotalEntries[month]["Monthly Gain"] / trueTotalEntries[month]["MDdenominator"] * 100 if trueTotalEntries[month]["MDdenominator"] != 0 else 0
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
                            lowAccounts = load_from_db("positions_low", condStatement,tuple(parameters))
                            
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
        save_to_db("Months",dbDates)

    def pullInvestorNames(self):
        accountsHigh = load_from_db('positions_high')
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
        fundPoolLink = {}
        for filter in self.filterOptions:
            if filter["key"] not in self.highOnlyFilters:
                allOptions[filter["key"]] = []
        accountsHigh = load_from_db("positions_high")
        if accountsHigh is not None:
            for account in accountsHigh:
                for filter in self.filterOptions:
                    if (filter["key"] in allOptions and "dynNameHigh" in filter.keys() and
                        account[filter["dynNameHigh"]] is not None and
                        account[filter["dynNameHigh"]] not in allOptions[filter["key"]]):
                        allOptions[filter["key"]].append(account[filter["dynNameHigh"]])
        else:
            print("no investor to pool accounts found")
        accountsLow = load_from_db("positions_low")
        if accountsLow is not None:
            for lowAccount in accountsLow:
                for filter in self.filterOptions:
                    if (filter["key"] in allOptions and "dynNameLow" in filter.keys() and
                        lowAccount[filter["dynNameLow"]] is not None and
                        lowAccount[filter["dynNameLow"]] not in allOptions[filter["key"]]):
                        allOptions[filter["key"]].append(lowAccount[filter["dynNameLow"]])
                fundPoolLink[lowAccount["Target name"]] = lowAccount["Source name"]
        else:
            print("no pool to fund accounts found")
        self.fullLevelOptions = {}
        for filter in self.filterOptions:
            if filter["key"] in allOptions:
                allOptions[filter["key"]].sort()
                self.filterDict[filter["key"]].addItems(allOptions[filter["key"]])
                self.fullLevelOptions[filter["key"]] = allOptions[filter["key"]]
        self.filterDict["Classification"].setCheckedItem("HFC")
        self.fundPoolLinks = fundPoolLink
        self.pullInvestorNames()
        self.pullBenchmarks()

    def pullBenchmarks(self):
        benchmarks = load_from_db("benchmarks")
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
                previous = load_from_db(table) or []

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
                if diffCount > 0 and not demoMode:
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
                "tranCols": "Investment in, Investing Entity, Transaction Type, Effective date, Asset Class (E), Sub-asset class (E), HF Classification, Remaining commitment change, Trade date/time, Amount in system currency, Cash flow change (USD), Parent investor",
                "tranName": "InvestmentTransaction",
                "tranSort": "Effective date:desc",
                "accountCols": "As of Date, Balance Type, Asset Class, Sub-asset class, Value of Investments, Investing entity, Investment in, HF Classification, Parent investor, Value in system currency",
                "accountName": "InvestmentPosition",
                "accountSort": "As of Date:desc",
                "fundCols" : "Fund Name, Asset class category, Parent fund, Fund Pipeline Status",
                "benchCols" : (f"Index, As of date, MTD %, QTD %, YTD %, ITD cumulative %, ITD TWRR %, "
                               f"{', '.join(f'Last {y} yr %' for y in yearOptions)}"), 
            }
            calculationsTest = load_from_db("calculations")
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
                        if j == 0: #fund level
                            payload = {
                                        "advf": {
                                            "e": [
                                                {
                                                    "_name": "InvestmentTransaction",
                                                    "e": [
                                                        {
                                                            "_name": "InvestorAccount",
                                                            "_not": True
                                                        },
                                                        {
                                                            "_name": "Fund",
                                                            "rule": [
                                                                {
                                                                    "_op": "is",
                                                                    "_prop": "Fund Pipeline Status",
                                                                    "values": [
                                                                        {
                                                                            "id": "d33af081-c4c8-431b-a98b-de9eaf576324",
                                                                            "es": "L_FundPipelineStatus",
                                                                            "name": "I - Internal"
                                                                        }
                                                                    ]
                                                                }
                                                            ]
                                                        }
                                                    ],
                                                    "rule": [
                                                        {
                                                            "_op": "not_null",
                                                            "_prop": "Cash flow change (USD)"
                                                        },
                                                        {
                                                            "_op": "not_null",
                                                            "_prop": "Investing entity"
                                                        }
                                                    ]
                                                },
                                                {
                                                    "_name": "InvestmentTransaction",
                                                    "e": [
                                                        {
                                                            "_name": "InvestorAccount",
                                                            "_not": True
                                                        },
                                                        {
                                                            "_name": "Fund",
                                                            "rule": [
                                                                {
                                                                    "_op": "is",
                                                                    "_prop": "Fund Pipeline Status",
                                                                    "values": [
                                                                        {
                                                                            "id": "d33af081-c4c8-431b-a98b-de9eaf576324",
                                                                            "es": "L_FundPipelineStatus",
                                                                            "name": "I - Internal"
                                                                        }
                                                                    ]
                                                                }
                                                            ]
                                                        }
                                                    ],
                                                    "rule": [
                                                        {
                                                            "_op": "not_null",
                                                            "_prop": "Investing entity"
                                                        },
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
                                                        }
                                                    ]
                                                }
                                            ]
                                        },
                                        "mode": "compact"
                                    }
                        else: #investor level
                            payload = {
                                        "advf": {
                                            "e": [
                                                {
                                                    "_name": "InvestmentTransaction",
                                                    "e": [
                                                        {
                                                            "_name": "InvestorAccount"
                                                        }
                                                    ],
                                                    "rule": [
                                                        {
                                                            "_op": "not_null",
                                                            "_prop": "Cash flow change (USD)"
                                                        },
                                                        {
                                                            "_op": "not_null",
                                                            "_prop": "Investing entity"
                                                        }
                                                    ]
                                                }
                                            ]
                                        },
                                        "mode": "compact"
                                    }
                        
                    else: #account (position)
                        if j == 0: #fund level
                            payload = {
                                    "advf": {
                                        "e": [
                                            {
                                                "_name": "InvestmentPosition",
                                                "e": [
                                                    {
                                                        "_name": "Fund",
                                                        "rule": [
                                                            {
                                                                "_op": "is",
                                                                "_prop": "Fund Pipeline Status",
                                                                "values": [
                                                                    {
                                                                        "id": "d33af081-c4c8-431b-a98b-de9eaf576324",
                                                                        "es": "L_FundPipelineStatus",
                                                                        "name": "I - Internal"
                                                                    }
                                                                ]
                                                            }
                                                        ]
                                                    },
                                                    {
                                                        "_name": "InvestorAccount",
                                                        "_not": True
                                                    }
                                                ],
                                                "rule": [
                                                    {
                                                        "_op": "not_null",
                                                        "_prop": "Investing entity"
                                                    }
                                                ]
                                            }
                                        ]
                                    },
                                    "mode": "compact"
                                }
                        else: #investor level
                            payload = {
                                            "advf": {
                                                "e": [
                                                    {
                                                        "_name": "InvestmentPosition",
                                                        "e": [
                                                            {
                                                                "_name": "InvestorAccount"
                                                            }
                                                        ],
                                                        "rule": [
                                                            {
                                                                "_op": "not_null",
                                                                "_prop": "Investing entity"
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
                            save_to_db('positions_low', rows)
                        else:
                            #positions_high is not checked for new data, as the calculations overwrite Dynamo's calculations
                            save_to_db('positions_high', rows)
                    else:
                        if j == 0:
                            if skipCalculations:
                                checkNewestData('transactions_low',rows)
                            save_to_db('transactions_low', rows)
                        else:
                            if skipCalculations:
                                checkNewestData('transactions_high',rows)
                            save_to_db('transactions_high', rows)
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
                        save_to_db("funds",rows)
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
                    save_to_db("benchmarks",rows)
                except Exception as e:
                    print(f"Error proccessing benchmark API data : {e} {e.args}.  {traceback.format_exc()}")
                
            else:
                print(f"Error in API call for benchmarks. Code: {response.status_code}. {response}. {traceback.format_exc()}")
            if skipCalculations:
                print("Earliest change: ", self.earliestChangeDate)
            gui_queue.put(lambda: self.apiLoadingBar.setValue(100))
            
            while not gui_queue.empty(): #wait to assure database has been updated in main thread before continuing
                time.sleep(0.2)
            


            currentTime = datetime.now().strftime("%B %d, %Y @ %I:%M %p")
            changeData = datetime.strftime(self.earliestChangeDate, "%B %d, %Y @ %I:%M %p")
            save_to_db(None,None,query="UPDATE history SET [lastImport] = ?, [changeDate] = ?", inputs=(currentTime,changeData), action="replace")
            self.lastImportLabel.setText(f"Last Data Import: {currentTime}")
            gui_queue.put(lambda: self.apiLoadingBarBox.setVisible(False))
            gui_queue.put(lambda: self.calculateReturn())
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
            print("Calculating return....")
            fundListDB = load_from_db("funds")
            fundList = {}
            for fund in fundListDB:
                fundList[fund["Name"]] = fund[nameHier["sleeve"]["sleeve"]]
            months = load_from_db("Months", f"ORDER BY [dateTime] ASC")
            calculations = []
            monthIdx = 0
            if load_from_db("calculations") == []:
                noCalculations = True
            else:
                noCalculations = False

            if self.earliestChangeDate > datetime.now() and not noCalculations:
                #if no new data exists, use old calculations
                calculations = load_from_db("calculations")
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
            
            # proces pool section----------------------------------------------------------------
            save_to_db("progress",None,action="reset")
            self.init_db()

            # ------------------- build data cache ----------------------
            tables = ["positions_low", "transactions_low", "positions_high", "transactions_high", "calculations"]
            table_rows = {t: load_from_db(t) for t in tables}
            cache = {}
            for table, rows in table_rows.items():
                for row in rows:
                    if table in ("positions_low", "transactions_low"):
                        poolKey = row.get("Source name")
                    elif table in ("positions_high", "transactions_high"):
                        poolKey = row.get("Target name")
                    else:
                        poolKey = row.get("Pool")
                    if poolKey is None:
                        continue
                    for m in months:
                        if table == "calculations":
                            if row.get("dateTime") != m["dateTime"]:
                                continue
                        else:
                            start = m["accountStart"] if table in ("positions_low", "positions_high") else m["tranStart"]
                            date = row.get("Date")
                            if not (start <= date <= m["endDay"]):
                                continue
                        cache.setdefault(poolKey, {}).setdefault(table, {}).setdefault(m["dateTime"], []).append(row)

            self.manager = Manager()
            self.lock = self.manager.Lock()

            self.pool = Pool()
            self.futures = []
            executor.submit(self.watch_db)

            commonData = {"earliestChangeDate" : self.earliestChangeDate, "noCalculations" : noCalculations,
                            "months" : months, "fundList" : fundList
                            }
            
            self.calcStartTime = datetime.now()
            for pool in self.pools:
                pool["cache"] = cache.get(pool.get("poolName"))
                res = self.pool.apply_async(processPool, args=(pool, commonData, self.lock))
                self.futures.append(res)
            self.pool.close()

            self.timer.start(int(calculationPingTime * 0.75) * 1000) #check at 0.75 the ping time to prevent queue buildup
            #-----------------^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^-----------------------------------
        except Exception as e:
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
            gui_queue.put(lambda: self.importButton.setEnabled(True))
            print(f"Error occured running calculations: {e}")
            print("e.args:", e.args)
            # maybe also:
            print(traceback.format_exc())
    def init_db(self):
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS progress (
                pool STRING PRIMARY KEY,
                completed INTEGER NOT NULL,
                total INTEGER NOT NULL,
                status STRING NOT NULL
            )
        """)
        conn.commit()
        conn.close()
    def watch_db(self):
        while True:
            try:
                self.lock.acquire()
                conn = sqlite3.connect(DATABASE_PATH)
                c = conn.cursor()
                c.execute("SELECT * FROM progress")
                cols = [d[0] for d in c.description]
                statusLines = [dict(zip(cols, row)) for row in c.fetchall()]
                failed = []
                working = []
                for line in statusLines:
                    if line["status"] == "Failed":
                        failed.append(line)
                    elif line["status"] == "Working":
                        working.append(line)
                if len(failed) > 0:
                    print(f"Halting progress watch due to worker '{failed[0].get("pool","Bad Pull")}' failure.")
                    self.queue.append(-86) #will halt the queue
                    return
                elif len(working) == 0 and len(statusLines) == len(self.pools):
                    print("All workers have declared complete.")
                    self.queue.append(100) #backup in case the numbers below fail
                c.execute("SELECT SUM(completed), SUM(total) FROM progress")
                result = c.fetchone()
                conn.close()
                if result and result[1]:
                    complete, total = result
                    percent = int((complete / total) * 100)
                    self.queue.append(percent)
                    if complete >= total:
                        return
                self.lock.release()
            except Exception as e:
                self.lock.release()
                print(f"Error watching database: {e}")
                print(traceback.format_exc())
                pass
            if self.cancel:
                break
            time.sleep(calculationPingTime)
    def update_from_queue(self):
        if self.queue:
            val = self.queue.pop(0)
            self.calculationLoadingBar.setValue(val)
            timeElapsed = datetime.now() - self.calcStartTime
            secsElapsed = timeElapsed.total_seconds()
            loadingFraction = float(val) / 100 #decimal format percentage
            if loadingFraction > 0:
                est_total_secs = secsElapsed / loadingFraction
                secs_remaining = est_total_secs - secsElapsed
            else:
                secs_remaining = 0
            mins, secs = divmod(int(secs_remaining), 60)
            time_str = f"{mins}m {secs}s" # format as “Xm Ys” or “MM:SS”
            self.calculationLabel.setText(f"Estimated time remaining: {time_str}")
            if val >= 100:
                self.timer.stop()
                self.calcCompletion()
            elif val == -86:
                self.timer.stop()
                QMessageBox.warning(self,"Calculation Failure", "A worker thread has failed. Calculations will not be properly completed.")
                self.pool.terminate()
                self.pool.join()
                
    def calcCompletion(self):
        try:
            print("Checking worker completion...")
            self.pool.join()
            print("All workers finished")
            calculations = []
            for fut in self.futures:
                try:
                    calculations.extend(fut.get())
                except Exception as e:
                    print(f"Error appending calculations: {e}")
            keys = []
            for row in calculations:
                for key in row.keys():
                    if key not in keys:
                        keys.append(key)
            save_to_db("calculations",calculations, keys=keys)
            try:
                apiPullTime = load_from_db("history")[0]["lastImport"]
                save_to_db(None,None,query="UPDATE history SET [lastCalculation] = ?", inputs=(apiPullTime,), action="replace")
            except:
                print("failed to update last calculation time")
            gui_queue.put( lambda: self.populate(self.calculationTable,calculations,keys = keys))
            gui_queue.put( lambda: self.buildReturnTable())
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
            if not testDataMode:
                gui_queue.put(lambda: self.importButton.setEnabled(True))
            print("Calculations complete.")
            save_to_db("progress",None,action="reset")
        except:
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
            gui_queue.put(lambda: self.importButton.setEnabled(True))
            print(f"Error occured processing calculation results. Resetting... ")
            print(traceback.format_exc())
    def checkVersion(self):
        self.currentVersionAccess = False
        self.globalVersion = None
        try:
            row = load_from_db("history")[0]
            self.globalVersion = row["currentVersion"]
            if row["currentVersion"] == currentVersion:
                self.currentVersionAccess = True
            else:
                QMessageBox.warning(self,"Outdated Version", f"Your current version ({currentVersion}) is outdated. The current version is {row["currentVersion"]}. \n" + 
                                    " Please request the newer version. \n Your version will run, but cannot access the shared data. This will be signifigantly slower and may have bugs/errors.")
        except:
            QMessageBox.warning(self,"Error checking version", f"An error occured checking that your version is up to date. This session has been granted limited access. \n "+
                                "Your session will run, but cannot access the shared data. This will be signifigantly slower and may have bugs/errors. \n \n Try restarting the app." +
                                " If this error persists, contact an admin.")
    def separateRowCode(self, label):
        header = re.sub(r'##\(.*\)##', '', label, flags=re.DOTALL)
        code = re.findall(r'##\(.*\)##', label, flags=re.DOTALL)[0]
        return header, code
    def headerSortClosed(self):
        self.populateReturnsTable(self.currentTableData)
    def orderColumns(self,keys):
        mode = self.tableBtnGroup.checkedButton().text()
        if mode == "Monthly Table":
            dates = [datetime.strptime(k, "%B %Y") for k in keys]
            dates = sorted(dates, reverse=True)
            keys = [d.strftime("%B %Y") for d in dates]
        elif mode == "Complex Table":
            newOrder = ["NAV", "Commitment", "Unfunded","MTD","QTD","YTD"] + [f"{y}YR" for y in yearOptions] + ["ITD"]
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

        rows = copy.deepcopy(origRows) #prevents alteration of self.returnsTableData
        for f in self.filterOptions:
            if f["key"] not in self.filterBtnExclusions and not self.filterRadioBtnDict[f["key"]].isChecked():
                to_delete = [k for k,v in rows.items() if v["dataType"] == "Total " + f["key"]]
                for k in to_delete:
                    rows.pop(k)
        
        self.filteredReturnsTableData = copy.deepcopy(rows) #prevents removal of dataType key for data lookup

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

        if not self.headerSort.active or mode == "Monthly Table":
            col_keys = set()
            for d in cleaned.values():
                col_keys |= set(d.keys())
            col_keys = list(col_keys)

            col_keys = self.orderColumns(col_keys)
            if mode == "Complex Table":
                exceptions = ("Return", "Ownership", "MDdenominator", "Monthly Gain")
                self.headerSort.set_items(col_keys,[item for item in col_keys if item not in exceptions])
        else:
            col_keys = self.headerSort.popup.get_checked_sorted_items()

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
def save_to_db(table, rows, action = "", query = "",inputs = None, keys = None, connection = None, lock = None):
    try:
        if lock is not None:
            lock.acquire()
        if connection is None:
            conn = sqlite3.connect(DATABASE_PATH)
            cur = conn.cursor()
        else:
            conn = connection
            cur = connection.cursor()
        if action == "reset":
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        elif action == "add":
            try:
                for row in rows:
                    cols = list(row.keys())
                    quoted_cols = ','.join(f'"{c}"' for c in cols)
                    placeholders = ','.join('?' for _ in cols)
                    sql = f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
                    vals = tuple(str(row.get(c, '')) for c in cols)
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
        if connection is None:
            conn.close()
        else:
            cur.close()
        if lock is not None:
            lock.release()
    except:
        print("DB save failed. closing connections")
        try:
            if lock is not None:
                lock.release()
            cur.close()
        except:
            pass
def load_from_db(table, condStatement = "",parameters = None, cursor = None, lock = None):
    try:
        if lock is not None:
            lock.acquire()
        # Transactions
        if os.path.exists(DATABASE_PATH):
            if cursor is None:
                conn = sqlite3.connect(DATABASE_PATH)
                cur = conn.cursor()
            else:
                cur = cursor
            try:
                if condStatement != "" and parameters is not None:
                    cur.execute(f'SELECT * FROM {table} {condStatement}',parameters)
                elif condStatement != "" and parameters is None:
                    cur.execute(f'SELECT * FROM {table} {condStatement}')
                else:
                    cur.execute(f'SELECT * FROM {table}')
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                if cursor is None:
                    conn.close()
                if lock is not None:
                    lock.release()
                return rows
            except Exception as e:
                try:
                    if parameters is not None and table != "calculations":
                        print(f"Error loading from database: {e}, table: {table} condStatment: {condStatement}, parameters: {parameters}")
                    elif table != "calculations":
                        print(f"Error loading from database: {e}, table: {table} condStatment: {condStatement}")
                    else:
                        print(f"Info: {e}, {e.args}")
                    if cursor is None:
                        conn.close()
                except:
                    pass
                if lock is not None:
                    lock.release()
                return []
        else:
            if lock is not None:
                lock.release()
            return []
    except:
        print("DB load failed. closing connections")
        try:
            if lock is not None:
                lock.release()
            if cursor is None:
                cur.close()
        except:
            pass
def updateStatus(pool,totalLoops, lock, status = "Working", connection = None):
    failure = False
    try:
        
        lock.acquire()
        if connection is None:
            conn = sqlite3.connect(DATABASE_PATH)
        else:
            conn = connection
        c = conn.cursor()

        c.execute("SELECT status FROM progress WHERE status = ?",("Failed",))
        failed = c.fetchall()
        if len(failed) > 0:
            failure = True
        # Update or insert progress for this worker
        if status == "Working":
            c.execute("""
                INSERT INTO progress (pool, completed, total,status)
                VALUES (?, 0, ?,?)
                ON CONFLICT(pool) DO UPDATE SET completed = completed + 1
            """, (pool, totalLoops,status))
        elif status == "Completed":
            c.execute("UPDATE progress SET completed = completed + 1, status = ? WHERE pool = ?", (status,pool))
        else:
            c.execute("UPDATE progress SET status = ? WHERE pool = ?", (status,pool))

        conn.commit()
        if connection is None:
            conn.close()
    except Exception as e:
        print(f"Error updating status: {e}")
    finally:
        lock.release()
    return failure
def processPool(poolData : dict,selfData : dict, lock):
    try:
        earliestChangeDate = selfData.get("earliestChangeDate")
        noCalculations = selfData.get("noCalculations")
        months = selfData.get("months")
        fundList = selfData.get("fundList")
        calculations = []
        pool = poolData.get("poolName")
        cache = poolData.get("cache")
        calcConnection = sqlite3.connect(DATABASE_PATH)
        calcCursor = calcConnection.cursor()
        newMonths = []
        insert_low = []
        update_low = []
        insert_high = []
        update_high = []


        if not noCalculations: #if there are calculations, find all months before the data pull, and then pull those calculations
            for month in months:
                #if the calculations for the month have already been complete, pull the old data
                if earliestChangeDate > datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S"):
                    calculations.extend(cache.get("calculations", {}).get(month["dateTime"], []))
                else:
                    newMonths.append(month)
        else:
            newMonths = months
        for month in newMonths:
            failed = updateStatus(pool,len(newMonths),lock,connection=calcConnection) #iterate progress up once after each completed month
            if failed: #if other workers failed, halt the process
                print(f"Exiting worker {pool} due to other failure...")
                return []
            totalDays = int(datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S").day  - datetime.strptime(month["tranStart"], "%Y-%m-%dT%H:%M:%S").day) + 1
            poolFunds = cache.get("positions_low", {}).get(month["dateTime"], [])
            #find MD denominator for each investor
            #find total gain per pool
            funds = []
            fundNames = []
            startEntries = {}
            endEntries = {}
            for account in poolFunds:
                if account["Target name"] not in fundNames:
                    fundNames.append(account["Target name"])
                    funds.append({"fundName" : account["Target name"], "hidden" : False})
                if account["Date"] == month["accountStart"]:
                    if account["Target name"] not in startEntries:
                        startEntries[account["Target name"]] = [account,]
                    else:
                        startEntries[account["Target name"]].append(account)
                elif account["Date"] == month["endDay"]:
                    if account["Target name"] not in endEntries:
                        endEntries[account["Target name"]] = [account,]
                    else:
                        endEntries[account["Target name"]].append(account)

            hiddenFunds = cache.get("transactions_low", {}).get(month["dateTime"], [])
            #funds that do not have account positions. Just transactions that should not appear as a fund (ex: deferred liabilities)
            allPoolTransactions = {}
            for transaction in hiddenFunds:
                if transaction["Target name"] not in fundNames:
                    fundNames.append(transaction["Target name"])
                    funds.append({"fundName" : transaction["Target name"], "hidden" : True})
                if transaction["Target name"] not in allPoolTransactions:
                    allPoolTransactions[transaction["Target name"]] = [transaction,]
                else:
                    allPoolTransactions[transaction["Target name"]].append(transaction)

            poolGainSum = 0
            poolNAV = 0
            poolMDdenominator = 0
            poolWeightedCashFlow = 0
            fundEntryList = []
            for fundDict in funds:
                fund = fundDict["fundName"]
                if fund in (None,'None'):
                    continue
                hidden = fundDict["hidden"]
                assetClass = None
                subAssetClass = None
                fundClassification = None
                startEntryCache = startEntries.get(fund, [])
                endEntryCache = endEntries.get(fund, [])
                startEntry = copy.deepcopy(startEntryCache)
                endEntry = copy.deepcopy(endEntryCache)
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
                    commitment = float(startEntry[0].get(nameHier["Commitment"]["local"],0))
                    unfunded = float(startEntry[0].get(nameHier["Unfunded"]["local"],0))
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
                fundTransactions = allPoolTransactions.get(fund,[])
                cashFlowSum = 0
                weightedCashFlow = 0
                for transaction in fundTransactions:
                    if assetClass is None or assetClass == "None":
                        assetClass = transaction["SysProp_FundTargetNameAssetClass(E)"]
                    if subAssetClass is None or subAssetClass == "None":
                        subAssetClass = transaction["SysProp_FundTargetNameSub-assetClass(E)"]
                    if fundClassification is None or fundClassification == "None":
                        fundClassification = transaction["Target nameExposureHFClassificationLevel2"]
                    if transaction["TransactionType"] not in commitmentChangeTransactionTypes and transaction[nameHier["CashFlow"]["dynLow"]] not in (None, "None"):
                        cashFlowSum -= float(transaction[nameHier["CashFlow"]["dynLow"]])
                        backDate = calculateBackdate(transaction, noStartValue)
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
                    if unfunded < 0:
                        unfunded = 0 #corrects for if original commitment was not logged
                    if fundNAV == 0 and fundMDdenominator == 0 and unfunded == 0:
                        #skip if there is no value and no change in value
                        continue
                    elif createFinalValue:
                        fundEOMentry = {"Date" : month["endDay"], "Source name" : pool, "Target name" : fund , nameHier["Value"]["dynLow"] : endEntry[nameHier["Value"]["dynLow"]],
                                            "Balancetype" : "Calculated_R", "ExposureAssetClass" : assetClass, "ExposureAssetClassSub-assetClass(E)" : subAssetClass,
                                            nameHier["Commitment"]["local"] : commitment, nameHier["Unfunded"]["local"] : unfunded,
                                            nameHier["sleeve"]["local"] : fundList[fund], nameHier["Classification"]["dynLow"] : fundClassification}
                        insert_low.append(fundEOMentry)
                        # update cache for subsequent months
                        for m in months:
                            if m["accountStart"] <= month["endDay"] <= m["endDay"]:
                                cache.setdefault("positions_low", {}).setdefault(m["dateTime"], []).append(fundEOMentry)
                                break
                    else:
                        query = f"UPDATE positions_low SET [{nameHier['Commitment']["local"]}] = ? , [{nameHier['Unfunded']["local"]}] = ?, [{nameHier["sleeve"]["local"]}] = ? WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?"
                        inputs = (commitment,unfunded,fundList.get(fund),pool,fund,month["endDay"])
                        update_low.append(inputs)
                        # update cache for all months referencing this date
                        for m in months:
                            if m["accountStart"] <= month["endDay"] <= m["endDay"]:
                                for lst in cache.get("positions_low", {}).get(m["dateTime"], []):
                                    if lst["Target name"] == fund and lst["Date"] == month["endDay"]:
                                        lst[nameHier["Commitment"]["local"]] = commitment
                                        lst[nameHier["Unfunded"]["local"]] = unfunded
                                        lst[nameHier["sleeve"]["local"]] = fundList.get(fund)
                    poolGainSum += fundGain
                    poolMDdenominator += fundMDdenominator
                    poolNAV += fundNAV
                    poolWeightedCashFlow += weightedCashFlow
                    monthFundEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Fund", "Pool" : pool, "Fund" : fund ,
                                    "assetClass" : assetClass, "subAssetClass" : subAssetClass,
                                    "NAV" : fundNAV, "Monthly Gain" : fundGain, "Return" : fundReturn , 
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
                    print(traceback.format_exc())
                    #skips fund if the values are zero and cause an error
            if poolNAV == 0 and poolWeightedCashFlow == 0:
                #skips the pool if there is no cash flow or value in the pool
                continue
            poolReturn = poolGainSum/poolMDdenominator * 100 if poolMDdenominator != 0 else 0
            monthPoolEntry = {"dateTime" : month["dateTime"], "Investor" : "Total Pool", "Pool" : pool, "Fund" : None ,
                            "assetClass" : poolData.get("assetClass"), "subAssetClass" : poolData.get("subAssetClass") ,
                            "NAV" : poolNAV, "Monthly Gain" : poolGainSum, "Return" : poolReturn , "MDdenominator" : poolMDdenominator,
                                "Ownership" : None, "Calculation Type" : "Total Fund"}
            investorStartEntries = {}
            investorEndEntries = {}
            investorPositions = cache.get("positions_high", {}).get(month["dateTime"], [])
            for pos in investorPositions:
                investor = pos["Source name"]
                if pos["Date"] == month["accountStart"]:
                    if investor not in investorStartEntries:
                        investorStartEntries[investor] = [pos,]
                    else:
                        investorStartEntries[investor].append(pos)
                if pos["Date"] == month["endDay"]:
                    if investor not in investorEndEntries:
                        investorEndEntries[investor] = [pos,]
                    else:
                        investorEndEntries[investor].append(pos)

            allInvestorTransactions = {}
            transactions = cache.get("transactions_high", {}).get(month["dateTime"], [])
            for tran in transactions:
                investor = tran["Source name"]
                if investor not in allInvestorTransactions:
                    allInvestorTransactions[investor] = [tran,]
                else:
                    allInvestorTransactions[investor].append(tran)


            investorMDdenominatorSum = 0
            tempInvestorDicts = {}
            poolOwnershipSum = 0
            for investor in investorStartEntries:
                investorWeightedCashFlow = 0
                investorCashFlowSum = 0
                tempInvestorDict = {}
                try:
                    startEntry = investorStartEntries.get(investor)[0]
                    tempInvestorDict["Active"] = True
                except Exception as e:
                    #skip month for this investor if there is no starting balance
                    tempInvestorDict["Active"] = False
                    continue
                investorTransactions = allInvestorTransactions.get(investor,[])
                
                for transaction in investorTransactions:
                    investorCashFlowSum -= float(transaction[nameHier["CashFlow"]["dynHigh"]])
                    backDate = calculateBackdate(transaction)
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
                    monthPoolEntryInvestor["Monthly Gain"] = investorGain
                    monthPoolEntryInvestor["Return"] = investorReturn * 100
                    monthPoolEntryInvestor["MDdenominator"] = investorMDdenominator
                    ownershipPerc = investorEOM/poolNAV * 100 if poolNAV != 0 else 0
                    monthPoolEntryInvestor["Ownership"] = ownershipPerc
                    poolOwnershipSum += ownershipPerc
                    monthPoolEntryInvestorList.append(monthPoolEntryInvestor)
                    inputs = (investorEOM, investor,pool, month["endDay"])
                    EOMcheck = investorEndEntries.get(investor,[])
                    if len(EOMcheck) < 1:
                        EOMentry = {"Date" : month["endDay"], "Source name" : investor, "Target name" : pool, nameHier["Value"]["dynHigh"] : investorEOM,
                                    "Balancetype" : "Calculated_R", "ExposureAssetClass" : tempInvestorDicts[investor]["ExposureAssetClass"],
                                    "ExposureAssetClassSub-assetClass(E)" : tempInvestorDicts[investor]["ExposureAssetClassSub-assetClass(E)"],
                                    nameHier["Family Branch"]["dynHigh"] : tempInvestorDicts[investor][nameHier["Family Branch"]["local"]]}
                        insert_high.append(EOMentry)
                        for m in months:
                            if m["accountStart"] <= month["endDay"] <= m["endDay"]:
                                cache.setdefault("positions_high", {}).setdefault(m["dateTime"], []).append(EOMentry)
                                break
                    else:
                        update_high.append(inputs)
                        for m in months:
                            if m["accountStart"] <= month["endDay"] <= m["endDay"]:
                                for lst in cache.get("positions_high", {}).get(m["dateTime"], []):
                                    if lst["Source name"] == investor and lst["Target name"] == pool and lst["Date"] == month["endDay"]:
                                        lst[nameHier["Value"]["dynHigh"]] = investorEOM
                                        break
            monthPoolEntry["Ownership"] = poolOwnershipSum
            for investorEntry in monthPoolEntryInvestorList:
                for fundEntry in fundEntryList:
                    fundInvestorNAV = investorEntry["Ownership"] / 100 * fundEntry["NAV"]
                    fundInvestorGain = fundEntry["Monthly Gain"] / monthPoolEntry["Monthly Gain"] * investorEntry["Monthly Gain"] if monthPoolEntry["Monthly Gain"] != 0 else 0
                    fundInvestorMDdenominator = investorEntry["MDdenominator"] / monthPoolEntry["MDdenominator"] * fundEntry["MDdenominator"] if monthPoolEntry["MDdenominator"] != 0 else 0
                    fundInvestorReturn = fundInvestorGain / fundInvestorMDdenominator if fundInvestorMDdenominator != 0 else 0
                    fundInvestorOwnership = fundInvestorNAV /  fundEntry["NAV"] if fundEntry["NAV"] != 0 else 0
                    fundInvestorCommitment = fundEntry[nameHier["Commitment"]["local"]] * fundInvestorOwnership
                    fundInvestorUnfunded = fundEntry[nameHier["Unfunded"]["local"]] * fundInvestorOwnership
                    monthFundInvestorEntry = {"dateTime" : month["dateTime"], "Investor" : investorEntry["Investor"], "Pool" : pool, "Fund" : fundEntry["Fund"] ,
                                    "assetClass" : fundEntry["assetClass"], "subAssetClass" : fundEntry["subAssetClass"],
                                    "NAV" : fundInvestorNAV, "Monthly Gain" : fundInvestorGain , "Return" :  fundInvestorReturn * 100, 
                                    "MDdenominator" : fundInvestorMDdenominator, "Ownership" : fundInvestorOwnership * 100,
                                    "Classification" : fundEntry["Classification"], nameHier["Family Branch"]["local"] : investorEntry[nameHier["Family Branch"]["local"]],
                                    nameHier["Commitment"]["local"] : fundInvestorCommitment, nameHier["Unfunded"]["local"] : fundInvestorUnfunded, 
                                    "Calculation Type" : "Total Fund",
                                    nameHier["sleeve"]["local"] : fundList.get(fundEntry["Fund"])
                                    }
                    calculations.append(monthFundInvestorEntry)
            #end of months loop
        if insert_low:
            save_to_db("positions_low", insert_low, action="add", connection=calcConnection, lock=lock)
        if update_low:
            query = f"UPDATE positions_low SET [{nameHier['Commitment']['local']}] = ?, [{nameHier['Unfunded']['local']}] = ?, [{nameHier['sleeve']['local']}] = ? WHERE [Source name] = ? AND [Target name] = ? AND [Date] = ?"
            lock.acquire()
            try:
                calcCursor.executemany(query, update_low)
                calcConnection.commit()
            finally:
                lock.release()
        failed = updateStatus(pool,len(newMonths),lock,connection=calcConnection, status="Completed")
        return calculations
    except Exception as e:
        failed = updateStatus(pool,len(newMonths),lock,connection=calcConnection, status="Failed")
        print(f"Worker for {poolData.get("poolName")} failed.")
        print(traceback.format_exc())
        print("\n")
        return []
            
def calculateBackdate(transaction,noStartValue = False):
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
        buttonBox = QWidget()
        buttonLayout = QHBoxLayout()
        exportBtn = QPushButton("Export Data to Excel")
        exportBtn.clicked.connect(self.exportTable)
        buttonLayout.addWidget(exportBtn)
        self.headerOptions = SortButtonWidget()
        self.headerOptions.popup.popup_closed.connect(self.buildTable)
        buttonLayout.addWidget(self.headerOptions)
        buttonBox.setLayout(buttonLayout)
        layout.addWidget(buttonBox)
        self.table = QTableWidget(self)
        layout.addWidget(self.table)
        self.success = False
        self.headerOrder = ["Date", "TradeDate", "_source","Source name","Target name", "CashFlowSys", "ValueInSystemCurrency","Balancetype","TransactionType"]
        self.buildTable()

    def exportTable(self):
        # 1) prompt user
        path, _ = QFileDialog.getSaveFileName(
            self, "Save as…", "", "Excel Files (*.xlsx)"
        )
        if not path:
            return
        if not path.lower().endswith(".xlsx"):
            path += ".xlsx"

        def processExport():
            try:
                data = self.allData  # list of dicts

                all_cols = self.allCols


                # 4) create workbook
                wb = Workbook()
                ws = wb.active

                rowStart = 1
                for idx, colname in enumerate(all_cols, start=1):
                    ws.cell(row=rowStart, column=idx, value=colname)


                # 7) populate rows
                for r, row_dict in enumerate(data, start=rowStart + 1):


        
                    # data cells with proper formatting
                    for c, colname in enumerate(all_cols, start=1):
                        val = row_dict.get(colname, None)
                        cell = ws.cell(row=r, column=c, value=val)
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
            except Exception as e:
                gui_queue.put(lambda error = e, trace = traceback.format_exc(): QMessageBox.critical(self, "Processing error", f"{error} \n {trace}"))
            try:
                wb.save(path)
            except Exception as e:
                gui_queue.put(lambda error = e: QMessageBox.critical(self, "Save error", str(error)))
            else:
                gui_queue.put(lambda: QMessageBox.information(self, "Saved", f"Excel saved to:\n{path}"))
                gui_queue.put(lambda: QDesktopServices.openUrl(QUrl.fromLocalFile(path)))
        executor.submit(processExport)    
    def buildTable(self):
        if self.parent.tableBtnGroup.checkedButton().text() == "Monthly Table":
            selectedMonth = datetime.strptime(self.parent.selectedCell["month"], "%B %Y")
            tranStart = selectedMonth.replace(day = 1)
            accountStart = tranStart - relativedelta(days= 1)
            allEnd = (tranStart + relativedelta(months=1)) - relativedelta(days=1)
        else:
            endTime = datetime.strptime(self.parent.dataEndSelect.currentText(),"%B %Y")
            allEnd = (endTime.replace(day = 1) + relativedelta(months=1)) - relativedelta(days=1)
            selection = self.parent.selectedCell["month"]
            if selection not in timeOptions or selection == "MTD": #MTD timeframe
                tranStart = endTime.replace(day = 1)
                accountStart = tranStart - relativedelta(days= 1)
            elif selection == "ITD":
                tranStart = self.parent.dataTimeStart
                accountStart = self.parent.dataTimeStart
            else:
                if selection == "QTD":
                    subtract = (int((endTime.month)) % 3 if (int(endTime.month)) % 3 != 0 else 3) - 1
                elif selection == "YTD":
                    subtract = (int((endTime.month)) % 12 if (int(endTime.month)) % 12 != 0 else 12) - 1
                else:
                    subtract = int(selection.removesuffix("YR")) * 12 - 1
                tranStart = (endTime - relativedelta(months=subtract)).replace(day=1)
                accountStart = tranStart - relativedelta(days= 1)

        tranStart = datetime.strftime(tranStart,"%Y-%m-%dT00:00:00")
        accountStart = datetime.strftime(accountStart,"%Y-%m-%dT00:00:00")
        allEnd = datetime.strftime(allEnd,"%Y-%m-%dT00:00:00")
        dataType = self.parent.selectedCell["dataType"]
        if dataType == "Total":
            return
        dataType = dataType.removeprefix("Total ")
        code = self.parent.selectedCell["rowKey"]
        header, code= self.parent.separateRowCode(code)
        if header == "Cash ":
            header = "Cash"
        hier = code.removeprefix("##(").removesuffix(")##").split("::")
        hierSelections = self.parent.sortHierarchy.checkedItems()
        if dataType == "Fund":
            hier.append(header)
            hierSelections.append(dataType)
        highTables = {"positions_high": accountStart,"transactions_high" : tranStart}
        lowTables = {"positions_low": accountStart,"transactions_low": tranStart}
        all_rows = []
        if self.parent.filterDict["Investor"].checkedItems() != [] or self.parent.filterDict["Family Branch"].checkedItems() != []: #investor to pool level entries
            for idx, table in enumerate(highTables.keys()):
                query = "WHERE"
                inputs = []
                for hierIdx, tier in enumerate(hier):
                    for filter in self.parent.filterOptions:
                        dynName = filter.get("dynNameHigh")
                        if hierSelections[hierIdx] == filter["key"] and dynName is not None:
                            if filter["key"] == "assetClass" and idx == 1:
                                dynName = "SysProp_FundTargetNameAssetClass(E)"
                            elif filter["key"] == "subAssetClass" and idx == 1:
                                dynName = "SysProp_FundTargetNameSub-assetClass(E)"
                            query += f" [{dynName}] = ? AND"
                            inputs.append(tier)
                            break #continue to next tier
                if dataType == "Fund":
                    query += " [Target name] = ? AND"
                    inputs.append(self.parent.fundPoolLinks.get(header))
                for filter in self.parent.filterOptions:
                    filterSelections = self.parent.filterDict[filter["key"]].checkedItems()
                    dynName = filter.get("dynNameHigh")
                    if filter["key"] not in ("Classification") and filterSelections != [] and dynName is not None:
                        if filter["key"] == "assetClass" and idx == 1:
                            dynName = "SysProp_FundTargetNameAssetClass(E)"
                        elif filter["key"] == "subAssetClass" and idx == 1:
                            dynName = "SysProp_FundTargetNameSub-assetClass(E)"
                        if filter["key"] == "subAssetSleeve":
                            for sleeve in filterSelections:
                                if self.parent.sleeveFundLinks.get(sleeve) is not None:
                                    placeholders = ','.join('?' for _ in self.parent.sleeveFundLinks.get(tier)) 
                                    query += f" [Target name] in ({placeholders}) AND"
                                    inputs.extend(self.parent.sleeveFundLinks.get(tier))
                                else:
                                    print("Failed to find subAssetSleeve")
                        else:
                            placeholders = ','.join('?' for _ in filterSelections) 
                            query += f" [{dynName}] in ({placeholders}) AND"
                            inputs.extend(filterSelections)
                inputs.extend([highTables[table],allEnd])
                try:
                    rows = load_from_db(table,query.removesuffix("AND") + " AND [Date] BETWEEN ? AND ?", tuple(inputs))
                except Exception as e:
                    print(f"Error in call : {e} ; {e.args}")
                    rows = []
                for row in rows or []:
                    row['_source'] = table
                    all_rows.append(row)
        self.allData = all_rows
        for idx, table in enumerate(lowTables.keys()):
            query = "WHERE"
            inputs = []
            for hierIdx, tier in enumerate(hier): #iterate through each tier down to selection
                for filter in self.parent.filterOptions: #iterate through filter to find the matching keys
                    dynName = filter.get("dynNameLow")
                    if hierSelections[hierIdx] == filter["key"] and dynName is not None: #matching filter key
                        if filter["key"] == "assetClass" and idx == 1:
                            dynName = "SysProp_FundTargetNameAssetClass(E)"
                        elif filter["key"] == "subAssetClass" and idx == 1:
                            dynName = "SysProp_FundTargetNameSub-assetClass(E)"
                        if filter["key"] == "subAssetSleeve":
                            if self.parent.sleeveFundLinks.get(tier) is not None:
                                placeholders = ','.join('?' for _ in self.parent.sleeveFundLinks.get(tier)) 
                                query += f" [Target name] in ({placeholders}) AND"
                                inputs.extend(self.parent.sleeveFundLinks.get(tier))
                            else:
                                print("Failed to find subAssetSleeve")
                        elif filter["key"] == "Fund" and self.parent.cFundToFundLinks.get(tier) is not None: #account for consolidated funds
                            funds = self.parent.cFundToFundLinks.get(tier)
                            placeholders = ','.join('?' for _ in funds) 
                            inputs.extend(funds)
                            query += f" [Target name] in ({placeholders}) AND"
                        else:
                            query += f" [{dynName}] = ? AND"
                            inputs.append(tier)
                        break #continue to next tier
            for filter in self.parent.filterOptions:
                filterSelections = self.parent.filterDict[filter["key"]].checkedItems()
                dynName = filter.get("dynNameLow")
                if filterSelections != [] and dynName is not None:
                    if filter["key"] == "assetClass" and idx == 1:
                        dynName = "SysProp_FundTargetNameAssetClass(E)"
                    elif filter["key"] == "subAssetClass" and idx == 1:
                        dynName = "SysProp_FundTargetNameSub-assetClass(E)"
                    if filter["key"] == "subAssetSleeve":
                        for sleeve in filterSelections:
                            if self.parent.sleeveFundLinks.get(sleeve) is not None:
                                placeholders = ','.join('?' for _ in self.parent.sleeveFundLinks.get(tier)) 
                                query += f" [Target name] in ({placeholders}) AND"
                                inputs.extend(self.parent.sleeveFundLinks.get(tier))
                            else:
                                print("Failed to find subAssetSleeve")
                    else:
                        placeholders = ','.join('?' for _ in filterSelections) 
                        query += f" [{dynName}] in ({placeholders}) AND"
                        inputs.extend(filterSelections)
            inputs.extend([lowTables[table],allEnd])
            try:
                rows = load_from_db(table,query.removesuffix("AND") + " AND [Date] BETWEEN ? AND ?", tuple(inputs))
            except Exception as e:
                print(f"Error in call : {e}; {e.args}")
                rows = []
            for row in rows or []:
                row['_source'] = table
                all_rows.append(row) 

        if len(all_rows) == 0:
            print("No rows found")
            return
        # 3) Sort by dateTime column (handles ISO or space-separated)
        def parse_dt(s):
            return datetime.strptime(s, "%Y-%m-%dT00:00:00")

        all_rows.sort(key=lambda r: parse_dt(r.get('Date', '')))

        # 4) Collect the union of all column keys
        if not self.headerOptions.active:
            all_cols = self.headerOrder
            for row in all_rows:
                for key in row.keys():
                    if key not in all_cols:
                        all_cols.append(key)
            self.allCols = all_cols
            self.headerOptions.set_items(all_cols)
        else:
            self.allCols = self.headerOptions.popup.get_checked_sorted_items()

        # 5) Configure the table widget
        self.table.setRowCount(len(all_rows))
        self.table.setColumnCount(len(self.allCols))
        self.table.setHorizontalHeaderLabels(self.allCols)

        # 6) Populate each cell
        for r, row in enumerate(all_rows):
            for c, key in enumerate(self.allCols):
                raw = row.get(key,"")
                try:
                    num = float(raw)
                    text = f"{num:,.2f}"
                    item = QTableWidgetItem(text)
                    item.setData(Qt.UserRole,num)
                except:
                    item = QTableWidgetItem(str(raw))
                self.table.setItem(r, c, item)

        self.success = True

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
        self.searchBar = QLineEdit()
        self.searchBar.setPlaceholderText("Search")
        self.searchBar.textChanged.connect(self.parent.updateOptionVisibility)
        layout.addWidget(self.searchBar)
        # scrollable checkbox area
        self.scroll = QScrollArea(self)
        self.scroll.setWidgetResizable(True)
        container = QWidget()
        container.setObjectName("subPanel")
        self.box_layout = QVBoxLayout(container)
        self.box_layout.addStretch()
        container.setLayout(self.box_layout)
        self.scroll.setWidget(container)
        #self.scroll.setFixedHeight(150)
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
        self.popup.setObjectName("mainPage")
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
    def updateOptionVisibility(self):
        for cbKey in self._checkboxes.keys():
            self._checkboxes[cbKey].setVisible(self.popup.searchBar.text().lower() in self._checkboxes[cbKey].text().lower())
    def hierarchyMode(self):
        self.hierarchy = True
    def _togglePopup(self):
        if self.popup.isVisible():
            self.popup.hide()
        else:
            self.choiceChange = False
            self.popup.adjustSize()

            # 1. Get global position under the line edit
            pos = self.line_edit.mapToGlobal(QPoint(0, self.line_edit.height()))
            popup_size = self.popup.sizeHint()

            # 2. Get screen geometry
            screen_geom = QApplication.desktop().availableGeometry(self)

            # 3. Clamp the right edge
            if pos.x() + popup_size.width() > screen_geom.right():
                pos.setX(screen_geom.right() - popup_size.width())

            # 4. Clamp the bottom edge
            if pos.y() + popup_size.height() > screen_geom.bottom():
                pos.setY(screen_geom.bottom() - popup_size.height())

            # 5. Prevent left/top overflow
            pos.setX(max(screen_geom.left(), pos.x()))
            pos.setY(max(screen_geom.top(), pos.y()))

            # 6. Apply
            self.popup.move(pos)
            self.popup.show()
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
            if len(self.currentItems) == 0:
                return ["assetClass"]
            else:
                return self.currentItems
        else:
            return [t for t, cb in self._checkboxes.items() if cb.isChecked()]

    def clearSelection(self):
        for cb in self._checkboxes.values():
            cb.setChecked(False)
        self.popup.searchBar.setText("")
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

class SortPopup(QDialog):
    popup_closed = pyqtSignal()

    def __init__(self, items=None, checked_set=None, parent=None):
        super().__init__(parent, Qt.Popup)
        self.setWindowTitle("Sort Items")
        self.setMinimumSize(200, 300)

        self.list_widget = QListWidget(self)
        self.list_widget.setDragDropMode(QListWidget.InternalMove)
        self.list_widget.setDefaultDropAction(Qt.MoveAction)

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Select and Sort Headers:"))
        layout.addWidget(self.list_widget)

        self.set_items(items or [], checked_set or set())

        self.list_widget.itemChanged.connect(self.on_item_toggled)

    def set_items(self, items, checked_set):
        self.list_widget.blockSignals(True)
        self.list_widget.clear()
        for item in items:
            list_item = QListWidgetItem(item)
            list_item.setFlags(list_item.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsDragEnabled)
            list_item.setCheckState(Qt.Checked if item in checked_set else Qt.Unchecked)
            self.list_widget.addItem(list_item)
        self.list_widget.blockSignals(False)

    def on_item_toggled(self, item: QListWidgetItem):
        # Remove the item from current position
        row = self.list_widget.row(item)
        self.list_widget.takeItem(row)

        if item.checkState() == Qt.Unchecked:
            # Add to end of list
            self.list_widget.addItem(item)
        else:
            # Insert before first unchecked item
            insert_index = self.list_widget.count()
            for i in range(self.list_widget.count()):
                if self.list_widget.item(i).checkState() == Qt.Unchecked:
                    insert_index = i
                    break
            self.list_widget.insertItem(insert_index, item)

        # Reselect the item for visual consistency (optional)
        self.list_widget.setCurrentItem(item)

    def get_checked_sorted_items(self):
        return [
            self.list_widget.item(i).text()
            for i in range(self.list_widget.count())
            if self.list_widget.item(i).checkState() == Qt.Checked
        ]

    def get_all_items(self):
        return [self.list_widget.item(i).text() for i in range(self.list_widget.count())]

    def get_checked_items_set(self):
        return {
            self.list_widget.item(i).text()
            for i in range(self.list_widget.count())
            if self.list_widget.item(i).checkState() == Qt.Checked
        }

    def closeEvent(self, event):
        self.popup_closed.emit()
        super().closeEvent(event)


class SortButtonWidget(QWidget):
    popup_closed = pyqtSignal(list)  # emits checked, sorted items

    def __init__(self, parent=None):
        super().__init__(parent)
        self.items = []
        self.checked_items = set()
        self.active = False

        self.button = QPushButton("Header Options", self)
        self.button.clicked.connect(self.show_popup)

        layout = QVBoxLayout(self)
        layout.addWidget(self.button)
        layout.setContentsMargins(0, 0, 0, 0)

        self.popup = SortPopup(self.items, self.checked_items, self)

    def add_item(self, item, checked=True):
        self.items.append(item)
        if checked:
            self.checked_items.add(item)
        self.popup.set_items(self.items,self.checked_items)
        self.active = True

    def set_items(self, items, checked_items=None):
        self.items = list(items)
        self.checked_items = set(checked_items or items)
        self.popup.set_items(self.items,self.checked_items)
        self.active = True

    def show_popup(self):
        if self.popup.isVisible():
            self.popup.hide()
        else:
            self.popup.popup_closed.connect(self.on_popup_closed)

            button_pos = self.button.mapToGlobal(QPoint(0, self.button.height()))
            self.popup.move(button_pos)
            self.popup.show()

    def on_popup_closed(self):
        if self.popup:
            self.items = self.popup.get_all_items()
            self.checked_items = self.popup.get_checked_items_set()
            #self.popup_closed.emit(self.popup.get_checked_sorted_items())
            self.popup.hide()

class SmartStretchTable(QTableWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        # Use interactive resizing by default
        self.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)

        # Watch for resize events to re-evaluate
        self.viewport().installEventFilter(self)

        # Optional: Smooth scrolling
        self.setHorizontalScrollMode(self.ScrollPerPixel)

    def eventFilter(self, obj, event):
        if obj == self.viewport():
            QTimer.singleShot(0, self._maybeStretchColumns)
        return super().eventFilter(obj, event)

    def _maybeStretchColumns(self):
        col_count = self.columnCount()
        if col_count == 0:
            return

        total_width = sum(self.columnWidth(i) for i in range(col_count))
        available = self.viewport().width()

        if total_width < available:
            stretch_width = available // col_count
            for i in range(col_count):
                self.setColumnWidth(i, stretch_width)

    def updateData(self, data):
        # Example dynamic population method
        row_count = len(data)
        col_count = len(data[0]) if row_count else 0

        self.setRowCount(row_count)
        self.setColumnCount(col_count)

        for r in range(row_count):
            for c in range(col_count):
                self.setItem(r, c, QTableWidgetItem(str(data[r][c])))

        QTimer.singleShot(0, self._maybeStretchColumns)

if __name__ == '__main__':
    freeze_support()
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
