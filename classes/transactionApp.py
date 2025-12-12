from scripts.importList import *
from scripts.loggingFuncs import attach_logging_to_class
from classes.DatabaseManager import DatabaseManager
from scripts.instantiate_basics import *
from classes.widgetClasses import *
from scripts.commonValues import *
from classes.DatabaseManager import *
from scripts.processNode import processNode
from scripts.basicFunctions import *
from classes.windowClasses import *
from classes.tableWidgets import *

@attach_logging_to_class
class transactionApp(QWidget):
    def __init__(self,  dbMan : DatabaseManager, start_index=0, apiKey = None):
        super().__init__()
        self.setWindowTitle('Transaction Compare App')
        self.setGeometry(100, 100, 1000, 600)

        os.makedirs(ASSETS_DIR, exist_ok=True)
        self.db = dbMan
        self.start_index = start_index
        self.api_key = apiKey
        self.filterCallLock = False
        self.cancel = False
        self.lock = self.db._lock
        self.tableWindows = {}
        self.dataTimeStart = datetime(2000,1,1)
        self.earliestChangeDate = datetime.now() + relativedelta(months = 1)
        self.poolChangeDates = {"active" : False}
        self.currentTableData = None
        self.fullLevelOptions = {}
        self.buildTableCancel = None
        self.buildTableFuture = None
        self.cFundsCalculated = False
        self.previousGrouping = []

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
        loadLay = QGridLayout()
        self.calculationLoadingBar = QProgressBar()
        self.calculationLoadingBar.setRange(0,100)
        self.calculationLabel = QLabel()
        self.cancelCalcBtn = QPushButton("Cancel Calculations")
        self.cancelCalcBtn.setObjectName("cancelBtn")
        self.cancelCalcBtn.setEnabled(False)
        self.cancelCalcBtn.clicked.connect(self.cancelCalc)
        loadLay.addWidget(self.calculationLabel,0,0,1,5)
        loadLay.addWidget(self.calculationLoadingBar, 1,0, 1,5)
        loadLay.addWidget(self.cancelCalcBtn, 2, 2)
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
        controlsLayout.addStretch(1)
        btn_to_results = QPushButton('See Calculation Database')
        btn_to_results.clicked.connect(self.show_results)
        controlsLayout.addWidget(btn_to_results, stretch=0)
        self.exportBtn = QPushButton("Export Current Table to Excel")
        self.exportBtn.clicked.connect(self.exportCurrentTable)
        self.exportBtn.setObjectName("exportBtn")
        controlsLayout.addWidget(self.exportBtn, stretch=0)
        controlsLayout.addStretch(1)
        controlsBox.setLayout(controlsLayout)
        layout.addWidget(controlsBox)

        optionsBox = QWidget()
        optionsBox.setObjectName("borderFrame")
        optionsGrid = QGridLayout()
        optionsTitle = QLabel("Options")
        optionsTitle.setObjectName("titleBox")
        optionsGrid.addWidget(optionsTitle,0,0,2,1)
        self.tableBtnGroup = QButtonGroup()
        self.complexTableBtn = QRadioButton("Complex Table")
        self.complexTableBtn.setVisible(False)
        
        
        self.monthlyTableBtn = QRadioButton("Monthly Table")
        self.monthlyTableBtn.setVisible(False)
        buttonBox = QWidget()
        buttonLayout = QVBoxLayout()
        for idx, rb in enumerate((self.monthlyTableBtn,self.complexTableBtn)):
            self.tableBtnGroup.addButton(rb)
            #rb.toggled.connect(self.updateTableType)
            buttonLayout.addWidget(rb)
        self.monthlyTableBtn.setChecked(True)
        self.complexTableBtn.setEnabled(False)
        self.returnOutputType = QComboBox()
        self.returnOutputType.addItems(tranAppHeaderOptions)
        self.returnOutputType.currentTextChanged.connect(self.buildReturnTable)
        self.returnOutputType.setVisible(False)
        self.dataTypeBox = QWidget()
        dataTypeLayout = QHBoxLayout()
        dataTypeLayout.addWidget(self.returnOutputType)
        self.dataTypeBox.setLayout(dataTypeLayout)
        buttonLayout.addWidget(self.dataTypeBox)
        buttonBox.setLayout(buttonLayout)
        optionsGrid.addWidget(buttonBox, 0,1,2,1)
        self.tableBtnGroup.buttonClicked.connect(self.buildReturnTable)
        
        self.dataStartSelect = simpleMonthSelector()
        self.dataEndSelect = simpleMonthSelector()
        for idx, [text, CB] in enumerate((["Start: ", self.dataStartSelect], ["End: ", self.dataEndSelect])):
            optionsGrid.addWidget(QLabel(text),idx,2)
            optionsGrid.addWidget(CB,idx,3)
        self.sortHierarchy = MultiSelectBox()
        self.sortHierarchy.hierarchyMode()
        self.sortHierarchy.addItem("Pool")
        self.sortHierarchy.setCheckedItem("Pool")
        self.sortHierarchy.setEnabled(False)
        self.sortHierarchy.setVisible(False)
        self.sortHierarchy.popup.closed.connect(self.groupingChange)
        optionsGrid.addWidget(self.sortHierarchy,1,5)
        self.consolidateFundsBtn = QRadioButton("Consolidate Funds")
        self.consolidateFundsBtn.setChecked(True)
        self.consolidateFundsBtn.setVisible(False)
        self.consolidateFundsBtn.clicked.connect(self.buildReturnTable)
        optionsGrid.addWidget(self.consolidateFundsBtn,0,6)
        self.exitedFundsBtn = QRadioButton("Show Exited Funds (Cannot turn off)")
        self.exitedFundsBtn.setChecked(False)
        self.exitedFundsBtn.setEnabled(False) #remove later
        self.exitedFundsBtn.setChecked(True)  #remove later
        self.exitedFundsBtn.setVisible(False)
        optionsGrid.addWidget(self.exitedFundsBtn,1,6)
        self.headerSort = SortButtonWidget()
        self.headerSort.popup.popup_closed.connect(self.headerSortClosed)
        self.headerSort.setVisible(False)
        optionsGrid.addWidget(self.headerSort,0,7,2,1)
        optionsBox.setLayout(optionsGrid)
        layout.addWidget(optionsBox)

        mainFilterBox = QWidget()
        mainFilterBox.setObjectName("borderFrame")
        mainFilterLayout = QGridLayout()
        filterTitle = QLabel("Filters")
        filterTitle.setObjectName("titleBox")
        mainFilterLayout.addWidget(filterTitle,0,0,2,1)

        self.filterOptions = [
                            {"key": "Node",           "name": "Node", "dataType" : "Total Node"},                            
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
            self.filterDict[filter["key"]] = MultiSelectBox(dispLib=self.db.userDisplayLib())
            self.filterDict[filter["key"]].popup.closed.connect(lambda: self.filterUpdate())
            
            mainFilterLayout.addWidget(self.filterDict[filter["key"]],1,col)
        self.filterBtnGroup.buttonToggled.connect(self.filterBtnUpdate)
        mainFilterBox.setLayout(mainFilterLayout)
        layout.addWidget(mainFilterBox)
        t1 = QVBoxLayout() #build table loading bar
        self.buildTableLoadingBox = QWidget()
        t1.addWidget(QLabel("Building transactions table..."))
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
        unDataBox = QWidget()
        unDataLayout = QHBoxLayout()
        unDataLayout.addStretch(1)
        self.viewUnderlyingDataBtn = QPushButton("View Underlying Data")
        self.viewUnderlyingDataBtn.clicked.connect(self.viewUnderlyingData)
        unDataLayout.addWidget(self.viewUnderlyingDataBtn,stretch=0)
        unDataLayout.addStretch(1)
        unDataBox.setLayout(unDataLayout)
        layout.addWidget(unDataBox)
        


        page.setLayout(layout)
        self.stack.addWidget(page)

        self.instantiateFilters()
        self.updateMonthOptions()
        if self.start_index != 0:
            self.filterUpdate()
        self.dataEndSelect.currentTextChanged.connect(self.buildReturnTable)
        self.dataStartSelect.currentTextChanged.connect(self.buildReturnTable)
    def init_data_processing(self):
        self.calcSubmitted = False
        lastImportDB = load_from_db(self.db,"history")
        lastImportDB  if len(lastImportDB) == 1 else None
        if lastImportDB is None:
            QMessageBox.warning(self,"Missing Data","Missing data from the returns app. Transaction app uses data pulled by the returns app.")
        else:
            lastImportString = lastImportDB[0]["lastCalculation"]
            lastImport = datetime.strptime(lastImportString, "%B %d, %Y @ %I:%M %p")  
            self.lastImportLabel.setText(f"Last Data Calculation: {lastImportString}")
            now = datetime.now()
            #run calculations if they do not match the last import from returns Calculator, otherwise, continue as normal
            if lastImportDB[0]["lastImport"] != lastImportDB[0].get("lastCalculation", "None"):
                self.earliestChangeDate = datetime.strptime(lastImportDB[0].get("changeDate"), "%B %d, %Y @ %I:%M %p")
                self.calculateReturn()
            else:
                calculations = load_from_db(self.db,"tranCalculations")
                if calculations != []:
                    self.populate(self.calculationTable,calculations)
                    self.buildReturnTable()
                else:
                    self.calculateReturn()
            self.bgWatch = QTimer()
            self.bgWatch.timeout.connect(self.watchForUpdateTime)
            hours = 0.05 #check every 3 minutes for an update
            self.bgWatch.start(int(hours * 60 * 60 * 1000))
    def watchForUpdateTime(self):
        try:
            print("Checking if update required.")
            history = load_from_db(self.db,"history")[0]
            if history["lastCalculation"] != history["lastImport"]:
                print("Recalculating due to a new import detected...")
                self.calculateReturn()
        except:
            print("Background watch failed")
    def cancelCalc(self, *_):
        _ = updateStatus(self,"DummyFail",99, status="Failed")
        self.cancel = True
    def viewUnderlyingData(self,*_):
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
            window = underlyingDataWindow(parentSource=self, db=TRAN_DATABASE_PATH)
            self.udWindow = window
            if window.success:
                window.show()
        except Exception as e:
            print(f"Error in data viewing window: {e} {traceback.format_exc()}")
    def exportCurrentTable(self,*_):
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
                    full_hierarchy = ["Total"] + ["Total " + level for level in self.sortHierarchy.checkedItems()] + ["Total Pool"]
                else:
                    full_hierarchy = ["Total", "Total assetClass", "Total Pool"]
                hierarchy_levels = [lvl for lvl in full_hierarchy if lvl in all_types]
                num_hier = len(hierarchy_levels)

                # 3) dynamic data columns minus "dataType"
                all_cols = {
                    k for row in data.values() for k in row.keys()
                    if k != "dataType"
                }

                sorted_cols = self.orderColumns(all_cols)

                # 4) create workbook or add sheet if already exists
                if os.path.exists(path):
                    wb = load_workbook(path)
                    # Create a unique sheet name for export
                    import datetime
                    base_name = "Export"
                    i = 1
                    while True:
                        sheet_name = f"{base_name}{i}"
                        if sheet_name not in wb.sheetnames:
                            break
                        i += 1
                    ws = wb.create_sheet(sheet_name)
                else:
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
                    # Default: light gray
                    data_color = "A0A0A0"
                    header_color = data_color

                    # Reproduce the main table cell & header coloring logic from lines 2567-2586
                    if dtype == "Total":
                        # "Total" rows: shade startColor darker (0.8)
                        data_color = "CDCDCD"  # A0A0A0 at 80% gray
                        header_color = "B7B7B7"  # slightly darker for headers
                    elif dtype == "benchmark":
                        # "benchmark" rows: a blue as in GUI
                        data_color = "B6CDF5"
                        header_color = "A3B8DB"  # a harder blue for headers
                    else:
                        # hierarchy coloring as in the GUI
                        base_rgb = (160,160,160)
                        # In gui: 'depth = code.count("::") if dataType != "Total Fund" else code.count("::") + 1'
                        # But "Total Fund" doesn't exist here; treat as "else"
                        if dtype != "Total Fund":
                            depth = code.count("::")
                        else:
                            depth = code.count("::") + 1
                        maxDepth = max(len(self.sortHierarchy.checkedItems()),1)
                        cRange = 255 - base_rgb[0]
                        ratio = (depth / maxDepth) if maxDepth != 0 else 1
                        def clamp(x): return max(0,min(255,int(x)))
                        def rgb_to_hex(rgb):
                            return "".join(f"{clamp(x):02X}" for x in rgb)
                        # Table: color = int(startColor[i] + cRange * ratio)
                        color_rgb = tuple(
                            clamp(base_rgb[i] + cRange * ratio)
                            for i in range(3)
                        )
                        data_color = rgb_to_hex(color_rgb)
                        # Header: make it "harder" (darker) by multiplying ratio by 1.08 (max 1.0)
                        header_ratio = min(ratio * 1.08, 1.0)
                        header_rgb = tuple(
                            clamp(base_rgb[i] + cRange * header_ratio)
                            for i in range(3)
                        )
                        header_color = rgb_to_hex(header_rgb)

                    # Even/odd row striping: darken data color a bit for odd rows
                    if r % 2 == 1:
                        def hex_to_rgb(h): return tuple(int(h[i:i+2],16) for i in (0,2,4))
                        cur_rgb = hex_to_rgb(data_color)
                        data_color = rgb_to_hex(tuple(
                            int(x*0.93) for x in cur_rgb
                        ))
                        # Make header match "hardness": use the same darkening factor, but even slightly darker
                        header_rgb = hex_to_rgb(header_color)
                        header_color = rgb_to_hex(tuple(
                            int(x*0.91) for x in header_rgb
                        ))

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
    def buildReturnTable(self, *_):
        self.buildTableLoadingBox.setVisible(True)
        self.buildTableLoadingBar.setValue(2)
        def buildTable(cancelEvent):
            try:
                print("Building transactions table...")
                self.currentTableData = None #resets so a failed build won't be used
                
                if self.tableBtnGroup.checkedButton().text() == "Complex Table":
                    gui_queue.put(lambda: self.returnOutputType.setCurrentText("Return"))
                    gui_queue.put(lambda: self.dataTypeBox.setVisible(False))
                else:
                    gui_queue.put(lambda: self.dataTypeBox.setVisible(True))
                parameters = []
                condStatement = None
                for filter in self.filterOptions:
                    if filter["key"] != "Investor" and filter["key"] != nameHier["Family Branch"]["local"]:
                        if self.filterDict[filter["key"]].checkedItems() != []:
                            id2Node = self.db.pullId2Node()
                            paramTemp = self.filterDict[filter["key"]].checkedItems()
                            for param in paramTemp:
                                parameters.append((id2Node.get(int(param),param)) if param not in (None,'None') else param)
                            placeholders = ','.join('?' for _ in paramTemp)
                            if condStatement:
                                condStatement += f" AND [{filter["key"]}] IN ({placeholders})"
                            else:
                                condStatement = f"WHERE [{filter["key"]}] IN ({placeholders})"
                gui_queue.put(lambda: self.buildTableLoadingBar.setValue(3))
                if cancelEvent.is_set(): #exit if new table build request is made
                    return
                data = load_from_db(self.db,"tranCalculations",condStatement if condStatement else "", tuple(parameters))
                output = {"Total##()##" : {}}
                #output , data = self.calculateUpperLevels(output,data)
                gui_queue.put(lambda: self.buildTableLoadingBar.setValue(4))
                if cancelEvent.is_set(): #exit if new table build request is made
                    return
                complexOutput = copy.deepcopy(output)
                multiPoolFunds = {}
                dataOutputType = self.returnOutputType.currentText()
                Dtype = "Total Node"
                for entry in (entry for entry in data if entry.get('dateTime') not in (None,'')):
                    if (datetime.strptime(entry["dateTime"], "%Y-%m-%d %H:%M:%S") >  datetime.strptime(self.dataEndSelect.currentText(),"%B %Y") or 
                        datetime.strptime(entry["dateTime"], "%Y-%m-%d %H:%M:%S") <  datetime.strptime(self.dataStartSelect.currentText(),"%B %Y")):
                        #don't build in data outside the selection
                        continue
                    date = datetime.strftime(datetime.strptime(entry["dateTime"], "%Y-%m-%d %H:%M:%S"), "%B %Y")
                    level = entry.get("Node") + "##(" + entry.get("Node") + ")##"

                    
                    if level not in output.keys():
                        output[level] = {}
                    if entry.get(dataOutputType) not in (None,"None",""):
                        if date not in output[level].keys():
                            #creates value if not exists. If it is not return percent, sums the values
                            output[level][date] = float(entry.get(dataOutputType))
                        elif dataOutputType not in ("Return", "Ownership"):
                            output[level][date] += float(entry.get(dataOutputType))
                        else: #should only reach here if two calculations exist of the same exact row which needs special handling of the return
                            if level not in multiPoolFunds:
                                multiPoolFunds[level] = [entry,]
                            else:
                                multiPoolFunds[level].append(entry)
                    if "dataType" not in output[level].keys():
                        output[level]["dataType"] = Dtype
                if multiPoolFunds and dataOutputType == "Return": #must iterate through data again to correct for returns of multi pool funds
                    multiData = {}
                    for rowKey in multiPoolFunds: #instantiate multiData with the row
                        multiData[rowKey] = {}
                        # date = multiPoolFunds.get(rowKey).get("dateTime")
                        # multiData[rowKey][date] = {"MDdenominator" : 0, "Monthly Gain" : 0}
                    for entry in data:
                        if (datetime.strptime(entry["dateTime"], "%Y-%m-%d %H:%M:%S") >  datetime.strptime(self.dataEndSelect.currentText(),"%B %Y") or 
                            datetime.strptime(entry["dateTime"], "%Y-%m-%d %H:%M:%S") <  datetime.strptime(self.dataStartSelect.currentText(),"%B %Y")):
                            #don't build in data outside the selection
                            continue
                        if entry.get("rowKey") in multiData: #only occurs for the multifunds
                            #sums all gains and MDden for a row for a month
                            dateTime = entry.get("dateTime")
                            if dateTime not in multiData[entry.get("rowKey")]:
                                multiData[entry.get("rowKey")][entry.get("dateTime")] = {"MDdenominator" : float(entry.get("MDdenominator")), "Monthly Gain" : float(entry.get("Monthly Gain"))}
                            else:
                                multiData[entry.get("rowKey")][entry.get("dateTime")]["MDdenominator"] += float(entry.get("MDdenominator"))
                                multiData[entry.get("rowKey")][entry.get("dateTime")]["Monthly Gain"] += float(entry.get("Monthly Gain"))
                    for rowKey in multiData: #set proper return values
                        for date in multiData.get(rowKey):
                            strDate = datetime.strftime(datetime.strptime(date, "%Y-%m-%d %H:%M:%S"), "%B %Y")
                            MDden = multiData.get(rowKey).get(date).get("MDdenominator")
                            returnVal = multiData.get(rowKey).get(date).get("Monthly Gain") / MDden * 100 if MDden != 0 else 0
                            output[rowKey][strDate] = returnVal
                            if self.tableBtnGroup.checkedButton().text() == "Complex Table" and strDate == self.dataEndSelect.currentText():
                                complexOutput[rowKey]["Return"] = returnVal
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
    def buildCode(self, path):
            code = f"##({"::".join(path)})##"
            return code
    def calculateUpperLevels(self, tableStructure,data):
        
        def buildLevel(levelName,levelIdx, struc,data,path : list):
            levelIdx += 1
            entryTemplate = {"dateTime" : None,  "Pool" : None}
            for header in tranAppHeaderOptions:
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
                            for label in tranAppDataOptions:
                                highEntries[total["dateTime"]][label] = total[label]
                            if levelName not in ("Investor","Family Branch"):
                                highEntries[total["dateTime"]][levelName] = total[levelName] if total[levelName] != "Cash" or levelName != "assetClass" else "Cash "
                                if levelName == "subAssetClass":
                                    highEntries[total["dateTime"]]["assetClass"] = total["assetClass"] if total["assetClass"] != "Cash" else "Cash "
                        for header in tranAppHeaderOptions:
                            if header != "Ownership":
                                highEntries[total["dateTime"]][header] += float(total[header])
                            elif levelName in ("Pool", "Investor", "Family Branch") and total.get(header) not in (None,"None","",0) and "Pool" in sortHierarchy[:levelIdx]:
                                if highEntries[total["dateTime"]].get(header) is None:
                                    highEntries[total["dateTime"]][header] = float(total[header]) #initialize
                                else:
                                    highEntries[total["dateTime"]][header] += float(total[header]) #aggregate pool ownerships
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
                    #gui_queue.put(lambda rows = levelData, name = option: self.openTableWindow(rows,f"data for: {name}"))
                    nameList = []
                    investorsAccessed = {}
                    for entry in levelData:
                        fundName = entry["Fund"] if not self.consolidateFundsBtn.isChecked() or entry["Fund"] not in self.consolidatedFunds or entry["Fund"] in self.filterDict["Fund"].checkedItems() else self.consolidatedFunds.get(entry["Fund"]).get("cFund")
                        nameList.append(fundName + code)
                        temp = entry.copy()
                        temp["rowKey"] = fundName + code
                        totalDataLow.append(temp)
                        if entry["dateTime"] not in totalEntriesLow:
                            totalEntriesLow[entry["dateTime"]] = copy.deepcopy(entryTemplate)
                            totalEntriesLow[entry["dateTime"]]["rowKey"] =name + code
                            for label in tranAppDataOptions:
                                totalEntriesLow[entry["dateTime"]][label] = entry[label]
                            if levelName not in ("Investor","Family Branch"):
                                totalEntriesLow[entry["dateTime"]][levelName] = entry[levelName] if entry[levelName] != "Cash" or levelName != "assetClass" else "Cash "
                                if levelName == "subAssetClass":
                                    totalEntriesLow[entry["dateTime"]]["assetClass"] = entry["assetClass"] if entry["assetClass"] != "Cash" else "Cash "
                        for header in tranAppHeaderOptions:
                            if header != "Ownership":
                                totalEntriesLow[entry["dateTime"]][header] += float(entry[header])
                            elif levelName in ("Investor", "Family Branch") and "Pool" in sortHierarchy and entry.get(header) not in (None,"None","") and float(entry.get(header)) != 0:
                                investor = entry.get("Investor")
                                if totalEntriesLow[entry["dateTime"]].get(header) is None:
                                    totalEntriesLow[entry["dateTime"]][header] = float(entry[header]) #assign investor to ownership based on fund
                                    investorsAccessed[entry["dateTime"]] = [investor,]
                                elif investor not in investorsAccessed.get(entry["dateTime"], []): #accounts for family branch level to add the investor level ownerships
                                    totalEntriesLow[entry["dateTime"]][header] += float(entry[header])
                                    investorsAccessed[entry["dateTime"]].append(investor)
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
                for header in tranAppHeaderOptions:
                    if header != "Ownership":
                        trueTotalEntries[total["dateTime"]][header] = 0
                for label in tranAppDataOptions:
                    trueTotalEntries[total["dateTime"]][label] = total[label]
            for header in tranAppHeaderOptions:
                if header != "Ownership":
                    trueTotalEntries[total["dateTime"]][header] += float(total[header])
        for month in trueTotalEntries.keys():
            trueTotalEntries[month]["Return"] = trueTotalEntries[month]["Monthly Gain"] / trueTotalEntries[month]["MDdenominator"] * 100 if trueTotalEntries[month]["MDdenominator"] != 0 else 0
            newEntries.append(trueTotalEntries[month])
        #data.extend(newEntries)
        return tableStructure,newEntries
                    
    def filterUpdate(self):
        self.buildReturnTable()
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
        save_to_db(self.db,"Months",dbDates)
    def instantiateFilters(self,*_):
        self.filterDict['Node'].clearItems()
        self.filterDict['Node'].addItems([str(n) for n in list(self.db.pullId2Node().keys())])

    def groupingChange(self, *_):
        groupOpts = self.sortHierarchy.checkedItems()
        self.filterCallLock = True
            
        self.filterCallLock = False
        self.buildReturnTable()
    def check_api_key(self, *_):
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

    def show_results(self,*_):
        self.stack.setCurrentIndex(2)

    def openTableWindow(self, rows, name = "Table", headers = None):
        window = tableWindow(parentSource=self,all_rows=rows,table=name, headers=headers)
        self.tableWindows[name] = window
        window.show()
    def calculateReturn(self):
        def initalizeCalc():
            try:
                gui_queue.put(lambda: self.calculationLoadingBox.setVisible(True))
                print("Calculating differences....")
                fundListDB = load_from_db(self.db,"funds")
                fundList = {}
                for fund in fundListDB:
                    fundList[fund["Name"]] = fund[nameHier["sleeve"]["sleeve"]]
                months = load_from_db(self.db,"Months", f"ORDER BY [dateTime] ASC")
                calculations = []
                if load_from_db(self.db,"tranCalculations") == []:
                    noCalculations = True
                else:
                    noCalculations = False
                noCalculations = True #force to calculate from scratch

                if self.earliestChangeDate > datetime.now() and not noCalculations:
                    #if no new data exists, use old calculations
                    calculations = load_from_db(self.db,"tranCalculations")
                    keys = []
                    for row in calculations:
                        for key in row.keys():
                            if key not in keys:
                                keys.append(key)
                    gui_queue.put( lambda: self.populate(self.calculationTable,calculations,keys = keys))
                    gui_queue.put( lambda: self.buildReturnTable())
                    gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
                    print("Calculations skipped.")
                    return
                
                # proces pool section----------------------------------------------------------------
                self.workerProgress = {}

                # ------------------- build data cache ----------------------
                tables = ["transactions",]
                table_rows = {t: load_from_db(self.db, t) for t in tables}
                nodes = self.db.pullId2Node().values()
                cache = {}
                for table, rows in table_rows.items():
                    for row in rows:
                        for direction in ["Target name" , "Source name", 'node']:
                            potNode = row.get(direction)
                            if potNode not in nodes:
                                continue
                            else:
                                tableName = 'transactions_below' if 'Source' in direction else 'transactions_above'
                                for m in months: #find the month the account balance or transaction belongs in
                                    start = m["accountStart"] if table == "positions" else m["tranStart"]
                                    date = row.get("Date")
                                    if not (start <= date <= m["endDay"]):
                                        continue
                                    cache.setdefault(potNode, {}).setdefault(tableName, {}).setdefault(m["dateTime"], []).append(row)
                runNodes = [{'name' : node} for node in nodes]
                for idx, node in enumerate(runNodes):
                    runNodes[idx]["cache"] = cache.get(node.get("name"))
                    runNodes[idx]["earliestChangeDate"] =  self.earliestChangeDate 
                    newMonths = []
                    if not noCalculations: #if there are calculations, find all months before the data pull, and then pull those calculations
                        for month in months:
                            #if the calculations for the month have already been complete, pull the old data
                            if runNodes[idx]["earliestChangeDate"] > datetime.strptime(month["endDay"], "%Y-%m-%dT%H:%M:%S"):
                                pass
                            else:
                                newMonths.append(month)
                    else:
                        newMonths = months
                    _ = updateStatus(self,node.get("name"),len(newMonths),status="Initialization")
                def initializeWorkerPool():
                    self.manager = Manager()
                    self.lock = self.manager.Lock()
                    self.workerStatusQueue = self.manager.Queue()
                    self.workerDBqueue = self.manager.Queue()
                    self.calcFailedFlag = self.manager.Value('b', False)
                    self.cancelCalcBtn.setEnabled(True) #only allows cancelling once the lock for the db exists

                    self.pool = Pool()
                    self.futures = []
                    executor.submit(self.watch_db, len(runNodes))

                    commonData = {"noCalculations" : noCalculations,
                                    "months" : months, "fundList" : fundList
                                    }
                    
                    self.calcStartTime = datetime.now()
                    for node in runNodes:
                        res = self.pool.apply_async(processNode, args=(node, commonData,self.workerStatusQueue, self.workerDBqueue, self.calcFailedFlag, True))
                        self.futures.append(res)
                    self.pool.close()

                    self.timer.start(int(calculationPingTime * 0.25) * 1000) #check at 0.75 the ping time to prevent queue buildup
                gui_queue.put(lambda: initializeWorkerPool()) #puts on main thread
            except Exception as e:
                gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
                print(f"Error occured running calculations: {e}")
                print("e.args:", e.args)
                # maybe also:
                print(traceback.format_exc())
        executor.submit(initalizeCalc)
    def watch_db(self, nodeCount):
        while True:
            count = 0
            while not self.workerStatusQueue.empty() and count < 300:
                count += 1 #count to allow the loading bar to take the lock and update
                vars = self.workerStatusQueue.get()
                try:
                    failed = updateStatus(self, vars[0],vars[1],status=vars[2])
                    if failed:
                        self.calcFailedFlag = failed
                except Exception as e:
                    trace = traceback.format_exc()
                    print(f"Error occured while attempting to run background worker status update: {e}. \n traceback: \n {trace}")
            try:
                statusLines = [entry for _, entry in self.workerProgress.items()]
                failed = []
                completed = []
                complete = 0
                total = 0
                for line in statusLines:
                    complete += line.get("completed",0)
                    total += line.get("total",0)
                    if line["status"] == "Failed":
                        failed.append(line)
                    elif line["status"] == "Completed":
                        completed.append(line)
                if len(failed) > 0:
                    print(f"Halting progress watch due to worker '{failed[0].get("pool","Bad Pull")}' failure.")
                    self.queue.append(-86) #will halt the queue
                    break
                elif len(completed) == nodeCount:
                    print("All workers have declared complete.")
                    self.queue.append(100) #backup in case the numbers below fail
                    break
                if total != 0:
                    percent = int((complete / total) * 100)
                    self.queue.append(percent)
                    if complete >= total:
                        break
            except Exception as e:
                print(f"Error watching database: {e}")
                print(traceback.format_exc())
                pass
            time.sleep(calculationPingTime * 0.01)
    def update_from_queue(self):
        if self.queue:
            while self.queue: #cycle through the queue options to get most up to date value. Breaks out if complete or halted
                val = self.queue.pop(0)
                if val in (-86,100):
                    break
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
                executor.submit(self.calcCompletion)
            elif val == -86:
                self.timer.stop()
                if self.cancel:
                    QMessageBox.warning(self,"Calculation Halted", "Calculations are being halted.")
                    self.cancel = False
                else:
                    QMessageBox.warning(self,"Calculation Failure", "A worker thread has failed. Calculations will not be properly completed.")
                self.pool.terminate()
                self.pool.join()
                gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
                
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
            calculations = [entry for entry in calculations if isinstance(entry,dict)]
            for row in calculations:
                for key in row.keys():
                    if key not in keys:
                        keys.append(key)
            save_to_db(self.db,"tranCalculations",calculations, keys=keys)
            try:
                apiPullTime = load_from_db(self.db,"history")[0]["lastImport"]
                save_to_db(self.db,None,None,query="UPDATE history SET [lastCalculation] = ?", inputs=(apiPullTime,), action="replace")
                self.lastImportLabel.setText(f"Last Data Calculation: {apiPullTime}")
            except:
                print("failed to update last calculation time")
            gui_queue.put(self.instantiateFilters)
            gui_queue.put( lambda: self.populate(self.calculationTable,calculations,keys = keys))
            gui_queue.put( lambda: self.buildReturnTable())
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
            print("Calculations complete.")
            self.workerProgress = {}
        except:
            gui_queue.put(lambda: self.calculationLoadingBox.setVisible(False))
            print(f"Error occured processing calculation results. Resetting... ")
            print(traceback.format_exc())
    def separateRowCode(self, label):
        header = re.sub(r'##\(.*\)##', '', label, flags=re.DOTALL)
        code = re.findall(r'##\(.*\)##', label, flags=re.DOTALL)[0]
        return header, code
    def headerSortClosed(self):
        self.populateReturnsTable(self.currentTableData)
    def orderColumns(self,keys, exceptions = []):
        mode = self.tableBtnGroup.checkedButton().text()
        if mode == "Monthly Table":
            dates = [datetime.strptime(k, "%B %Y") for k in keys]
            dates = sorted(dates, reverse=True)
            keys = [d.strftime("%B %Y") for d in dates]
        elif mode == "Complex Table":
            newOrder = ["NAV", "Commitment", "Unfunded","MTD","QTD","YTD"] + [f"{y}YR" for y in yearOptions] + ["ITD"]
            ordered = [h for h in newOrder if h in keys]
            ordered += [h for h in keys if h not in newOrder and h not in exceptions]
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

            exceptions = ["Return", "Ownership", "MDdenominator", "Monthly Gain"]
            col_keys = self.orderColumns(col_keys, exceptions=exceptions)
            if mode == "Complex Table":
                allKeys = col_keys.copy()
                allKeys.extend(exceptions) #all key options for the header selections
                self.headerSort.set_items(allKeys,[item for item in allKeys if item not in exceptions])
                self.headerSort.setEnabled(True)
            else:
                self.headerSort.setEnabled(False)
        else:
            col_keys = self.headerSort.popup.get_checked_sorted_items()
            self.headerSort.setEnabled(True)

        # 3) Resize & set horizontal headers (we no longer call setVerticalHeaderLabels)
        self.returnsTable.setRowCount(len(row_entries))
        self.returnsTable.setColumnCount(len(col_keys))
        self.returnsTable.setHorizontalHeaderLabels(col_keys)


        # 4) Populate each row
        for r, (fund_label, code, row_dict) in enumerate(row_entries):
            # pull & remove dataType for coloring
            dataType = row_dict.pop("dataType", "")

            startColor = (160, 160, 160)
            if dataType != "Total":
                depth      = code.count("::") if dataType != "Total Pool" else code.count("::") + 1
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
                        if c in percent_headers or (mode == "Monthly Table" and self.returnOutputType.currentText() in percent_headers):
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
