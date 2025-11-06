from classes.DatabaseManager import load_from_db, save_to_db
from scripts.importList import *
from scripts.loggingFuncs import attach_logging_to_class
from classes.widgetClasses import *
from scripts.basicFunctions import *
from scripts.exportTableToExcel import exportTableToExcel

class linkBenchmarksWindow(QWidget):
    def __init__(self, parent=None, flags=Qt.WindowFlags(), parentSource=None):
        super().__init__(parent, flags)
        self.parent = parentSource
        self.setWindowTitle("Link Benchmarks")
        self.resize(800, 500)
        self.setStyleSheet(self.parent.appStyle)
        self.setObjectName("mainPage")
        self._benchmarks = [{},]
        self._links = []
        self.asset_levels = [("Level 1", 1), ("Level 2", 2), ("Level 3", 3)]
        self.selected_asset_level = None
        self.selected_asset = None
        self.selected_benchmark = None
        self.init_ui()

    def init_ui(self):
        try:
            mainLayout = QVBoxLayout(self)
            splitter = QSplitter(Qt.Horizontal)

            # --- Left: Benchmark Links Table ---
            leftWidget = QWidget()
            leftLayout = QVBoxLayout(leftWidget)
            self.linksTable = QTableWidget()
            self.linksTable.setColumnCount(4)
            self.linksTable.setHorizontalHeaderLabels(['Benchmark', 'Asset', 'Level', 'Delete'])
            self.linksTable.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            leftLayout.addWidget(QLabel("Current Benchmark Links:"))
            leftLayout.addWidget(self.linksTable)
            splitter.addWidget(leftWidget)

            # --- Right Side: Vertically Split ---
            rightWidget = QWidget()
            rightVSplitter = QSplitter(Qt.Vertical)

            # --- Top half: Add Benchmark Link ---
            topWidget = QWidget()
            topLayout = QVBoxLayout(topWidget)
            topLayout.addWidget(QLabel("Add Benchmark Link:"))

            # Asset Class Level ComboBox
            self.assetLevelCombo = QComboBox()
            self.assetLevelCombo.addItem("")  # blank/default
            for label, num in self.asset_levels:
                self.assetLevelCombo.addItem(label, num)
            self.assetLevelCombo.currentIndexChanged.connect(self.updateAssetCombo)
            topLayout.addWidget(QLabel("Asset Class Level:"))
            topLayout.addWidget(self.assetLevelCombo)

            # Asset ComboBox - will be populated depending on above
            self.assetCombo = QComboBox()
            self.assetCombo.addItem("")  # blank
            topLayout.addWidget(QLabel("Asset:"))
            topLayout.addWidget(self.assetCombo)

            # Benchmark ComboBox - will be populated
            self.benchmarkCombo = QComboBox()
            self.benchmarkCombo.addItem("")  # blank
            topLayout.addWidget(QLabel("Benchmark:"))
            topLayout.addWidget(self.benchmarkCombo)

            # Confirm Button
            self.confirmBtn = QPushButton("Confirm Link")
            self.confirmBtn.clicked.connect(self.addBenchmarkLink)
            topLayout.addWidget(self.confirmBtn)
            topLayout.addStretch()

            # --- Bottom half: Table Element Benchmark Link ---
            bottomWidget = QWidget()
            bottomLayout = QVBoxLayout(bottomWidget)
            bottomLayout.addWidget(QLabel("Add Table Element Benchmark Link:"))

            # Table Element ComboBox
            self.tableElementCombo = QComboBox()
            self.tableElementCombo.addItem("")  # blank (populate later)
            bottomLayout.addWidget(QLabel("Total Portfolio or Family Branch:"))
            bottomLayout.addWidget(self.tableElementCombo)

            # Table Benchmark ComboBox
            self.tableBenchmarkCombo = QComboBox()
            self.tableBenchmarkCombo.addItem("")  # blank (populate later)
            bottomLayout.addWidget(QLabel("Benchmark:"))
            bottomLayout.addWidget(self.tableBenchmarkCombo)

            # Confirm Button
            self.tableConfirmBtn = QPushButton("Confirm Table Element Link")
            self.tableConfirmBtn.clicked.connect(self.addTableElementBenchmarkLink) # Connect to your logic
            bottomLayout.addWidget(self.tableConfirmBtn)
            bottomLayout.addStretch()

            rightVSplitter.addWidget(topWidget)
            rightVSplitter.addWidget(bottomWidget)
            splitter.addWidget(rightVSplitter)

            mainLayout.addWidget(splitter)
            self.setLayout(mainLayout)

            self.refreshBenchmarks()
            self.refreshLinks()
            self.updateTableElementCombo()
        except Exception as e:
            print(f"Error initializing benchmark window: {e} {e.args}")
            logging.error(f"Error initializing benchmark window: {e} {e.args}")
            QMessageBox.critical(self.parent, "Error initializing benchmark window", f"Error initializing benchmark window: {e} {e.args}")

    def refreshLinks(self):
        links = self.parent.db.fetchBenchmarkLinks()
        self._links = links
        self.linksTable.setRowCount(len(links))
        for row, link in enumerate(links):
            self.linksTable.setItem(row, 0, QTableWidgetItem(str(link.get("benchmark", ""))))
            self.linksTable.setItem(row, 1, QTableWidgetItem(str(link.get("asset", ""))))
            self.linksTable.setItem(row, 2, QTableWidgetItem(str(link.get("assetLevel", ""))))
            # Delete button
            btn = QPushButton("Delete")
            btn.clicked.connect(lambda _, r=row: self.deleteLink(r))
            self.linksTable.setCellWidget(row, 3, btn)

    def refreshBenchmarks(self):
        # try to fetch all benchmarks for use in the Benchmark combobox
        try:
            if hasattr(self.parent.db, "fetchBenchmarks"):
                benchmarks = self.parent.db.fetchBenchmarks()
            else:
                benchmarks = {}  # fallback - maybe empty or stub
        except Exception as e:
            print(f"Failed to fetch benchmarks: {e}")
            benchmarks = []
        self._benchmarks = benchmarks
        self.updateBenchmarkCombo()

    def updateBenchmarkCombo(self):
        for combo in (self.benchmarkCombo, self.tableBenchmarkCombo):
            combo.clear()
            combo.addItem("")
            combo.addItems(sorted([b["benchmark"] if isinstance(b,dict) else str(b) for b in self._benchmarks]))
    def updateTableElementCombo(self):
        self.tableElementCombo.clear()
        opts = ["","Total"]
        opts.extend([famBranch for famBranch in self.parent.fullLevelOptions.get("Family Branch",[]) if famBranch])
        self.tableElementCombo.addItems(opts)
    def updateAssetCombo(self):
        self.assetCombo.clear()
        self.assetCombo.addItem("")
        asset_level_num = self.assetLevelCombo.currentData()
        asset_key = None
        if asset_level_num == 1:
            asset_key = "assetClass"
        elif asset_level_num == 2:
            asset_key = "subAssetClass"
        elif asset_level_num == 3:
            asset_key = "subAssetSleeve"
        all_opts = getattr(self.parent, "fullLevelOptions", {})
        options = all_opts.get(asset_key, [])
        # Remove duplicates and blank/None values
        assets = sorted({opt for opt in options if opt not in (None,"", "None")})
        self.assetCombo.addItems(assets)

    def deleteLink(self, row):
        if row < 0 or row >= len(self._links):
            return
        link = self._links[row]
        try:
            with self.parent.lock:
                cursor = self.parent.db._conn.cursor()
                cursor.execute(
                    "DELETE FROM benchmarkLinks WHERE benchmark = ? AND asset = ? AND assetLevel = ?",
                    (link["benchmark"], link["asset"], link["assetLevel"])
                )
                self.parent.db._conn.commit()
            self.parent.db.fetchBenchmarkLinks(update=True)
            QMessageBox.information(self, "Success", f"Deleted link: {link['benchmark']} to {link['asset']} at level {link['assetLevel']}.")
        except Exception as e:
            QMessageBox.warning(self, "Delete Error", f"Error deleting link: {e}")
        self.parent.db.fetchBenchmarkLinks(update=True)
        self.refreshLinks()
        self.parent.buildReturnTable()
    def addTableElementBenchmarkLink(self):
        asset = self.tableElementCombo.currentText()
        levelIdx = 0 if asset == "Total" else -1
        benchmark = self.tableBenchmarkCombo.currentText().strip()
        if asset == "" or benchmark == "":
            QMessageBox.warning(self, "Incomplete", "Please select asset and benchmark.")
            return
        self.updateLink(levelIdx,asset,benchmark)
    def addBenchmarkLink(self):
        # Get selections
        level_idx = self.assetLevelCombo.currentIndex()
        asset = self.assetCombo.currentText().strip()
        benchmark = self.benchmarkCombo.currentText().strip()
        if level_idx <= 0 or not asset or not benchmark:
            QMessageBox.warning(self, "Incomplete", "Please select asset class level, asset, and benchmark.")
            return
        asset_level = self.assetLevelCombo.currentData()
        self.updateLink(asset_level,asset,benchmark)
        
    def updateLink(self, level,asset,benchmark):
        try:
            with self.parent.lock:
                cursor = self.parent.db._conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO benchmarkLinks (benchmark, asset, assetLevel) VALUES (?, ?, ?)",
                    (benchmark, asset, level)
                )
                self.parent.db._conn.commit()
            QMessageBox.information(self, "Success", f"Linked {benchmark} to {asset} at level {level}.")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to save link: {e}")
        self.parent.db.fetchBenchmarkLinks(update=True)
        self.refreshLinks()
        self.parent.buildReturnTable()


class exportWindow(QWidget):
    def __init__(self, parent=None, flags=Qt.WindowFlags(), parentSource=None):
        super().__init__(parent, flags)
        self.parent = parentSource
        self.setWindowTitle("Export Data")
        self.resize(600, 400)
        self.setStyleSheet(self.parent.appStyle)
        self.setObjectName("mainPage")

        # Layouts
        main_layout = QVBoxLayout(self)
        form_layout = QFormLayout()
        self.filter_boxes = {}
        self.filter_labels = {}

        # --- Filter ComboBoxes for each filterOption ---
        self.filterOptions = getattr(self.parent, "filterOptions", [
            {"key": "Investor", "name": "Investor"},
            {"key": "Pool", "name": "Pool"},
            {"key": "Fund", "name": "Fund"},
            {"key": "assetClass", "name": "Asset Class"},
            {"key": "subAssetClass", "name": "Sub Asset Class"},
            {"key": "Classification", "name": "Classification"},
        ])
        # Use parent's lock if available
        self.lock = getattr(self.parent, "lock", None)

        # Query unique options for each filter from the calculations table
        with self.parent.lock:
            dbPath = getattr(self.parent, "dbPath", DATABASE_PATH)
            conn = sqlite3.connect(dbPath)
            cur = conn.cursor()
            for f in self.filterOptions:
                key = f["key"]
                name = f["name"]
                combo = QComboBox()
                combo.addItem("")  # blank for optional
                try:
                    cur.execute(f"SELECT DISTINCT [{key}] FROM calculations")
                    options = [row[0] for row in cur.fetchall() if row[0] not in (None, "", "None")]
                    options = sorted(set(options))
                    combo.addItems(options)
                except Exception as e:
                    print(f"Error loading filter options for {key}: {e}")
                self.filter_boxes[key] = combo
                self.filter_labels[key] = name
                form_layout.addRow(name + ":", combo)
            conn.close()

        # --- Date selectors ---
        date_layout = QHBoxLayout()
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.start_date_edit.setDate(QDate.currentDate())
        self.start_date_edit.setSpecialValueText("")  # blank for optional
        self.start_date_edit.setDateRange(QDate(1990, 1, 1), QDate(2100, 12, 31))
        self.start_date_edit.setDate(QDate(2000, 1, 1))
        self.start_date_edit.setMinimumWidth(120)
        self.start_date_edit.setDate(QDate())  # blank

        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.end_date_edit.setDate(QDate.currentDate())
        self.end_date_edit.setSpecialValueText("")  # blank for optional
        self.end_date_edit.setDateRange(QDate(1900, 1, 1), QDate(2100, 12, 31))
        self.end_date_edit.setDate(QDate())  # blank
        self.end_date_edit.setMinimumWidth(120)

        date_layout.addWidget(QLabel("Start Date:"))
        date_layout.addWidget(self.start_date_edit)
        date_layout.addSpacing(20)
        date_layout.addWidget(QLabel("End Date:"))
        date_layout.addWidget(self.end_date_edit)
        form_layout.addRow(date_layout)

        # --- Confirm Button ---
        self.confirm_btn = QPushButton("Export to Excel")
        self.confirm_btn.clicked.connect(self.export_to_excel)
        main_layout.addLayout(form_layout)
        main_layout.addWidget(self.confirm_btn, alignment=Qt.AlignRight)

    def export_to_excel(self):
        #Export to excel function for the calculations view page
        # Build WHERE clause from filters
        filters = []
        values = []
        for key, combo in self.filter_boxes.items():
            val = combo.currentText()
            if val:
                filters.append(f"[{key}] = ?")
                values.append(val)
        # Date filters
        start_date = self.start_date_edit.date()
        end_date = self.end_date_edit.date()
        if start_date.isValid():
            filters.append("[dateTime] >= ?")
            # Convert QDate to Python datetime, then format as string
            values.append(start_date.toPyDate().strftime("%Y-%m-%d %H:%M:%S"))
        if end_date.isValid():
            filters.append("[dateTime] <= ?")
            values.append(end_date.toPyDate().strftime("%Y-%m-%d %H:%M:%S"))

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        # Query the database
        with self.parent.lock:
            dbPath = getattr(self.parent, "dbPath", DATABASE_PATH)
            conn = sqlite3.connect(dbPath)
            cur = conn.cursor()
            try:
                cur.execute("PRAGMA table_info(calculations)")
                columns = [row[1] for row in cur.fetchall()]
                sql = f"SELECT * FROM calculations {where_clause}"
                print(sql)
                print(tuple(values))
                cur.execute(sql, tuple(values))
                rows = cur.fetchall()
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to query database: {e}")
                conn.close()
                return

        if not rows:
            QMessageBox.information(self, "No Data", "No data found for the selected filters.")
            conn.close()
            return

        # Prompt user for file path
        path, _ = QFileDialog.getSaveFileName(self, "Save as…", "", "Excel Files (*.xlsx)")
        if not path:
            conn.close()
            return
        if not path.lower().endswith(".xlsx"):
            path += ".xlsx"

        # Write to Excel
        try:
            df = pd.DataFrame(rows, columns=columns)

            if os.path.exists(path):
                wb = load_workbook(path)
                # Generate a unique sheet name
                base_name = "Export"
                i = 1
                while True:
                    sheet_name = f"{base_name}{i}"
                    if sheet_name not in wb.sheetnames:
                        break
                    i += 1
                # Write DataFrame to the new sheet
                ws = wb.create_sheet(sheet_name)
                for col_idx, col_name in enumerate(df.columns, 1):
                    ws.cell(row=1, column=col_idx, value=col_name)
                for row_idx, row in enumerate(df.itertuples(index=False), 2):
                    for col_idx, value in enumerate(row, 1):
                        ws.cell(row=row_idx, column=col_idx, value=value)
                wb.save(path)
            else:
                df.to_excel(path, index=False)

            QMessageBox.information(self, "Success", f"Data exported to {path}")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to export to Excel: {e}")
        finally:
            conn.close()

class displayWindow(QWidget):
    def __init__(self, parent=None, flags=Qt.WindowFlags(), parentSource = None, text = "", title=""):
        super().__init__(parent, flags)
        self.setWindowTitle(title)
        self.parent = parentSource
        if self.parent:
            self.setObjectName("mainPage")
            self.setStyleSheet(self.parent.appStyle)
        #self.resize(1000, 600)

        # Layout and table
        layout = QVBoxLayout(self)
        
        scrollArea = QScrollArea(self) #make a scroll area with a layout inside containing a widget of the text that is styled to the color theme
        textContainer = QWidget()
        textContainer.setObjectName("subPanel")
        textLayout = QVBoxLayout()
        textLayout.addWidget(QLabel(text))
        textContainer.setLayout(textLayout)
        scrollArea.setWidget(textContainer)
        layout.addWidget(scrollArea)

@attach_logging_to_class
class underlyingDataWindow(QWidget):
    """
    A window that loads data from four database sources in the parent,
    merges and sorts it by dateTime, and displays it in a QTableWidget
    with a unified set of columns.
    """
    def __init__(self, parent=None, flags=Qt.WindowFlags(), parentSource = None, db = None):
        super().__init__(parent, flags)
        self.parent = parentSource
        self.setWindowTitle("Underlying Data Viewer")
        self.resize(1000, 600)
        self.db = db #only input from transactionApp
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

    def exportTable(self, *_):
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


                if os.path.exists(path):
                    wb = load_workbook(path)
                    # Create a unique sheet name for export
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

                rowStart = 1
                for idx, colname in enumerate(all_cols, start=1):
                    ws.cell(row=rowStart, column=idx, value=colname)


                # 7) populate rows
                for r, row_dict in enumerate(data, start=rowStart + 1):        
                    # data cells with proper formatting
                    for c, colname in enumerate(all_cols, start=1):
                        val = row_dict.get(colname, None)
                        try: #make numerical format if possible
                            val = float(val)
                        except:
                            pass
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
            elif selection in ("ITD","IRR ITD"):
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
        if self.db:
            highTables = {"transactions_high" : tranStart}
            lowTables = {"transactions_low": tranStart}
        else:
            highTables = {"positions_high": accountStart,"transactions_high" : tranStart}
            lowTables = {"positions_low": accountStart,"transactions_low": tranStart}
        all_rows = []
        if not self.db and (self.parent.filterDict["Investor"].checkedItems() != [] or self.parent.filterDict["Family Branch"].checkedItems() != []): #investor to pool level entries
            for idx, table in enumerate(highTables.keys()):
                query = "WHERE"
                inputs = []
                for hierIdx, tier in enumerate(hier):
                    if tier == "hiddenLayer":
                        continue #hidden layer should not affect the query
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
                    rows = load_from_db(self.parent,table,query.removesuffix("AND") + " AND [Date] BETWEEN ? AND ?", tuple(inputs))
                except Exception as e:
                    print(f"Error in call : {e} ; {e.args}")
                    rows = []
                for row in rows or []:
                    row['_source'] = table
                    all_rows.append(row)
        elif self.db:
            for idx, table in enumerate(highTables.keys()):
                query = "WHERE"
                inputs = []
                for hierIdx, tier in enumerate(hier):
                    if tier == "hiddenLayer":
                        continue #hidden layer should not affect the query
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
                    rows = load_from_db(self.parent,table,query.removesuffix("AND") + " AND [Date] BETWEEN ? AND ?", tuple(inputs))
                except Exception as e:
                    print(f"Error in call : {e} ; {e.args}")
                    rows = []
                for row in rows or []:
                    row['_source'] = table
                    all_rows.append(row)
        for idx, table in enumerate(lowTables.keys()):
            query = "WHERE"
            inputs = []
            for hierIdx, tier in enumerate(hier): #iterate through each tier down to selection
                if tier == "hiddenLayer":
                        continue #hidden layer should not affect the query
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
                                placeholders = ','.join('?' for _ in self.parent.sleeveFundLinks.get(sleeve)) 
                                query += f" [Target name] in ({placeholders}) AND"
                                inputs.extend(self.parent.sleeveFundLinks.get(sleeve))
                            else:
                                print("Failed to find subAssetSleeve")
                    else:
                        placeholders = ','.join('?' for _ in filterSelections) 
                        query += f" [{dynName}] in ({placeholders}) AND"
                        inputs.extend(filterSelections)
            inputs.extend([lowTables[table],allEnd])
            try:
                rows = load_from_db(self.parent,table,query.removesuffix("AND") + " AND [Date] BETWEEN ? AND ?", tuple(inputs))
            except Exception as e:
                print(f"Error in call : {e}; {e.args}")
                rows = []
            for row in rows or []:
                row['_source'] = table
                all_rows.append(row) 
        self.allData = all_rows

        if self.db:
            #build organized differences by pool/investor versus transaction type
            diffTableDict = { "Total" : {"Transaction Type" : "Total", "Pool Cashflow" : 0, "Investor Cashflow" : 0}} 
            for transaction in all_rows: #build dict for easy sorting
                if transaction.get("CashFlowSys") not in (None,"None"):
                    tranType = transaction.get("TransactionType")
                    if tranType not in diffTableDict:
                        diffTableDict[tranType] = {"Transaction Type" : tranType, "Pool Cashflow" : 0, "Investor Cashflow" : 0}
                    if transaction.get("_source") == "transactions_low":
                        diffTableDict[tranType]["Pool Cashflow"] += float(transaction.get("CashFlowSys"))
                        diffTableDict["Total"]["Pool Cashflow"] += float(transaction.get("CashFlowSys"))
                    elif transaction.get("_source") == "transactions_high":
                        diffTableDict[tranType]["Investor Cashflow"] += float(transaction.get("CashFlowSys"))
                        diffTableDict["Total"]["Investor Cashflow"] += float(transaction.get("CashFlowSys"))
                    
            diffTable = []
            for tranType in diffTableDict: #calculate differences and put in list of dicts for table
                if tranType != "Total":
                    diffTableDict[tranType]["Difference"] = diffTableDict.get(tranType).get("Pool Cashflow") - diffTableDict.get(tranType).get("Investor Cashflow")
                    diffTable.append(diffTableDict.get(tranType))
            diffTableDict["Total"]["Difference"] = diffTableDict.get("Total").get("Pool Cashflow") - diffTableDict.get("Total").get("Investor Cashflow")
            diffTable.append(diffTableDict.get("Total"))
            diffHeaders = ["Transaction Type", "Pool Cashflow", "Investor Cashflow", "Difference"]
            self.parent.openTableWindow(diffTable, name = f"Transaction types for {hier} in {selectedMonth}", headers = diffHeaders)

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

@attach_logging_to_class
class tableWindow(QWidget):
    """
    A window that loads data from four database sources in the parent,
    merges and sorts it by dateTime, and displays it in a QTableWidget
    with a unified set of columns.
    """
    def __init__(self, parent=None, flags=Qt.WindowFlags(), parentSource = None, all_rows = [], table = "", headers = None):
        super().__init__(parent, flags)
        self.parent = parentSource
        self.setWindowTitle(f"New data in {table}")
        self.resize(1000, 600)

        # Layout and table
        layout = QVBoxLayout(self)
        self.excelBtn = QPushButton("Export to Excel")
        self.excelBtn.clicked.connect(self.export)
        layout.addWidget(self.excelBtn)
        self.table = QTableWidget(self)
        self.table.setSortingEnabled(True)
        layout.addWidget(self.table)

        self.rows = all_rows

        # 4) Collect the union of all column keys
        if headers is None:
            all_cols = set()
            for row in all_rows:
                all_cols.update(row.keys())
            all_cols = list(all_cols)
        else:
            all_cols = headers
        self.headers = all_cols
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
    def export(self,*_):
        exportTableToExcel(self,self.rows,self.headers)
