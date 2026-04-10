from datetime import datetime
from PyQt5.QtCore import Qt, QPoint, pyqtSignal
from PyQt5.QtGui import QBrush, QColor
from PyQt5.QtWidgets import (
    QRadioButton, QTableWidget, QTableWidgetItem, QWidget, QLineEdit, QVBoxLayout, QPushButton, QScrollArea,
    QCheckBox, QListWidget, QListWidgetItem, QDialog, QLabel,
    QHBoxLayout, QComboBox, QDateEdit, QApplication, QSpinBox
)
from dateutil.relativedelta import relativedelta
from scripts.commonValues import dataTimeStart
from scripts.loggingFuncs import attach_logging_to_class

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
    def __init__(self, parent=None, dispLib:dict = {'id2disp' : {}, 'disp2id' : {}}, singleSelect = False):
        super().__init__(parent)

        # ——— top line edit ———
        self.line_edit = ClickableLineEdit(self)
        self.line_edit.setReadOnly(True)
        self.line_edit.setPlaceholderText("Click to select…")
        self.line_edit.clicked.connect(self._togglePopup)
        self.hierarchy = False
        self.currentItems = []
        self.disp2id = dispLib['disp2id'].get
        self.id2disp = dispLib['id2disp'].get
        self.singleSelect = singleSelect

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
        sbText = self.popup.searchBar.text().lower()
        for cbKey in self._checkboxes.keys():
            cb = self._checkboxes[cbKey]
            cb.setVisible(sbText in cb.text().lower())
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
            self.popup.searchBar.setFocus()
    def addItems(self,items):
        for item in items:
            self.addItem(item)
    def addItem(self, text):
        text = self.id2disp(text,text) #put checkboxes to show the display version
        if text in self._checkboxes:
            return
        if self.singleSelect:
            cb = QRadioButton(text,self.popup)
            cb.clicked.connect(self._updateLine)
        else:
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
            text = self.disp2id(text,text) #check as id version
            cb.setChecked(text in items)
        self._updateLine()
    def setCheckedItem(self, item):
        for text, cb in self._checkboxes.items():
            text = self.disp2id(text,text) #check as id version
            if text == item:
                cb.setChecked(True)
        self._updateLine()
    def checkedItems(self):
        if self.hierarchy:
            return [self.disp2id(item,item) for item in self.currentItems]
        else:
            return [self.disp2id(t,t) for t, cb in self._checkboxes.items() if cb.isChecked()]

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
        sel = [self.id2disp(item,item) for item in sel.copy()]
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
            lines = [f"({i+1}) '{text}'" for i, text in enumerate(self.currentItems)]

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
        self.currentOrder = [c for c in items if c in checked_set]
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
        #self.list_widget.setCurrentItem(item)

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
        currOrder = self.get_checked_sorted_items()
        if currOrder != self.currentOrder: #only emit when actually changed
            self.currentOrder = currOrder
            self.popup_closed.emit()
        super().closeEvent(event)


class SortButtonWidget(QWidget):
    popup_closed = pyqtSignal(list)  # emits checked, sorted items

    def __init__(self, parent=None, btnName = 'Header Options'):
        super().__init__(parent)
        self.items = []
        self.checked_items = set()
        self.active = False
        self.exclusions = []

        self.button = QPushButton(btnName, self)
        self.button.clicked.connect(self.show_popup)

        layout = QVBoxLayout(self)
        layout.addWidget(self.button)
        layout.setContentsMargins(0, 0, 0, 0)

        self.popup = SortPopup(self.items, self.checked_items, self)
        self.popup.popup_closed.connect(self.on_popup_closed)
    def options(self):
        return self.items
    def add_item(self, item, checked=True):
        if item in self.exclusions:
            return
        self.items.append(item)
        if checked:
            self.checked_items.add(item)
        self.popup.set_items(self.items,self.checked_items)
        self.active = True

    def set_items(self, items, checked_items=None):
        self.items = list((item for item in items if item not in self.exclusions))
        self.checked_items = set(checked_items or items)
        self.popup.set_items(self.items,self.checked_items)
        self.active = True

    def show_popup(self):
        if self.popup.isVisible():
            self.popup.hide()
        else:
            self.popup.adjustSize()

            # 1. Get global position under the line edit
            pos = self.button.mapToGlobal(QPoint(0, 0))
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

    def on_popup_closed(self):
        if self.popup:
            self.items = self.popup.get_all_items()
            self.checked_items = self.popup.get_checked_items_set()
            #self.popup_closed.emit(self.popup.get_checked_sorted_items())
            self.popup.hide()

class simpleMonthSelector(QWidget):
    currentTextChanged = pyqtSignal()
    def __init__(self, parent = None, autoPopulate = False):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        self.monthSelect = QComboBox()
        self.yearSelect = QComboBox()
        self.changeLock = False
        layout.addWidget(self.monthSelect)
        layout.addWidget(self.yearSelect)
        self.monthSelect.currentTextChanged.connect(self.emitSignal)
        self.yearSelect.currentTextChanged.connect(self.emitSignal)
        if autoPopulate:
            start = dataTimeStart
            end = datetime.now() + relativedelta(months=3)
            index = start
            monthList = []
            while index < end:
                monthList.append(datetime.strftime(index,"%B %Y"))
                index += relativedelta(months=1)
            self.addItems(monthList)
            if len(monthList) > 5:
                self.setCurrentText(monthList[-5])
    def addItems(self, items):
        self.changeLock = True
        months = set()
        years = set()
        for item in items:
            month, year = item.split(" ")
            months.add(month)
            years.add(year)
        years.add(str(max([int(y) for y in years]) + 1))
        month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        self.monthSelect.addItems([m for m in month_order if m in months])
        self.yearSelect.addItems(sorted(years))
        self.changeLock = False
    def setCurrentText(self,text):
        self.changeLock = True
        [month,year] = text.split(" ")
        self.monthSelect.setCurrentText(month)
        self.yearSelect.setCurrentText(year)
        self.changeLock = False
    def currentText(self):
        month = self.monthSelect.currentText()
        year = self.yearSelect.currentText()
        joined = " ".join([month,year])
        return joined
    def emitSignal(self):
        if not self.changeLock:
            self.currentTextChanged.emit()
class CheckboxIntInputWidget(QWidget):
    valChange = pyqtSignal()
    def __init__(self, dispString, defaultInt, intStr, parent=None):
        super().__init__(parent)
        # Create horizontal layout for one line
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Checkbox with text following it
        self.checkbox = QCheckBox(dispString, self)
        self.checkbox.clicked.connect(self.emitChange)
        
        # Integer input box
        self.intInput = QSpinBox(self)
        self.intInput.setValue(defaultInt)
        self.intInput.setMinimum(0)  # Minimum integer value
        self.intInput.setMaximum(2147483647)    # Maximum integer value
        self.intInput.setMaximumWidth(50)
        self.intInput.valueChanged.connect(self.emitChange,True)
        # Text label to the right of integer input
        self.intLabel = QLabel(intStr, self)
        
        # Add widgets to layout in order
        layout.addWidget(self.checkbox)
        layout.addWidget(self.intInput)
        layout.addWidget(self.intLabel)
        layout.addStretch()  # Push everything to the left
        
        self.setLayout(layout)
    def emitChange(self, intChange = False):
        if intChange and not self.checkbox.isChecked():
            return #only emit for change if the widget is even active
        self.valChange.emit()
    def setChecked(self,status : bool):
        self.checkbox.setChecked(status)
    def setInt(self,val : int):
        self.intInput.setValue(val)
    def isChecked(self):
        return self.checkbox.isChecked()
    def getStatus(self):
        """
        Retrieve both the checkbox and integer status in one call.
        Returns a tuple: (is_checked: bool, integer_value: int)
        """
        return (self.checkbox.isChecked(), self.intInput.value())

class EditableDBTableWidget(QTableWidget):
    """
    Table widget for displaying and editing database-backed tabular data.

    Args:
        db_manager (DatabaseManager): The database manager for data updates.
        table_name (str): The name of the table in the database.
        month (str): Month string or identifier for filtering rows.
        headers (list): List of headers/column names to display.
        data_rows (list of dict): Each row is a dict with keys matching headers.

    When a cell is edited and the new value is different, calls:
        db_manager.update_table_row(table_name, original_row_dict, updated_row_dict)
    The table updates the cell only if the update returns True.
    """

    def __init__(self, db_manager, table_name, _, headerDict, data_rows, constants = {}, XYmode = [], parent=None, total = False):
        super().__init__(parent)
        self.db_manager = db_manager
        self.table_name = table_name
        self.constants = constants
        self.XYmode = XYmode if XYmode else []
        self.xy_active = len(self.XYmode) == 3
        self.total = total
        headers = list(headerDict.keys()) if not self.xy_active else []
        self.headers = headers
        self.headerDict = headerDict
        self.data_rows = data_rows
        self.totalRow = 0
        self._original_rows = []  # Track original dicts for change comparison
        self._xy_lookup = {}
        self._xy_rows = []
        self._xy_cols = []
        self._xy_value_type = str
        
        headerConversions = {'team' : 'Team', 'share' : 'Share (%)', 'teamValue' : 'Team Value',
                             'debt' : 'Debt', 'equity' : 'Equity' , 'ownership' : 'Ownership (%)'}
        totalHeaderLabels = [] if not total else ['Total',]
        if self.xy_active:
            x_key, y_key, value_key = self.XYmode
            row_labels = []
            col_labels = []
            for row_dict in self.data_rows:
                x_val = row_dict.get(x_key)
                y_val = row_dict.get(y_key)
                if x_val not in row_labels:
                    row_labels.append(x_val)
                if y_val not in col_labels:
                    col_labels.append(y_val)
                self._xy_lookup[(x_val, y_val)] = row_dict.copy()
                curr_val = row_dict.get(value_key)
                if curr_val is not None:
                    self._xy_value_type = type(curr_val)
            self._xy_rows = row_labels
            self._xy_cols = col_labels
            self.setColumnCount(len(self._xy_cols))
            self.setRowCount(len(self._xy_rows) + len(totalHeaderLabels))
            y_headers = [headerConversions.get(h, h) for h in self._xy_cols]
            self.setHorizontalHeaderLabels(y_headers)
            self.setVerticalHeaderLabels([h for h in row_labels] + totalHeaderLabels)
        else:
            self.setColumnCount(len(headers))
            self.setRowCount(len(data_rows) + len(totalHeaderLabels))
            self.setHorizontalHeaderLabels([headerConversions.get(h,h) for h in headers])
        self.setEditTriggers(QTableWidget.DoubleClicked | QTableWidget.SelectedClicked | QTableWidget.EditKeyPressed)

        self.populate_table()
        self.cellChanged.connect(self.on_cell_changed)
        self._editing = False  # Prevent recursive triggers

    def populate_table(self):
        """Populate table with current data_rows and build _original_rows."""
        self._original_rows = []
        row_idx = 0
        if self.xy_active:
            x_key, y_key, value_key = self.XYmode
            for row_idx, x_val in enumerate(self._xy_rows):
                for col_offset, y_val in enumerate(self._xy_cols, start=0):
                    source_row = self._xy_lookup.get((x_val, y_val), {})
                    item_val = source_row.get(value_key, "")
                    table_item = QTableWidgetItem("" if item_val is None else str(item_val))
                    table_item.setBackground(QColor("#FFFFFF"))
                    table_item.setData(Qt.UserRole, self.headerDict.get(value_key, self._xy_value_type))
                    self.setItem(row_idx, col_offset, table_item)
                    base_row = source_row.copy()
                    if x_key not in base_row:
                        base_row[x_key] = x_val
                    if y_key not in base_row:
                        base_row[y_key] = y_val
                    if value_key not in base_row:
                        base_row[value_key] = item_val
                    self._original_rows.append(base_row)
        else:
            for row_idx, row_dict in enumerate(self.data_rows):
                self._original_rows.append(row_dict.copy())
                for col_idx, header in enumerate(self.headers):
                    item_val = row_dict.get(header, "")
                    if self.headerDict.get(header) == bool:
                        item_val = 'True' if item_val == 1 else 'False'
                    table_item = QTableWidgetItem(str(item_val))
                    table_item.setBackground(QColor("#FFFFFF"))
                    table_item.setData(Qt.UserRole, self.headerDict.get(header,str))  # Save type info for validation
                    self.setItem(row_idx, col_idx, table_item)
        self.totalRow = row_idx + 1
        if self.total:
            self.setTotals()
    def setTotals(self):
        self._editing = True
        cols = self.columnCount()
        rows = self.rowCount() - 1
        for c in range(cols):
            colSum = 0
            for r in range(rows):
                val = self.item(r,c).text()
                dt = self.item(r,c).data(Qt.UserRole)
                if dt in (int,float):
                    val = float(val)
                    colSum += val
            totalItem = QTableWidgetItem(str(colSum))
            if colSum == 100:
                totalItem.setBackground(QColor("#99ff99"))
            else:
                totalItem.setBackground(QColor("#ff6666"))
            totalItem.setFlags(totalItem.flags() & ~Qt.ItemIsEditable)
            self.setItem(self.totalRow, c, totalItem)
        self._editing = False
    def on_cell_changed(self, row, column):
        """
        Called when a cell's value is changed by the user.
        Compares with original data and calls db_manager.update_table_row if a real change is made.
        If not successful, resets item to old value.
        """
        if self._editing:
            return  # Avoid recursive triggers
        try:
            if self.xy_active:
                x_key, y_key, value_key = self.XYmode
                header = value_key
                x_val = self._xy_rows[row]
                y_val = self._xy_cols[column]
                source_row = self._xy_lookup.get((x_val, y_val), {})
                old_row = source_row.copy()
                if x_key not in old_row:
                    old_row[x_key] = x_val
                if y_key not in old_row:
                    old_row[y_key] = y_val
                if value_key not in old_row:
                    old_row[value_key] = ""
                new_row = old_row.copy()
            else:
                header = self.headers[column]
                old_row = self._original_rows[row].copy()
                new_row = self._row_to_dict(row)
            # type conversion: get expected type from Qt.UserRole
            item = self.item(row, column)
            orig_type = item.data(Qt.UserRole)
            new_val_raw = self.item(row, column).text()

            # Try to convert to original type, fallback to string
            try:
                if orig_type is int:
                    new_val = int(new_val_raw)
                elif orig_type is float:
                    new_val = float(new_val_raw)
                elif orig_type is bool:
                    new_val = new_val_raw.lower() in ("true", "1", "yes")
                else:
                    new_val = new_val_raw
            except Exception:
                #Don't allow bad data types. Revert value and mark failed
                self._editing = True
                self.item(row, column).setText(str(old_row.get(header, "")))
                self.item(row, column).setBackground(QBrush(QColor('#FFB4B4')))
                self._editing = False
                return

            # Only proceed if change is real
            if old_row.get(header, "") != new_val:
                new_row[header] = new_val
                success = False
                if hasattr(self.db_manager, "updateReportData"):  # Defensive for not-yet-created function
                    dbOld = old_row.copy()
                    for k,v in self.constants.items(): #put the hidden col values that don't change
                        dbOld[k] = v
                    dbNew = new_row.copy()
                    for k,v in self.constants.items():
                        dbNew[k] = v
                    success = self.db_manager.updateReportData(
                        self.table_name, dbOld, dbNew
                    )
                if success:
                    if self.xy_active:
                        self._xy_lookup[(x_val, y_val)] = new_row.copy()
                    else:
                        self._original_rows[row] = new_row.copy()
                    # Optionally, visually mark success
                    self.item(row, column).setBackground(QBrush(QColor('#FFFFFF')))
                    self.setTotals()
                else:
                    # Revert value and mark failed
                    self._editing = True
                    self.item(row, column).setText(str(old_row.get(header, "")))
                    self.item(row, column).setBackground(QBrush(QColor('#FFB4B4')))
                    self._editing = False
            else:
                # No real change, silently ignore/reset background
                self.item(row, column).setBackground(QBrush())
        except Exception as e:
            # Fallback: if error, revert cell
            print(f'Table edit failed: {e.args}')
            if self.xy_active:
                x_key, y_key, value_key = self.XYmode
                if column == 0:
                    return
                x_val = self._xy_rows[row]
                y_val = self._xy_cols[column]
                old_row = self._xy_lookup.get((x_val, y_val), {})
                header = value_key
            else:
                old_row = self._original_rows[row]
                header = self.headers[column]
            self._editing = True
            self.item(row, column).setText(str(old_row.get(header, "")))
            self._editing = False

    def _row_to_dict(self, row_idx):
        """Build a dictionary representing the table row at row_idx."""
        row_dict = {}
        for col_idx, header in enumerate(self.headers):
            item = self.item(row_idx, col_idx)
            val = item.text() if item else ""
            # Optionally, try to infer type from original
            orig_type = item.data(Qt.UserRole) if item else str
            try:
                if orig_type is int:
                    val = int(val)
                elif orig_type is float:
                    val = float(val)
                elif orig_type is bool:
                    val = val.lower() in ("true", "1", "yes")
            except Exception:
                pass
            row_dict[header] = val
        return row_dict