from scripts.importList import *
from scripts.loggingFuncs import attach_logging_to_class
from scripts.commonValues import *

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
            self.popup.searchBar.setFocus()
    def addItems(self,items):
        for item in items:
            self.addItem(item)
    def addItem(self, text):
        text = displayLinks.get(text,text) #put checkboxes to show the display version
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
            text = displayLinks.get(text,text) # check as display version
            if text in items:
                cb.setChecked(True)
        self._updateLine()
    def setCheckedItem(self, item):
        for text, cb in self._checkboxes.items():
            text = displayLinks.get(text,text) # check as display version
            if text == item:
                cb.setChecked(True)
        self._updateLine()

    def checkedItems(self):
        if self.hierarchy:
            items = []
            for item in self.currentItems:
                items.append(displayLinks.get(item,item)) #revert to normal version for output
            return items
        else:
            return [displayLinks.get(t,t) for t, cb in self._checkboxes.items() if cb.isChecked()]

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
        sel = [displayLinks.get(item,item) for item in sel.copy()]
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
        self.popup.popup_closed.connect(self.on_popup_closed)

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
    def __init__(self, parent = None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        self.monthSelect = QComboBox()
        self.yearSelect = QComboBox()
        self.changeLock = False
        layout.addWidget(self.monthSelect)
        layout.addWidget(self.yearSelect)
        self.monthSelect.currentTextChanged.connect(self.emitSignal)
        self.yearSelect.currentTextChanged.connect(self.emitSignal)
    def addItems(self, items):
        self.changeLock = True
        months = set()
        years = set()
        for item in items:
            month, year = item.split(" ")
            months.add(month)
            years.add(year)
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
