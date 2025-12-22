from PyQt5.QtCore import Qt, QModelIndex, QTimer, QAbstractTableModel
from PyQt5.QtGui import QBrush, QColor
from PyQt5.QtWidgets import  QTableWidget, QTableWidgetItem, QHeaderView
from scripts.loggingFuncs import attach_logging_to_class
@attach_logging_to_class
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
