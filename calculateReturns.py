import sys
import os
import json
import subprocess
import sqlite3
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from PyQt5.QtWidgets import (
    QApplication, QWidget, QStackedWidget, QVBoxLayout,
    QLabel, QLineEdit, QPushButton, QFormLayout,
    QRadioButton, QButtonGroup, QComboBox, QHBoxLayout,
    QTableWidget, QTableWidgetItem
)

# Determine assets path, works in PyInstaller bundle or script
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(BASE_DIR, 'assets')
TIMESTAMP_FILE = os.path.join(ASSETS_DIR, 'last_update.txt')
DB_TRANSACTIONS = os.path.join(ASSETS_DIR, 'transactions.db')
DB_ACCOUNTS = os.path.join(ASSETS_DIR, 'accounts.db')

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

        # last update indicator
        self.last_update_label = QLabel(self.load_last_timestamp() or 'Never updated')
        form.addRow('Last Update:', self.last_update_label)

        # refresh button
        btn_update = QPushButton('Refresh Data')
        btn_update.clicked.connect(self.refresh_data)
        form.addRow(btn_update)

        # navigation to results (loads from DB)
        btn_to_results = QPushButton('Go to Results')
        btn_to_results.clicked.connect(self.show_results)
        form.addRow(btn_to_results)

        # form inputs (submit disabled)
        self.investor_input = QLineEdit()
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

        submit = QPushButton('Submit')
        submit.setEnabled(False)
        form.addRow(submit)

        page.setLayout(form)
        self.stack.addWidget(page)

    def init_results_page(self):
        page = QWidget()
        layout = QVBoxLayout()
        self.info_label = QLabel('Results')
        layout.addWidget(self.info_label)

        hl = QHBoxLayout()
        self.table_tran = QTableWidget(); self.table_tran.setSortingEnabled(True)
        self.table_acc = QTableWidget(); self.table_acc.setSortingEnabled(True)
        hl.addWidget(self.table_tran); hl.addWidget(self.table_acc)
        layout.addLayout(hl)

        btn_to_form = QPushButton('Go to Form')
        btn_to_form.clicked.connect(lambda: self.stack.setCurrentIndex(1))
        layout.addWidget(btn_to_form)

        page.setLayout(layout)
        self.stack.addWidget(page)

    def load_last_timestamp(self):
        try:
            with open(TIMESTAMP_FILE) as f: return f.read().strip()
        except FileNotFoundError:
            return None

    def save_last_timestamp(self, ts):
        with open(TIMESTAMP_FILE, 'w') as f: f.write(ts)

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

    def refresh_data(self):
        # pullData uses original API logic and populates tables
        self.pullData()
        # save data to DB
        self.save_to_db(DB_TRANSACTIONS, 'transactions', self.last_rows_tran)
        self.save_to_db(DB_ACCOUNTS, 'positions', self.last_rows_acc)
        # update timestamp and label
        now = datetime.now().isoformat()
        self.save_last_timestamp(now)
        self.last_update_label.setText(now)
        # show results
        self.stack.setCurrentIndex(2)

    def show_results(self):
        # load from DB if exists
        self.load_from_db()
        self.stack.setCurrentIndex(2)

    def load_from_db(self):
        # Transactions
        if os.path.exists(DB_TRANSACTIONS):
            conn = sqlite3.connect(DB_TRANSACTIONS)
            cur = conn.cursor()
            try:
                cur.execute('SELECT * FROM transactions')
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                self.populate(self.table_tran, rows)
            except sqlite3.OperationalError:
                pass
            conn.close()
        # Accounts
        if os.path.exists(DB_ACCOUNTS):
            conn = sqlite3.connect(DB_ACCOUNTS)
            cur = conn.cursor()
            try:
                cur.execute('SELECT * FROM positions')
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                self.populate(self.table_acc, rows)
            except sqlite3.OperationalError:
                pass
            conn.close()

    def pullData(self):
        self.table_tran.clear(); self.table_tran.setRowCount(0); self.table_tran.setColumnCount(0)
        self.table_acc.clear(); self.table_acc.setRowCount(0); self.table_acc.setColumnCount(0)
        for i in range(2):
            all_rows = []
            apiData = {
                "tranCols": "Investment in, Investing Entity, Amount, Transaction Type, Effective date",
                "tranName": "InvestmentTransaction",
                "tranSort": "Effective date:desc",
                "accountCols": "As of Date, Balance Type, Asset Class, Sub-asset class, Value of Investments, Investing entity, Investment in",
                "accountName": "InvestmentPosition",
                "accountSort": "As of Date:desc",
            }
            cols_key = 'accountCols' if i == 1 else 'tranCols'
            name_key = 'accountName' if i == 1 else 'tranName'
            sort_key = 'accountSort' if i == 1 else 'tranSort'
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "x-columns": apiData[cols_key],
                "x-sort": apiData[sort_key]
            }
            if i == 0: #transaction
                payload = {
                    "advf": {
                        "e": [
                            {
                                "_name": "InvestmentTransaction",
                                "rule": [
                                    {
                                        "_op": "any_item",
                                        "_prop": "Transaction type",
                                        "values": [
                                            [
                                                {
                                                    "id": "d681dc62-f2a3-4dd7-b04f-55455d6576c2",
                                                    "es": "L_TransactionType",
                                                    "name": "Realized investment gain (loss)"
                                                },
                                                {
                                                    "id": "7e564e00-3ec6-4fe3-8655-b11295f46c8d",
                                                    "es": "L_TransactionType",
                                                    "name": "Unrealized investment gain (loss)"
                                                },
                                                {
                                                    "id": "b136525a-2708-45c9-9d5e-405de439eaca",
                                                    "es": "L_TransactionType",
                                                    "name": "Reversal of unrealized investment gain (loss)"
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
                                            "2025-05-01T00:00:00.000Z",
                                            "2025-05-30T00:00:00.000Z"
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    "mode": "compact"
                }
            else: #account (position)
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
                                            "_prop": "Investing entity",
                                            "values": [
                                                "pool, llc"
                                            ]
                                        },
                                        {
                                            "_op": "between_date",
                                            "_prop": "As of date",
                                            "values": [
                                                "2025-04-01T00:00:00.000Z",
                                                "2025-06-30T00:00:00.000Z"
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

                all_rows.extend(rows)
            else:
                print(f"Error in API call. Code: {response.status_code}. {response}")
                try:
                    print(f"Error: {response.json()}")
                    print(f"Headers used:  \n {headers}, \n payload used: \n {payload}")
                except:
                    pass
            for idx, row in enumerate(all_rows[:5], start=1): print(f"Row {idx}: {row}")
            if i == 1:
                self.populate(self.table_acc, all_rows)
                self.last_rows_acc = all_rows
            else:
                self.populate(self.table_tran, all_rows)
                self.last_rows_tran = all_rows

    def save_to_db(self, db_path, table, rows):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        if rows:
            cols = list(rows[0].keys())
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
        conn.close()

    def populate(self, table, rows):
        if not rows: return
        headers = list(rows[0].keys())
        table.setColumnCount(len(headers)); table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(rows))
        for r, row in enumerate(rows):
            for c, h in enumerate(headers):
                table.setItem(r, c, QTableWidgetItem(str(row.get(h, ''))))

if __name__ == '__main__':
    key = os.environ.get('Dynamo_API')
    ok = key and key != 'value'
    app = QApplication(sys.argv)
    w = MyWindow(start_index=0 if not ok else 1)
    if ok: w.api_key = key
    w.show()
    sys.exit(app.exec_())
