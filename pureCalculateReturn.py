import sys
import os
import json
import subprocess
import sqlite3
import requests
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
from PyQt5.QtWidgets import (
    QApplication, QWidget, QStackedWidget, QVBoxLayout,
    QLabel, QLineEdit, QPushButton, QFormLayout,
    QRadioButton, QButtonGroup, QComboBox, QHBoxLayout,
    QTableWidget, QTableWidgetItem
)
from PyQt5.QtGui import QBrush, QColor
from PyQt5.QtCore import Qt

# Determine assets path, works in PyInstaller bundle or script
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(BASE_DIR, 'assets')
TIMESTAMP_FILE = os.path.join(ASSETS_DIR, 'last_update.txt')
DATABASE_PATH = os.path.join(ASSETS_DIR, 'Acc_Tran.db')

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
        self.investor_input = QLineEdit()
        form.addRow('Investor:', self.investor_input)
        self.investor_input.setText("James A. Haslam II Descendants Trust")
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
        submit.clicked.connect(self.submitForm)
        form.addRow(submit)

        page.setLayout(form)
        self.stack.addWidget(page)
    def submitForm(self):
        self.investor = self.investor_input.text()
        btn = self.radio_group.checkedButton()
        self.classType = btn.text()
        self.classString = None
        if self.classType == "Asset":
            self.classString = self.asset_input.text()
        elif self.classType == "Sub-asset":
            self.classString = self.subasset_input.text()
        self.month = self.month_combo.currentText()
        self.year = self.year_combo.currentText()
        print(f"'{self.investor}'", self.classType, self.month, self.year)
        if self.investor != "" and (self.classString is None or self.classString != ""):
            print("yes")
            self.pullData()

    def init_results_page(self):
        page = QWidget()
        layout = QVBoxLayout()
        self.info_label = QLabel('Results')
        layout.addWidget(self.info_label)
        self.recalculateButton = QPushButton("Recalculate Data")
        self.recalculateButton.clicked.connect(lambda: self.calculateReturn())
        layout.addWidget(self.recalculateButton)

        hl = QHBoxLayout()
        self.resultTable = QTableWidget(); self.resultTable.setSortingEnabled(True)
        hl.addWidget(self.resultTable)
        layout.addLayout(hl)

        btn_to_form = QPushButton('Go to Form')
        btn_to_form.clicked.connect(lambda: self.stack.setCurrentIndex(1))
        layout.addWidget(btn_to_form)

        page.setLayout(layout)
        self.stack.addWidget(page)

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
        month = int(datetime.strptime(self.month, "%B").month)
        
        year = str(self.year)
        lastDayCurrent = calendar.monthrange(int(year),month)[1]
        lastDayCurrent   = str(lastDayCurrent).zfill(2)
        lastDayLast = calendar.monthrange(int(year),month - 1)[1]
        lastDayLast   = str(lastDayLast).zfill(2)

        startMonth = str(month - 1).zfill(2)
        month = str(month).zfill(2)
        
        
        tranStart = f"{year}-{month}-01T00:00:00.000Z"
        tranEnd = f"{year}-{month}-{lastDayCurrent}T00:00:00.000Z"
        accountStart = f"{year}-{startMonth}-{lastDayLast}T00:00:00.000Z"
        accountEnd = f"{year}-{month}-{lastDayCurrent}T00:00:00.000Z"

        dbDates = [{"Month" : self.month, "tranStart" : tranStart.removesuffix(".000Z"), "tranEnd" : tranEnd.removesuffix(".000Z"), "accountStart" : accountStart.removesuffix(".000Z"), "accountEnd" : accountEnd.removesuffix(".000Z")}]
        self.save_to_db("Months",dbDates)
        
        apiData = {
            "tranCols": "Investment in, Investing Entity, Amount, Transaction Type, Effective date",
            "tranName": "InvestmentTransaction",
            "tranSort": "Effective date:desc",
            "accountCols": "As of Date, Balance Type, Asset Class, Sub-asset class, Value of Investments, Investing entity, Investment in",
            "accountName": "InvestmentPosition",
            "accountSort": "As of Date:desc",
        }
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
                                            "_prop": f"{investmentLevel}",
                                            "values": [
                                                "pool, llc"
                                            ]
                                        },
                                        {
                                            "_op": "between_date",
                                            "_prop": "Effective date",
                                            "values": [
                                                f"{tranStart}",
                                                f"{tranEnd}"
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
                                            "_prop": f"{investmentLevel}",
                                            "values": [
                                                "pool, llc"
                                            ]
                                        },
                                        {
                                            "_op": "between_date",
                                            "_prop": "Effective date",
                                            "values": [
                                                f"{tranStart}",
                                                f"{tranEnd}"
                                            ]
                                        },
                                        {
                                            "_op": "is_item",
                                            "_prop": "Investing entity",
                                            "values": [
                                                {
                                                    "id": "41883d69-5f52-4bd4-995e-30b9730b234e",
                                                    "es": "InvestorAccount",
                                                    "name": f"{self.investor}"
                                                }
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
                                                    f"{accountStart}",
                                                    f"{accountEnd}"
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
                                                            f"{accountStart}",
                                                            f"{accountEnd}"
                                                        ]
                                                    },
                                                    {
                                                        "_op": "is_item",
                                                        "_prop": "Investing entity",
                                                        "values": [
                                                            {
                                                                "id": "41883d69-5f52-4bd4-995e-30b9730b234e",
                                                                "es": "InvestorAccount",
                                                                "name": f"{self.investor}"
                                                            }
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
                        self.save_to_db('positions_low', rows)
                    else:
                        self.save_to_db('positions_high', rows)
                else:
                    if j == 0:
                        self.save_to_db('transactions_low', rows)
                    else:
                        self.save_to_db('transactions_high', rows)
        self.calculateReturn()

    def calculateReturn(self):
        print("Calculating return....")
        highAccounts = self.load_from_db("positions_high")
        self.populate(self.resultTable,highAccounts)
        pools = []
        for item in highAccounts:
            if item["Target name"] not in pools:
                pools.append(item["Target name"])
        months = self.load_from_db("Months")
        returns =[]
        for month in months:
            totalDays = int(datetime.strptime(month["tranEnd"], "%Y-%m-%dT%H:%M:%S").day  - datetime.strptime(month["tranStart"], "%Y-%m-%dT%H:%M:%S").day) + 1
            for pool in pools:
                #investor to pool level calculations
                startEntry = self.load_from_db("positions_high", f"WHERE [Target name] = ? AND [Date] LIKE ?",(pool,month["accountStart"]))[0]
                endEntry = self.load_from_db("positions_high", f"WHERE [Target name] = ? AND [Date] LIKE ?",(pool,month["accountEnd"]))[0]
                
                transactions = self.load_from_db("transactions_high", f"WHERE [Target name] = ? And [Date] BETWEEN ? AND ?", (pool,month["tranStart"],month["tranEnd"]))
                cashFlowSum = 0
                weightedCashFlow = 0
                
                for transaction in transactions:
                    cashFlowSum += float(transaction["Value"])
                    weightedCashFlow += float(transaction["Value"])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day))/totalDays
                returnFrac = (float(endEntry["Value"]) - float(startEntry["Value"]) - cashFlowSum)/( float(startEntry["Value"]) + weightedCashFlow)
                returnPerc = round(returnFrac * 100, 2)
                returnCash = round((float(endEntry["Value"]) - float(startEntry["Value"]) - cashFlowSum),2)
                returns.append({"Pool":pool , "Fund" : "","Month" : month["Month"], "Return (%)" : returnPerc, "Return ($)" : returnCash})
                #Start fund level calculations
                funds = []
                lowAccounts = self.load_from_db("positions_low","WHERE [Source name] = ?",(pool,))
                for account in lowAccounts:
                    if account["Target name"] not in funds:
                        funds.append(account["Target name"])
                for fund in funds:
                    startEntry = self.load_from_db("positions_low", f"WHERE [Target name] = ? AND [Date] LIKE ?",(fund,month["accountStart"]))
                    endEntry = self.load_from_db("positions_low", f"WHERE [Target name] = ? AND [Date] LIKE ?",(fund,month["accountEnd"]))
                    if len(startEntry) < 1 or len(endEntry) < 1: #skips if missing the values
                        break
                    elif len(startEntry) > 1 and len(endEntry) > 1: #combines the values for fund sub classes
                        for entry in startEntry[1:]:
                            startEntry[0]["Value"] = str(float(startEntry[0]["Value"]) + float(entry["Value"])) #adds values to the first index
                        for entry in endEntry[1:]:
                            endEntry[0]["Value"] = str(float(endEntry[0]["Value"]) + float(entry["Value"])) #adds values to the first index
                    startEntry = startEntry[0]
                    endEntry = endEntry[0]

                        
                        
                    transactions = self.load_from_db("transactions_low", f"WHERE [Target name] = ? And [Date] BETWEEN ? AND ?", (fund,month["tranStart"],month["tranEnd"]))
                    cashFlowSum = 0
                    weightedCashFlow = 0
                    
                    for transaction in transactions:
                        cashFlowSum += float(transaction["Value"])
                        weightedCashFlow += float(transaction["Value"])  *  (totalDays -int(datetime.strptime(transaction["Date"], "%Y-%m-%dT%H:%M:%S").day))/totalDays
                    try:
                        returnFrac = (float(endEntry["Value"]) - float(startEntry["Value"]) - cashFlowSum)/( float(startEntry["Value"]) + weightedCashFlow)
                        returnPerc = round(returnFrac * 100, 2)
                        returnCash = round((float(endEntry["Value"]) - float(startEntry["Value"]) - cashFlowSum),2)
                        returns.append({"Pool":pool , "Fund" : fund, "Month" : month["Month"], "Return (%)" : returnPerc, "Return ($)" : returnCash})
                    except:
                        print(f"Skipped fund {fund}")
                        #skips fund if the values are zero and cause an error
                    
        self.populate(self.resultTable,returns)

    def save_to_db(self, table, rows):
        conn = sqlite3.connect(DATABASE_PATH)
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
        else:
            print(f"No rows found for {table}")
        conn.close()

    def populate(self, table, rows):
        if not rows:
            return
        headers = list(rows[0].keys())
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(rows))

        # Determine if we need to shade rows where 'Fund' is empty
        shade_index = headers.index('Fund') if 'Fund' in headers else None

        for r, row in enumerate(rows):
            is_empty_fund = (row.get('Fund', '') == '') if shade_index is not None else False
            for c, h in enumerate(headers):
                item = QTableWidgetItem(str(row.get(h, '')))
                if is_empty_fund:
                    item.setBackground(QBrush(Qt.lightGray))
                table.setItem(r, c, item)

    def load_from_db(self,table, condStatement = "",parameters = None):
        # Transactions
        if os.path.exists(DATABASE_PATH):
            conn = sqlite3.connect(DATABASE_PATH)
            cur = conn.cursor()
            try:
                if condStatement != "" and parameters is not None:
                    cur.execute(f'SELECT * FROM {table} {condStatement}',parameters)
                else:
                    cur.execute(f'SELECT * FROM {table}')
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                return rows
            except sqlite3.OperationalError:
                pass
            conn.close()

if __name__ == '__main__':
    key = os.environ.get('Dynamo_API')
    ok = key and key != 'value'
    app = QApplication(sys.argv)
    w = MyWindow(start_index=0 if not ok else 1)
    if ok: w.api_key = key
    w.show()
    sys.exit(app.exec_())
