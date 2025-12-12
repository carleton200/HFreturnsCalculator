from scripts.importList import *
from scripts.instantiate_basics import *
from scripts.commonValues import *
from scripts.basicFunctions import infer_sqlite_type
from classes.nodeLibrary import nodeLibrary

class DatabaseManager:
    """Thread-safe SQLite database manager.

    Uses a single connection with check_same_thread=False and an RLock to
    serialize access. Suitable for simple concurrent usage via a thread pool.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._lock = threading.RLock()
        self.server = 'hf-server1.database.windows.net'
        self.database = "CRSPRdata"
        self.username = "carleton2022"
        self.password = "Griffine1124"
        driver = 'ODBC Driver 17 for SQL Server'  # Ensure this driver is installed
        #self.connString = f"DRIVER={{{driver}}}; SERVER={server}; DATABASE={database}; UID={username}; PWD={password}; Connect Timeout=3" 
        self.instantiateConnections()
        self.instantiateTables()

    def instantiateConnections(self):
        if  not remoteDBmode:
            self._conn = self.makeConnection()
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.commit()
        else:
            attemptIdx = 0
            print("\n")
            while True:
                attemptIdx += 1
                if attemptIdx > 6:
                    databaseType = 2 #switch to non-database mode so gui can start and reconnect later
                    self._conn = None
                    break
                print(f"Attempting connection to database (attempt {attemptIdx})...")
                logging.info(f"Connection attempt to database initiated")
                try:
                    # Establish the connection
                    self._conn = self.makeConnection()
                    
                    print("Connection successful!")
                    break

                except pymssql.Error as e:
                    print("Error connecting to the database:", e)
                    pass
    def makeConnection(self):
        if not remoteDBmode:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
        else:
            print("Attempting connection with pymssql...")
            conn = pymssql.connect(
                server=self.server,
                database=self.database,
                user=self.username,
                password=self.password,
                timeout=30
            )
            print("✓ pymssql connection successful!")
        return conn
    def create_table_if_not_exists(self, cur, table_name, columns, primary_keys=None):
        """
        Helper to create table for both sqlite and remote modes.
        columns: list of 2-tuples, [(col, type), ...]. Type should be TEXT or INTEGER etc.
        primary_keys: list of col names to be primary keys, or None.
        """
        if not remoteDBmode:
            col_defs = []
            for col, typ in columns:
                col_defs.append(f"{col} {typ}")
            if primary_keys:
                col_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(col_defs) + "\n)"
        else:
            # For remote, use NVARCHAR(255) for TEXT, INT/INTEGER for INTEGER
            col_defs = []
            for col, typ in columns:
                if typ.upper() == "TEXT":
                    type_sql = "NVARCHAR(255)"
                elif typ.upper() in ["INTEGER", "INT"]:
                    type_sql = "INT"
                else:
                    # fallback/safe
                    type_sql = typ
                col_defs.append(f"{col} {type_sql}")
            if primary_keys:
                col_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
            sql = f"""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.{table_name}') AND type in (N'U'))
                    CREATE TABLE {table_name} (
                        {',\n    '.join(col_defs)}
                    )
                    """
        cur.execute(sql)

    def instantiateTables(self) -> None:
        """Instantiate the tables in the database."""
        with self._lock:
            cur = self._conn.cursor()
            # calculations
            self.create_table_if_not_exists(
                cur,
                "nodeCalculations",
                [
                    ("dateTime", "TEXT"),
                    ("[Source name]", "TEXT"),
                    ("Node", "TEXT"),
                    ("[Target name]", "TEXT"),
                    ("NAV", "REAL"),
                    ("[Monthly Gain]", "REAL"),
                    ("[Return]", "REAL"),
                    ("MDdenominator", "REAL"),
                    ("Ownership", "REAL"),
                    ("Commitment", "REAL"),
                    ("Unfunded", "REAL"),
                    ("[IRR ITD]", "REAL"),
                    ("ownershipAdjust", "BOOL"),
                ],
                primary_keys=["dateTime", "[Target name]", "Node", "[Source name]"]
            )
            # options
            self.create_table_if_not_exists(
                cur,
                "options",
                [
                    ("grouping", "TEXT"),
                    ("id", "TEXT"),
                    ("value", "TEXT"),
                ],
                primary_keys=["grouping", "id"]
            )
            # benchmarkLinks
            self.create_table_if_not_exists(
                cur,
                "benchmarkLinks",
                [
                    ("benchmark", "TEXT"),
                    ("asset", "TEXT"),
                    ("assetLevel", "INTEGER"),
                ]
            )
            # tranCalculations
            self.create_table_if_not_exists(
                cur,
                "tranCalculations",
                [
                    ("dateTime", "TEXT"),
                    ("Pool", "TEXT"),
                    ("[Transaction Sum]", "REAL"),
                ],
                primary_keys=["dateTime", "Pool"]
            )
            self.create_table_if_not_exists(
                cur,
                "history",
                [("lastImport" , "TEXT"),
                ("currentVersion", "TEXT"),
                ("lastCalculation", "TEXT"),
                ("changeDate", "TEXT")]
            )
            self.create_table_if_not_exists(
                cur,
                'nodes',
                [
                    ('id', 'INTEGER'),
                    ('name' , 'TEXT'),
                    ('lowestLevel', 'INTEGER'),
                    ('above', 'TEXT'),
                    ('below','TEXT')
                ],
                primary_keys=['id',]
            )
            cur.execute("SELECT * FROM history")
            history = cur.fetchall()
            if len(history) == 0: #add a history entry to work with. Will demand a new import
                params = ("December 1, 1999 @ 10:00 AM", currentVersion, "December 1, 1999 @ 10:00 AM", "December 1, 1999 @ 10:00 AM")
                cur.execute(f"INSERT INTO history (lastImport, currentVersion, lastCalculation, changeDate) VALUES ({sqlPlaceholder},{sqlPlaceholder},{sqlPlaceholder},{sqlPlaceholder})",params)
            self._conn.commit()
            cur.close()
            
    def fetchOptions(self, grouping : str, update: bool = False):
        if not hasattr(self, "options"):
            self.options = {}
        if not hasattr(self.options, grouping) or update:
            with self._lock:
                cursor = self._conn.cursor()
                cursor.execute(f"SELECT * FROM options WHERE grouping = {sqlPlaceholder}", (grouping,))
                headers = [d[0] for d in cursor.description]
                options = [dict(zip(headers, row)) for row in cursor.fetchall()]
                self.options[grouping] = {row["id"] : row["value"] for row in options}
                cursor.close()
        return self.options[grouping]
    def saveAsset3Visibility(self, visibility : list):
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(f"DELETE FROM options WHERE grouping = {sqlPlaceholder}", ("asset3Visibility",))
            for vis in visibility:
                cursor.execute(f"INSERT INTO options (grouping, id, value) VALUES ({sqlPlaceholder}, {sqlPlaceholder}, {sqlPlaceholder})", ("asset3Visibility", vis, "hide"))
            self._conn.commit()
            cursor.close()
        self.options["asset3Visibility"] = {vis : "hide" for vis in visibility}
        logging.info(f"Saved asset3Visibility: {visibility}")
        print(f"Saved asset3Visibility: {visibility}")
    def fetchBenchmarkLinks(self, update: bool = False):
        if not hasattr(self, "benchmarkLinks") or update:
            with self._lock:
                cursor = self._conn.cursor()
                cursor.execute("SELECT * FROM benchmarkLinks")
                headers = [d[0] for d in cursor.description]
                self.benchmarkLinks = [dict(zip(headers, row)) for row in cursor.fetchall()]
                cursor.close()
        return self.benchmarkLinks
    def fetchBenchmarks(self, update: bool = False):
        if not hasattr(self, "benchmarks") or update:
            with self._lock:
                cursor = self._conn.cursor()
                cursor.execute("SELECT DISTINCT [Index] FROM benchmarks")
                self.benchmarks = [row[0] for row in cursor.fetchall()]
                cursor.close()
        return self.benchmarks
    def fetchInvestors(self, update: bool = False):
        if not hasattr(self, "investors") or update:
            with self._lock:
                cursor = self._conn.cursor()
                cursor.execute("SELECT * FROM investors")
                headers = [d[0] for d in cursor.description]
                rows = [dict(zip(headers,row)) for row in cursor.fetchall()]
                self.investors = rows
                cursor.close()
            self.investor2family = self.connectInvestor2family()
        return self.investors
    def connectInvestor2family(self):
        investors = self.investors
        inv2fam = {}
        for investor in investors:
            inv2fam[investor['Name']] = investor['Parentinvestor']
        return inv2fam
    def fetchFunds(self, update: bool = False):
        if not hasattr(self, "funds") or update:
            with self._lock:
                rows = []
                for tableName in ('funds', 'securities'):
                    cursor = self._conn.cursor()
                    cursor.execute(f"SELECT * FROM {tableName}")
                    headers = [d[0] for d in cursor.description]
                    rows.extend([dict(zip(headers,row)) for row in cursor.fetchall()])
                self.funds = rows
                cursor.close()
            self.fund2trait = self.connectFund2Trait()
        return self.funds
    def fetchDyn2Key(self):
        filtOpts = masterFilterOptions
        dyn2key = {filt['fundDyn'] : filt['key'] for filt in filtOpts if filt['key'] not in nonFundCols}
        return dyn2key
    def connectFund2Trait(self):
        dyn2key = self.fetchDyn2Key()
        filtOpts = masterFilterOptions
        dyn2key = {filt['fundDyn'] : filt['key'] for filt in filtOpts if filt['key'] not in nonFundCols}
        fund2trait = {}
        for fund in self.funds:
            fund2trait[fund['Name']] = {}
            for key, data in fund.items():
                if key in dyn2key:
                    fund2trait[fund['Name']][dyn2key[key]] = data
        return fund2trait
    def fetchFund2Trait(self):
        if not hasattr(self,'fund2trait'):
            self.fetchFunds()
        return self.fund2trait
    def fetchNodes(self, update: bool = False):
        if not hasattr(self, "nodes") or update:
            with self._lock:
                cursor = self._conn.cursor()
                cursor.execute("SELECT * FROM nodes")
                headers = [d[0] for d in cursor.description]
                rows = [dict(zip(headers,row)) for row in cursor.fetchall()]
                self.nodes = rows
                cursor.close()
        return self.nodes
    def pullId2Node(self):
        nodes = self.fetchNodes()
        id2Node = {node['id'] : node['name'] for node in nodes}
        return id2Node
    def pullInvestorsFromFamilies(self, familyBranches: list[str]):
        investors = self.fetchInvestors()
        return [investor['Name'] for investor in investors if investor['Parentinvestor'] in familyBranches]
    def pullFundsFromFilters(self, filDict : dict[list[str]]):
        try:
            fund2trait = self.fetchFund2Trait()
            filteredFunds = []
            for fund_name, traits in fund2trait.items():
                # Check if this fund matches all filter criteria
                matches_all = True
                for filKey, Options in filDict.items():
                    # Get the trait value for this filter key
                    trait_value = traits.get(filKey, "")
                    if trait_value not in Options:
                        matches_all = False
                        break
                if matches_all:
                    filteredFunds.append(fund_name)
            return filteredFunds
        except Exception as e:
            print(f"ERROR: Connecting funds to filter options failed: {e.args}")
    def userDisplayLib(self):
        dispDict = {'id2disp' : {}, 'disp2id' : {}}
        dispDict['id2disp'] = {str(key) : str(val) for key,val in self.pullId2Node().items()}
        for key, val in displayLinks.items():
            dispDict['id2disp'][key] = val
        dispDict['disp2id'] = {val : key for key,val in dispDict['id2disp'].items()} #reverse id2disp
        return dispDict
    def buildNodeLib(self, update:bool = False):
        if not hasattr(self,'nodeLib'):
            self.nodeLib = nodeLibrary([*load_from_db(self,'transactions'),*load_from_db(self,'positions')])
    def close(self) -> None:
        try:
            with self._lock:
                self._conn.close()
        except Exception:
            pass


def save_to_db(db : DatabaseManager, table, rows, action = "", query = "",inputs = None, keys = None):
    try:
        conn = db._conn
        with db._lock:
            cur = db._conn.cursor()
            if action == "reset":
                cur.execute(f"DROP TABLE IF EXISTS {table}")
                conn.commit()
            elif action == "clear":
                if remoteDBmode:
                    cur.execute(f"TRUNCATE TABLE [{table}]")
                else:
                    cur.execute(f"DELETE FROM {table}")
                conn.commit()
            elif action == "add":
                try:
                    for row in rows:
                        cols = list(row.keys())
                        quoted_cols = ','.join(f'"{c}"' for c in cols)
                        placeholders = ','.join(sqlPlaceholder for _ in cols)
                        sql = f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
                        vals = tuple(str(row.get(c, '')) for c in cols)
                        cur.execute(sql,vals)
                        conn.commit()
                except Exception as e:
                    print(f"Error inserting row into database: {e}")
                    print("e.args:", e.args)
                    # maybe also:
                    try:
                        print(traceback.format_exc())
                    except:
                        pass
            elif action == "calculationUpdate":
                try:
                    cur.execute(f"DELETE FROM calculations WHERE [dateTime] = {sqlPlaceholder}", inputs) #inputs input should be the date for deletion
                    # Batch the inserts for better performance
                    if rows:
                        cols = list(rows[0].keys())
                        quoted_cols = ','.join(f'"{c}"' for c in cols)
                        placeholders = ','.join(sqlPlaceholder for _ in cols)
                        sql = (f"INSERT INTO calculations ({quoted_cols}) VALUES ({placeholders})")
                        
                        BATCH_SIZE = 50000
                        total_rows = len(rows)
                        for i in range(0, total_rows, BATCH_SIZE):
                            batch = rows[i:i+BATCH_SIZE]
                            vals = [tuple(str(row.get(c, '')) for c in cols) for row in batch]
                            cur.executemany(sql, vals)
                            if i % (BATCH_SIZE * 10) == 0:  # Progress every 10 batches
                                print(f"  Inserted {min(i+BATCH_SIZE, total_rows)}/{total_rows} calculations...")
                        conn.commit()
                except Exception as e:
                    print(f"Error updating calculations in database: {e}")
                    print("e.args:", e.args)
                    # maybe also:
                    try:
                        import traceback
                        print(traceback.format_exc())
                    except:
                        pass
            elif action == "replace":
                # Replace ? placeholders with the appropriate placeholder for the current DB mode
                processed_query = query.replace('?', sqlPlaceholder)
                cur.execute(processed_query,inputs)
                conn.commit()
            elif rows:
                if keys is None:
                    cols = list(rows[0].keys())
                else:
                    cols = list(keys)
                quoted_cols = ','.join(f'"{c}"' for c in cols)
                # Dynamically determine column types based on variable values in the first row
                

                # Use the first row to infer data types, fallback to TEXT if empty
                sample_row = rows[0] if rows else {}
                col_defs = ','.join(
                    f'"{c}" {infer_sqlite_type(sample_row.get(c, ""))}' for c in cols
                )
                placeholders = ','.join(sqlPlaceholder for _ in cols)
                sql = f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
                
                # Prepare all values first
                vals = [tuple(str(row.get(c, '')) for c in cols) for row in rows]
                
                colFail = False
                try: 
                    # Use TRUNCATE for remote DB (faster), DELETE for SQLite
                    if remoteDBmode:
                        cur.execute(f'TRUNCATE TABLE [{table}]')
                    else:
                        cur.execute(f'DELETE FROM "{table}"')
                    colFail = True
                    
                    # Batch the inserts for better performance with large datasets
                    total_rows = len(vals)
                    BATCH_SIZE = 50000

                    if total_rows > BATCH_SIZE:
                        print(f"  Inserting {total_rows} rows into {table} in batches of {BATCH_SIZE}...")
                        for i in range(0, total_rows, BATCH_SIZE):
                            batch_vals = vals[i:i+BATCH_SIZE]
                            cur.executemany(sql, batch_vals)
                            conn.commit()  # Commit after each batch
                            progress = min(i+BATCH_SIZE, total_rows)
                            if (i // BATCH_SIZE) % 5 == 0 or progress == total_rows:  # Progress every 5 batches
                                print(f"    {table}: {progress}/{total_rows} rows inserted ({progress*100//total_rows}%)")
                    else:
                        # Small datasets can be inserted all at once
                        cur.executemany(sql, vals)
                        conn.commit()
                        
                except Exception as e: #will fail if the table doesn't exist, so build the table from scratch
                    if colFail: #occurs if the table exists, but the columns don't match
                        logging.warning(f"Bad columns were attempted to be inserted into table {table}. {e.args}")
                        print(f"Bad columns were attempted to be inserted into table {table}. {e.args}")
                        cur.execute(f"DROP TABLE {table}")
                    if not remoteDBmode:
                        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')
                    else:
                        # Compose a CREATE TABLE statement for pymssql/MS SQL Server
                        col_defs_mssql = ','.join(f'[{c}] NVARCHAR(MAX)' for c in cols)
                        cur.execute(f'IF OBJECT_ID(N\'{table}\', N\'U\') IS NULL CREATE TABLE [{table}] ({col_defs_mssql})')
                    
                    # Retry with batching after table creation
                    total_rows = len(vals)
                    BATCH_SIZE = 50000
                    if total_rows > BATCH_SIZE:
                        print(f"  Inserting {total_rows} rows into {table} in batches of {BATCH_SIZE}...")
                        for i in range(0, total_rows, BATCH_SIZE):
                            batch_vals = vals[i:i+BATCH_SIZE]
                            cur.executemany(sql, batch_vals)
                            conn.commit()
                            progress = min(i+BATCH_SIZE, total_rows)
                            if (i // BATCH_SIZE) % 5 == 0 or progress == total_rows:
                                print(f"    {table}: {progress}/{total_rows} rows inserted ({progress*100//total_rows}%)")
                    else:
                        cur.executemany(sql, vals)
                        conn.commit()
                
                
                cur.close()
            else:
                print(f"No rows found for data input to '{table}'")
            return True
    except Exception as e:
        print(f"DB save failed. closing connections {e}, {e.args}") 
        try:
            cur.close()
        except:
            pass
        return False
def load_from_db(db : DatabaseManager, table, condStatement = "",parameters = None):
    try:
        conn = db._conn
        with db._lock:
            cur = db._conn.cursor()
            try:
                if condStatement != "" and parameters is not None:
                    # Replace ? placeholders with the appropriate placeholder for the current DB mode
                    processed_cond = condStatement.replace('?', sqlPlaceholder)
                    cur.execute(f'SELECT * FROM {table} {processed_cond}',parameters)
                elif condStatement != "" and parameters is None:
                    cur.execute(f'SELECT * FROM {table} {condStatement}')
                else:
                    cur.execute(f'SELECT * FROM {table}')
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                conn.commit()
                cur.close()
                return rows
            except Exception as e:
                try:
                    print(f"Error loading from database: {e}, table: {table} condStatment: {condStatement}, parameters: {parameters or ""}")
                    cur.close()
                except:
                    pass
                return []
    except:
        print("DB load failed. closing connections")
        try:
            cur.close()
        except:
            pass
