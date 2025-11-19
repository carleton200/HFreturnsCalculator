from scripts.importList import *
from scripts.instantiate_basics import *
from scripts.commonValues import *
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
        self.driver = 'ODBC Driver 18 for SQL Server'
        self.batch_size = 50000
        self.fetch_batch_size = 2000
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

                except pyodbc.Error as e:
                    print("Error connecting to the database:", e)
                    pass
    def makeConnection(self):
        if not remoteDBmode:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
        else:
            print("Attempting connection with pyodbc...")
            conn = pyodbc.connect(
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "Encrypt=yes;"
                "TrustServerCertificate=yes;"
                "Connection Timeout=30;",
                autocommit=False,
            )
            print("✓ pyodbc connection successful!")
        return conn

    def get_cursor(self):
        cursor = self._conn.cursor()
        if remoteDBmode:
            try:
                cursor.fast_executemany = True
            except AttributeError:
                pass
            cursor.arraysize = self.fetch_batch_size
        return cursor
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
            cur = self.get_cursor()
            # calculations
            self.create_table_if_not_exists(
                cur,
                "calculations",
                [
                    ("dateTime", "TEXT"),
                    ("Investor", "TEXT"),
                    ("Pool", "TEXT"),
                    ("Fund", "TEXT"),
                    ("assetClass", "TEXT"),
                    ("subAssetClass", "TEXT"),
                    ("subAssetSleeve", "TEXT"),
                    ("NAV", "TEXT"),
                    ("[Monthly Gain]", "TEXT"),
                    ("[Return]", "TEXT"),
                    ("MDdenominator", "TEXT"),
                    ("Ownership", "TEXT"),
                    ("Commitment", "TEXT"),
                    ("Unfunded", "TEXT"),
                    ("[IRR ITD]", "TEXT"),
                    ("Classification", "TEXT"),
                    ("[Calculation Type]", "TEXT"),
                    ("[Family Branch]","TEXT"),
                    ("ownershipAdjust", "TEXT"),
                ],
                primary_keys=["dateTime", "Investor", "Pool", "Fund"]
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
                    ("[Transaction Sum]", "TEXT"),
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
                cursor = self.get_cursor()
                cursor.execute(f"SELECT * FROM options WHERE grouping = {sqlPlaceholder}", (grouping,))
                headers = [d[0] for d in cursor.description]
                options = [dict(zip(headers, row)) for row in cursor.fetchall()]
                self.options[grouping] = {row["id"] : row["value"] for row in options}
                cursor.close()
        return self.options[grouping]
    def saveAsset3Visibility(self, visibility : list):
        with self._lock:
            cursor = self.get_cursor()
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
                cursor = self.get_cursor()
                cursor.execute("SELECT * FROM benchmarkLinks")
                headers = [d[0] for d in cursor.description]
                self.benchmarkLinks = [dict(zip(headers, row)) for row in cursor.fetchall()]
                cursor.close()
        return self.benchmarkLinks
    def fetchBenchmarks(self, update: bool = False):
        if not hasattr(self, "benchmarks") or update:
            with self._lock:
                cursor = self.get_cursor()
                cursor.execute("SELECT DISTINCT [Index] FROM benchmarks")
                self.benchmarks = [row[0] for row in cursor.fetchall()]
                cursor.close()
        return self.benchmarks
    def close(self) -> None:
        try:
            with self._lock:
                self._conn.close()
        except Exception:
            pass


def _batched_executemany(cursor, sql, values, batch_size, progress_label=None):
        total_rows = len(values)
        if total_rows == 0:
            return
        if total_rows <= batch_size:
            cursor.executemany(sql, values)
            return
        for i in range(0, total_rows, batch_size):
            batch_vals = values[i:i+batch_size]
            cursor.executemany(sql, batch_vals)
            if progress_label:
                progress = min(i + batch_size, total_rows)
                if progress == total_rows or (i // batch_size) % 5 == 0:
                    print(f"    {progress_label}: {progress}/{total_rows} rows inserted ({progress*100//total_rows}%)")

def save_to_db(self, table, rows, action = "", query = "",inputs = None, keys = None):
    cur = None
    try:
        conn = self.db._conn
        with self.db._lock:
            cur = self.db.get_cursor()
            batch_size = getattr(self.db, "batch_size", 50000)
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
                    if not rows:
                        print(f"No rows found for data input to '{table}'")
                    else:
                        cols = list(rows[0].keys())
                        quoted_cols = ','.join(f'"{c}"' for c in cols)
                        placeholders = ','.join(sqlPlaceholder for _ in cols)
                        sql = f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
                        vals = [tuple(str(row.get(c, '')) for c in cols) for row in rows]
                        _batched_executemany(cur, sql, vals, batch_size)
                        conn.commit()
                except Exception as e:
                    print(f"Error inserting row into database: {e}")
                    print("e.args:", e.args)
                    try:
                        print(traceback.format_exc())
                    except:
                        pass
            elif action == "calculationUpdate":
                try:
                    cur.execute(f"DELETE FROM calculations WHERE [dateTime] = {sqlPlaceholder}", inputs)
                    if rows:
                        cols = list(rows[0].keys())
                        quoted_cols = ','.join(f'"{c}"' for c in cols)
                        placeholders = ','.join(sqlPlaceholder for _ in cols)
                        sql = f"INSERT INTO calculations ({quoted_cols}) VALUES ({placeholders})"
                        vals = [tuple(str(row.get(c, '')) for c in cols) for row in rows]
                        _batched_executemany(cur, sql, vals, batch_size, progress_label="calculations")
                    conn.commit()
                except Exception as e:
                    print(f"Error updating calculations in database: {e}")
                    print("e.args:", e.args)
                    try:
                        import traceback
                        print(traceback.format_exc())
                    except:
                        pass
            elif action == "replace":
                processed_query = query.replace('?', sqlPlaceholder)
                cur.execute(processed_query,inputs)
                conn.commit()
            elif rows:
                if keys is None:
                    cols = list(rows[0].keys())
                else:
                    cols = list(keys)
                quoted_cols = ','.join(f'"{c}"' for c in cols)
                col_defs = ','.join(f'"{c}" TEXT' for c in cols)
                placeholders = ','.join(sqlPlaceholder for _ in cols)
                sql = f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
                vals = [tuple(str(row.get(c, '')) for c in cols) for row in rows]
                colFail = False
                try: 
                    if remoteDBmode:
                        cur.execute(f'TRUNCATE TABLE [{table}]')
                    else:
                        cur.execute(f'DELETE FROM "{table}"')
                    colFail = True
                    total_rows = len(vals)
                    if total_rows > batch_size:
                        print(f"  Inserting {total_rows} rows into {table} in batches of {batch_size}...")
                    _batched_executemany(cur, sql, vals, batch_size, progress_label=table if total_rows > batch_size else None)
                    conn.commit()
                except Exception as e:
                    if colFail:
                        logging.warning(f"Bad columns were attempted to be inserted into table {table}. {e.args}")
                        print(f"Bad columns were attempted to be inserted into table {table}. {e.args}")
                        cur.execute(f"DROP TABLE {table}")
                    if not remoteDBmode:
                        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')
                    else:
                        col_defs_mssql = ','.join(f'[{c}] NVARCHAR(MAX)' for c in cols)
                        cur.execute(f'IF OBJECT_ID(N\'{table}\', N\'U\') IS NULL CREATE TABLE [{table}] ({col_defs_mssql})')
                    total_rows = len(vals)
                    if total_rows > batch_size:
                        print(f"  Inserting {total_rows} rows into {table} in batches of {batch_size}...")
                    _batched_executemany(cur, sql, vals, batch_size, progress_label=table if total_rows > batch_size else None)
                    conn.commit()
            else:
                print(f"No rows found for data input to '{table}'")
        return True
    except Exception as e:
        print(f"DB save failed. closing connections {e}, {e.args}") 
        return False
    finally:
        try:
            if cur:
                cur.close()
        except:
            pass
def load_from_db(self, table, condStatement = "",parameters = None):
    cur = None
    try:
        conn = self.db._conn
        with self.db._lock:
            cur = self.db.get_cursor()
            try:
                if condStatement != "" and parameters is not None:
                    processed_cond = condStatement.replace('?', sqlPlaceholder)
                    cur.execute(f'SELECT * FROM {table} {processed_cond}',parameters)
                elif condStatement != "" and parameters is None:
                    cur.execute(f'SELECT * FROM {table} {condStatement}')
                else:
                    cur.execute(f'SELECT * FROM {table}')
                cols = [d[0] for d in cur.description]
                rows = []
                fetch_size = getattr(self.db, "fetch_batch_size", 2000)
                while True:
                    batch = cur.fetchmany(fetch_size)
                    if not batch:
                        break
                    rows.extend(dict(zip(cols, row)) for row in batch)
                conn.commit()
                return rows
            except Exception as e:
                if parameters is not None and table != "calculations":
                    print(f"Error loading from database: {e}, table: {table} condStatment: {condStatement}, parameters: {parameters}")
                elif table != "calculations":
                    print(f"Error loading from database: {e}, table: {table} condStatment: {condStatement}")
                else:
                    print(f"Info: {e}, {e.args}")
                return []
    except:
        print("DB load failed. closing connections")
        return []
    finally:
        try:
            if cur:
                cur.close()
        except:
            pass
