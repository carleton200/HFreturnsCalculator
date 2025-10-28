from scripts.importList import *
from scripts.instantiate_basics import *
class DatabaseManager:
    """Thread-safe SQLite database manager.

    Uses a single connection with check_same_thread=False and an RLock to
    serialize access. Suitable for simple concurrent usage via a thread pool.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.commit()
        self.instantiateTables()


    def instantiateTables(self) -> None:
        """Instantiate the tables in the database."""
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS calculations (
                    dateTime TEXT,
                    Investor TEXT,
                    Pool TEXT,
                    Fund TEXT,
                    assetClass TEXT,
                    subAssetClass TEXT,
                    NAV TEXT,
                    MonthlyGain TEXT,
                    Return TEXT,
                    MDdenominator TEXT,
                    Ownership TEXT,
                    Commitment TEXT,
                    Unfunded TEXT,
                    IRRITD TEXT,
                    Classification TEXT,
                    onwershipAdjust TEXT,
                    PRIMARY KEY (dateTime, Investor, Pool, Fund)
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS options (
                    grouping TEXT,
                    id TEXT,
                    value TEXT,
                    PRIMARY KEY (grouping, id)
                )
                """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS benchmarkLinks (
                    benchmark TEXT,
                    asset TEXT,
                    assetLevel INTEGER
                )
                """)
            self._conn.commit()
            
    def fetchOptions(self, grouping : str, update: bool = False):
        if not hasattr(self, "options"):
            self.options = {}
        if not hasattr(self.options, grouping) or update:
            with self._lock:
                cursor = self._conn.cursor()
                cursor.execute("SELECT * FROM options WHERE grouping = ?", (grouping,))
                headers = [d[0] for d in cursor.description]
                options = [dict(zip(headers, row)) for row in cursor.fetchall()]
                self.options[grouping] = {row["id"] : row["value"] for row in options}
                cursor.close()
        return self.options[grouping]
    def saveAsset3Visibility(self, visibility : list):
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM options WHERE grouping = ?", ("asset3Visibility",))
            for vis in visibility:
                cursor.execute("INSERT INTO options (grouping, id, value) VALUES (?, ?, ?)", ("asset3Visibility", vis, "hide"))
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
    def close(self) -> None:
        try:
            with self._lock:
                self._conn.close()
        except Exception:
            pass


def save_to_db(table, rows, action = "", query = "",inputs = None, keys = None, connection = None, lock = None, db = None):
    try:
        dbPath = db if db else DATABASE_PATH
        if lock is not None:
            lock.acquire()
        if connection is None:
            conn = sqlite3.connect(dbPath)
            cur = conn.cursor()
        else:
            conn = connection
            cur = connection.cursor()
        if action == "reset":
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
        elif action == "clear":
            cur.execute(f"DELETE FROM {table}")
            conn.commit()
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
                try:
                    print(traceback.format_exc())
                except:
                    pass
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
                try:
                    import traceback
                    print(traceback.format_exc())
                except:
                    pass
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
        return True
    except Exception as e:
        print(f"DB save failed. closing connections {e}, {e.args}") 
        try:
            if lock is not None:
                lock.release()
            cur.close()
        except:
            pass
        return False
def load_from_db(table, condStatement = "",parameters = None, cursor = None, lock = None, db = None):
    try:
        dbPath = db if db else DATABASE_PATH
        if lock is not None:
            lock.acquire()
        # Transactions
        if os.path.exists(dbPath):
            if cursor is None:
                conn = sqlite3.connect(dbPath)
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
