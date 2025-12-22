# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import os
from PyInstaller.utils.hooks import collect_data_files, collect_all

assets = [
    ('assets/CRSPRdata.db', 'assets'),
    ('assets/helpInfo.txt', 'assets'),
    ('calculateReturns.py','sourceCode'),
    ('calculateReturns.spec','sourceCode'),
    *[(str(f), 'sourceCode/classes') for f in Path('classes').glob('*.py')],
    *[(str(f), 'sourceCode/scripts') for f in Path('scripts').glob('*.py')],
    ('requirements.txt','sourceCode'),
]

# Collect data files for packages that need them
# dash_cytoscape needs package.json and other data files
dash_cytoscape_data = collect_data_files('dash_cytoscape')
# Dash and Plotly may need additional data files
dash_data = collect_data_files('dash')
plotly_data = collect_data_files('plotly')

# Combine all data files
all_data_files = assets + dash_cytoscape_data + dash_data + plotly_data

# Exclude only truly unnecessary modules (be more conservative)
excludes_list = [
    # Testing frameworks only (safe to exclude)
    'pytest', 'unittest', 'doctest', 'test', 'tests',
    # Development tools only (safe to exclude)
    'IPython', 'jupyter', 'notebook', 'ipykernel',
    # Documentation tools (safe to exclude)
    'sphinx',
    # Unused GUI frameworks (tkinter not used)
    'tkinter',
    # Heavy ML libraries (not used in this app)
    'tensorflow', 'torch', 'sklearn',
    # Unused web frameworks
    'tornado', 'bottle', 'cherrypy',
    # Unused database drivers
    'MySQLdb', 'psycopg2',
]

# Comprehensive hidden imports - include all that might be needed
hidden_imports_list = [
    # Core application modules
    'classes.returnsApp',
    'classes.windowClasses',
    'classes.tableWidgets',
    'classes.widgetClasses',
    'classes.DatabaseManager',
    'classes.transactionApp',
    'classes.nodeLibrary',
    # Scripts
    'scripts.importList',
    'scripts.commonValues',
    'scripts.instantiate_basics',
    'scripts.basicFunctions',
    'scripts.loggingFuncs',
    'scripts.processClump',
    'scripts.processInvestments',
    'scripts.processNode',
    'scripts.reportWorkbooks',
    'scripts.render_report',
    'scripts.exportTableToExcel',
    # TreeScripts (Dash app)
    'TreeScripts.unifiedv2',
    'TreeScripts.create_all_paths',
    'TreeScripts.dash_launcher',
    'TreeScripts.import_combined_acct',
    # Dash/Flask - comprehensive imports
    'dash',
    'dash._config',
    'dash.dash',
    'dash.dependencies',
    'dash.exceptions',
    'dash.html',
    'dash.dcc',
    'dash.dash_table',
    'dash_cytoscape',
    'dash_cytoscape._imports_',
    'flask',
    'flask.json',
    'werkzeug',
    'werkzeug.serving',
    'werkzeug.security',
    'plotly',
    'plotly.graph_objects',
    'plotly.express',
    'plotly.io',
    'plotly.offline',
    # Multiprocessing support - comprehensive
    'multiprocessing',
    'multiprocessing.pool',
    'multiprocessing.managers',
    'multiprocessing.context',
    'multiprocessing.shared_memory',
    # PyQt5 components - comprehensive
    'PyQt5.QtCore',
    'PyQt5.QtGui',
    'PyQt5.QtWidgets',
    'PyQt5.sip',
    # Database
    'sqlite3',
    'pyodbc',
    # Excel/Data processing
    'openpyxl',
    'openpyxl.workbook',
    'openpyxl.worksheet',
    'pandas',
    'pandas._libs',
    'pandas.io',
    'pandas.io.formats',
    'numpy',
    'numpy.core',
    'numpy.core._multiarray_umath',
    # Financial calculations
    'pyxirr',
    # Date utilities
    'dateutil',
    'dateutil.relativedelta',
    'dateutil.parser',
    # Standard library modules that might be dynamically imported
    'json',
    'pickle',
    'queue',
    'threading',
    'copy',
    'traceback',
    'logging',
    'warnings',
    'subprocess',
    'calendar',
    're',
    'time',
    'datetime',
    'collections',
    'collections.defaultdict',
    'collections.deque',
]

a = Analysis(
    ['calculateReturns.py'],
    pathex=[],
    binaries=[],
    datas=all_data_files,  # Use the combined data files including dash_cytoscape
    hiddenimports=hidden_imports_list,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes_list,
    noarchive=False,
    optimize=0,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=None)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='CRSPR',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,  # Disable UPX to speed up build (can re-enable later if needed)
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    hide_console='hide-early',
    icon=None,  # Add icon path here if you have one
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,  # Disable UPX to speed up build
    upx_exclude=[],
    name='CRSPR',
)
