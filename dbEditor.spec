# -*- mode: python ; coding: utf-8 -*-

import sys
from os import path
site_packages = 'C:/Users/coneil/Local Files/Returns Calculator/returnEnv/Lib/site-packages'

a = Analysis(
    ['dbEditor.py'],
    pathex=[],
    binaries=[],
    datas=[(path.join(site_packages, "customtkinter"), "customtkinter"),
        (path.join(site_packages, "darkdetect"), "darkdetect"),],
    hiddenimports=[
        'pyodbc',
        'customtkinter',
        'customtkinter.windows',  # Ensure all submodules are included
        'customtkinter.windows.widgets',
        'customtkinter.windows.theme',
        'darkdetect',
        'babel',
        'babel.numbers',
        'babel.dates',
        'babel.localedata',
        'sqlite3'
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='dbEditor',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    hide_console='hide-early',
)
