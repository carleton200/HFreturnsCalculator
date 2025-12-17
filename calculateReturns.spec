# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import os

assets = [
    ('assets/CRSPRdata.db', 'assets'),
    ('assets/helpInfo.txt', 'assets'),
    ('calculateReturns.py','sourceCode'),
    ('calculateReturns.spec','sourceCode'),
    *[(str(f), 'sourceCode/classes') for f in Path('classes').glob('*.py')],
    *[(str(f), 'sourceCode/scripts') for f in Path('scripts').glob('*.py')],
    ('requirements.txt','sourceCode'),
]

a = Analysis(
    ['calculateReturns.py'],
    pathex=[],
    binaries=[],
    datas=assets,
    hiddenimports=[],
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
    [],
    exclude_binaries=True,
    name='CRSPR',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    hide_console='hide-early',
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='CRSPR',
)
