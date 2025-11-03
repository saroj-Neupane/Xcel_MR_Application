# -*- mode: python ; coding: utf-8 -*-
import os
from pathlib import Path

# Get the base directory (where the spec file is located)
base_dir = Path(os.path.abspath(SPECPATH))

a = Analysis(
    ['run_app.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('xcel_config.json', '.'),  # Include config file in root of bundle
        ('src', 'src'),  # Include entire src directory
        ('static', 'static'),  # Include static directory with icon
    ],
    hiddenimports=[
        'pandas', 
        'numpy', 
        'openpyxl', 
        'psutil', 
        'PyPDF2', 
        'fitz',  # PyMuPDF import name
        'pymupdf',
        'tkinter',
        'tkinter.ttk',
        'tkinter.filedialog',
        'tkinter.messagebox',
        'tkinter.scrolledtext',
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
    name='Xcel_MakeReady_QC',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='static/app_icon.ico',
)
