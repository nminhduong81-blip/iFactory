# app_onedir.spec
# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
from PyInstaller.utils.hooks import collect_all, collect_submodules, collect_dynamic_libs
import numpy, pandas

pyside6 = collect_all('PySide6')

hiddenimports = (
    ['qasync', 'aiosqlite', 'tenacity', 'pyodbc', 'pytds', 'pytds.login']
    + collect_submodules('sqlalchemy.dialects')
    + collect_submodules('numpy')
    + collect_submodules('pandas')
)

numpy_bins = collect_dynamic_libs('numpy')

def collect_pyds(pkg, target_folder):
    root = Path(pkg.__file__).resolve().parent
    items = []
    for f in root.rglob('*.pyd'):
        rel = f.relative_to(root)
        dest = str(Path(target_folder) / rel.parent).replace('\\', '/')
        items.append((str(f), dest))
    return items

np_pyds = collect_pyds(numpy, 'numpy')
pd_pyds = collect_pyds(pandas, 'pandas')

datas = pyside6[0] + [
    (str(Path('e_ui/assets').resolve()), 'e_ui/assets'),
    (str(Path('a_core/configs').resolve()), 'a_core/configs'),
]
sd = Path('storage_data')
if sd.exists():
    datas += [(str(sd.resolve()), 'storage_data')]

binaries = pyside6[1] + numpy_bins + np_pyds + pd_pyds

a = Analysis(
    ['app.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['pyarrow', 'matplotlib', 'tkinter', 'pytest', 'tests'],
    noarchive=False,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    exclude_binaries=True,
    name='MyApp',
    console=False,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    name='MyApp',
)