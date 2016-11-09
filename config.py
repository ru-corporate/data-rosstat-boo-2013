import os
import platform

DATA_DIR = "data"
RAR_DIR = DATA_DIR
CSV_DIR = DATA_DIR
# todo: create if not exists

IS_WINDOWS = (platform.system() == 'Windows')
if IS_WINDOWS:
    UNPACK_RAR_EXE = os.path.join('bin', 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'
