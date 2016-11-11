import os
import platform

DATA_DIR = "rosstat"
RAR_DIR = DATA_DIR
CSV_DIR = DATA_DIR
# todo: create if not exists
# new data dir should be "data"

IS_WINDOWS = (platform.system() == 'Windows')
if IS_WINDOWS:
    UNPACK_RAR_EXE = os.path.join('bin', 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'
