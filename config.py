import os
import platform

SOURCE_URL = "http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20150707t000000-structure-20131231t000000.rar"
DATA_DIR = "data"
SOURCE_CSV_PATH = os.path.join(DATA_DIR, "G2013.csv")
TARGET_CSV_PATH = os.path.join(DATA_DIR, "Adjusted_2013.csv")


IS_WINDOWS = (platform.system() == 'Windows')

if IS_WINDOWS:
    UNPACK_RAR_EXE = os.path.join('bin', 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'
