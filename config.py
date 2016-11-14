# -*- coding: utf-8 -*-
import os
import platform

RAR_DIR = os.path.join("data","source","rar")
ROSSTAT_CSV_DIR = os.path.join("data","source","raw_csv")
CLEAN_CSV_DIR = os.path.join("data","user")# new data dir

# create directories if not exists
for directory in [RAR_DIR, ROSSTAT_CSV_DIR, CLEAN_CSV_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# RAR executable
IS_WINDOWS = (platform.system() == 'Windows')
if IS_WINDOWS:
    UNPACK_RAR_EXE = os.path.join('bin', 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'
