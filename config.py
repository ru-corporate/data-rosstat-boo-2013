# -*- coding: utf-8 -*-
"""Directory structure and other configuration."""

import os
import platform

# RAR executable
IS_WINDOWS = (platform.system() == 'Windows')

if IS_WINDOWS:
    UNPACK_RAR_EXE = os.path.join('bin', 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'

# archives to download from Rosstat
URL={2012:"http://www.gks.ru/opendata/storage/7708234640-bdboo2012/data-20161021t000000-structure-20121231t000000.rar"
   , 2013:"http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20161021t000000-structure-20131231t000000.rar"
   , 2014:"http://www.gks.ru/opendata/storage/7708234640-bdboo2014/data-20161021t000000-structure-20141231t000000.rar"
   , 2015:"http://www.gks.ru/opendata/storage/7708234640-bdboo2015/data-20161021t000000-structure-20151231t000000.rar"     
}

VALID_YEARS = [2012,2013,2014,2015] 

# folder tree for csv's and archives
FOLDERS=dict(rar          = os.path.join("data", "temp", "rar")
           , raw_csv      = os.path.join("data", "temp", "raw_csv")
           , csv          = os.path.join("data", "csv")
           , error_log    = os.path.join("data", "temp","errors")
           , inn_subsets  = os.path.join("data", "inn")
           , test         = os.path.join("data", "temp","test")           
           , user_slices  = os.path.join("data", "user_files")
        )

def get_raw_csv_folder():
    """Path to raw csv folder"""
    return FOLDERS['raw_csv']      
    
def get_rar_folder():
    """Path to raw csv folder"""
    return FOLDERS['rar']          
    
def make_dirs():        
    # create directories if not exist
    for directory in FOLDERS.values():
        if not os.path.exists(directory):
            os.makedirs(directory)       
            
# local filename creation functions
def _make_path(filename, dir_type):
   return os.path.join(FOLDERS[dir_type], filename)  
   
# wrappers for paths creation using year
def make_path_error_log(year):
    filename = "errors_{}.txt".format(str(year))
    return _make_path(filename, "error_log")
   
def make_path_parsed_csv(year):
    filename = "parsed_" + str(year) + ".csv"
    return _make_path(filename, "csv")

# inn subset
def get_inn_list_path():
    return _make_path('inn.txt', "inn_subsets") 
    
def make_path_inn_csv(year):
    filename = "inn_subset_" + str(year) + ".csv"
    return _make_path(filename, "inn_subsets")    