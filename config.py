# -*- coding: utf-8 -*-
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
FOLDERS=dict(rar         = os.path.join("data","source","rar")
           , raw_csv     = os.path.join("data","source","raw","dirty")
           , clean_csv   = os.path.join("data","source","raw","clean")
           , error_log   = os.path.join("data","source","raw","errors")
           , base_csv    = os.path.join("data","source","csv_base")
           , inn_subsets = os.path.join("data","inn")
           , user_slices = os.path.join("data")
           , test        = os.path.join("data","test")           
        )

def raw_csv_folder():
    """Path to raw csv folder"""
    return FOLDERS['raw_csv']        
        
# create directories if not exist
for directory in FOLDERS.values():
    if not os.path.exists(directory):
        os.makedirs(directory)

# local filename creation functions
def make_path(filename, dir_type):
   return os.path.join(FOLDERS[dir_type], filename)  

# wrappers for path creation
def from_rar_folder(filename):
    return make_path(filename, dir_type='rar')

def from_raw_csv_folder(filename):
    return make_path(filename, dir_type='raw_csv')

def from_test_folder(fn):
    return make_path(fn, dir_type='test')

def from_inn_folder(fn):
    return make_path(fn, dir_type='inn_subsets')

# wrappers for paths using year
def make_path_error_log(year):
    filename = "errors_{}.txt".format(str(year))
    return make_path(filename, "error_log")
    
def make_path_clean_csv(year):
    filename = "rosstat_" + str(year) + ".csv"
    return make_path(filename, "clean_csv")        

def make_path_base_csv(year):
    filename = "base_" + str(year) + ".csv"
    return make_path_for_user_output(year, 'base', ext=".csv")

def make_path_for_user_output(year, prefix, ext=".csv"):
    filename = prefix + "_" + str(year) + ext
    return os.path.join(FOLDERS['user_slices'], filename)  

    
# move to different file?     
TEST_RAW_CSV = from_test_folder("raw_csv_test.csv")