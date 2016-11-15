# -*- coding: utf-8 -*-
import os
import platform

# archives to download from Rosstat
URL={2012:"http://www.gks.ru/opendata/storage/7708234640-bdboo2012/data-20161021t000000-structure-20121231t000000.rar"
   , 2013:"http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20161021t000000-structure-20131231t000000.rar"
   , 2014:"http://www.gks.ru/opendata/storage/7708234640-bdboo2014/data-20161021t000000-structure-20141231t000000.rar"
   , 2015:"http://www.gks.ru/opendata/storage/7708234640-bdboo2015/data-20161021t000000-structure-20151231t000000.rar"     
}

VALID_YEARS = [2012,2013,2014,2015] 

# folder tree for csv's and archives
FOLDERS=dict(rar         = os.path.join("data","source","rar")
           , raw_csv     = os.path.join("data","source","raw")
           , clean_csv   = os.path.join("data","source","csv")
           , inn_subsets = os.path.join("data","inn")
           , user_slices = os.path.join("data")
           , test        = os.path.join("data","source","test")           
        )

def get_local_path(filename, dir_type='user_slices'):
   return os.path.join(FOLDERS[dir_type], filename)  

def get_clean_csv_path(year):
    filename = "rosstat_" + str(year) + ".csv"
    return get_local_path(filename, "clean_csv")        
         
TEST_RAW_CSV = get_local_path("raw_csv_test.csv", dir_type='test')         
         
# create directories if not exist
for directory in FOLDERS.values():
    if not os.path.exists(directory):
        os.makedirs(directory)

# RAR executable
IS_WINDOWS = (platform.system() == 'Windows')

if IS_WINDOWS:
    UNPACK_RAR_EXE = os.path.join('bin', 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'

##common utility function
#def print_elapsed_time(func):
#    def wrapper(**kwargs):
#        start_time = time.time()
#        result = func(**kwargs) 
#        print("Time elapsed: %.1f seconds" % (time.time()-start_time)) 
#        return result
#    return wrapper





