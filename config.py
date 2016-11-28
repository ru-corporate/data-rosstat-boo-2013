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
URL = {2012: "http://www.gks.ru/opendata/storage/7708234640-bdboo2012/data-20161021t000000-structure-20121231t000000.rar",
       2013: "http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20161021t000000-structure-20131231t000000.rar",
       2014: "http://www.gks.ru/opendata/storage/7708234640-bdboo2014/data-20161021t000000-structure-20141231t000000.rar",
       2015: "http://www.gks.ru/opendata/storage/7708234640-bdboo2015/data-20161021t000000-structure-20151231t000000.rar"
       }

VALID_YEARS = [2012, 2013, 2014, 2015]

# folder tree for csv's and archives
FOLDERS = dict(rar        = os.path.join("data", "temp", "rar")
           , raw_csv      = os.path.join("data", "temp", "raw_csv")
           , csv          = os.path.join("data", "csv")
           , errors       = os.path.join("data", "temp", "errors")
           , subset       = os.path.join("data", "subset")
               )

def make_dirs():
    """Create directories from FOLDERS if they do not exist."""
    for directory in FOLDERS.values():
        if not os.path.exists(directory):
            os.makedirs(directory)
               
class Path():

   def __init__(self, filename):
       self.fn = filename
    
   def in_rar(self):
       """Put file in RAR folder"""
       # Used in remote.py
       return os.path.join(FOLDERS['rar'], self.fn)
       
   def in_raw_csv(self):
       """Put file in RAW CSV folder"""
       # Used in remote.py
       return os.path.join(FOLDERS['raw_csv'], self.fn)

      
class ParsedCSV():
   def __init__(self, year):
      fn = "parsed_" + str(year) + ".csv"  
      self.path = os.path.join(FOLDERS["csv"], fn)             
   def get_filename(self):
      return self.path 

      
class ErrorLog():# not in use   
   def __init__(year): 
        filename = "errors_{}.txt".format(str(year))
        self.path = os.path.join(FOLDERS["errors"], filename)                  
   def get_filename(self):
      return self.path 

        
class Folder():
     def __init__(self, base, tag):
        self.folder = os.path.join(base, tag)
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)
     def get_path(self, fn):    
            return os.path.join(self.folder, fn)
            
class SubsetLocation():
   
    ROOT = FOLDERS['subset']
    SUBSETS = ['test1']

    def check(self, tag):
        if tag not in self.SUBSETS:
            msg = "\nSubset name not allowed: " + tag + \
                  "\nAllowed name(s): " + ", ".join(self.SUBSETS)
            raise ValueError(msg)    

    def __init__(self, year, tag):        
        self.check(tag)            
        self.f = Folder(self.ROOT, tag)             
        self.output_csv = self.f.get_path("{0}_{1}.csv".format(tag, str(year)))
        
    def get_output_csv(self):
        return self.output_csv      
        
    def get_inn_paths(self):    
        return self.f.get_path("include.csv"), self.f.get_path("exclude.csv")
