# -*- coding: utf-8 -*-
"""Directory structure and file locations."""

import os

# subfolder names in TREE['subset']
TEST_SUBSET = 'test1' # used in testing
SUBSETS = [TEST_SUBSET, 'subset1']

# folder tree for csv's, archives and subsets
TREE = dict(rar   = os.path.join("data", "temp", "rar")
        , raw_csv = os.path.join("data", "temp", "raw_csv")
        , errors  = os.path.join("data", "temp", "errors")
        , csv     = os.path.join("data", "csv")
        , subset  = os.path.join("data", "subset")
        )
        
        
def is_valid_subset_name(tag):
    if not tag in SUBSETS:
        msg1 = "\nSubset name not allowed: " + tag 
        msg2 = "\nAllowed name(s): " + ", ".join(SUBSETS)
        raise ValueError(msg1+msg2)
    else:
        return True 

        
def is_valid_folder_tag(tag):  
     if not tag in TREE.keys():
         msg1 = "\nWrong folder tag: " + str(tag)
         msg2 = "\nValid tags: " + ", ".join(TREE.keys())             
         raise ValueError(msg1+msg2)      
     else: 
         return True
         

def make_dirs(dir_list):
    """Create directories if they do not exist."""
    for directory in dir_list:
       if not os.path.exists(directory):
           os.makedirs(directory)
        
        
class Folder():
    
    folders = TREE
    make_dirs(folders.values()) 
               
    @classmethod
    def __all__(cls):
        return list(cls.folders.values())
    
    def __init__(self, tag):
        if is_valid_folder_tag(tag):
           self._path = self.folders[tag]
             
    def path(self):
        return self._path
        
    def filepath(self, filename):
        return os.path.join(self._path, filename)
     
    # Note:
    # methods .path() and .filepath() used in remote.py for 'rar' and 'raw_csv' folders
    
class ParsedCSV():

   def __init__(self, year):
      fn = "parsed_" + str(year) + ".csv"  
      self._path = os.path.join(Folder("csv").path(), fn)
      
   def filepath(self):
      return self._path 
      
class ErrorLog():

   def __init__(year): 
        filename = "errors_{}.txt".format(str(year))
        self.path = os.path.join(Folder("errors").path(), filename)                  
        
   def filepath(self):
      return self.path   

class SubsetFolder(Folder):      

     base_path = Folder('subset').path()
     
     def __init__(self, tag):
        if is_valid_subset_name(tag):
            self._path = os.path.join(self.base_path, tag)
            make_dirs([self._path])
      
class SubsetFiles():

    @staticmethod
    def make_filename(year, tag):
       return tag + "_" + str(year) + ".csv"
   
    def __init__(self, year, tag):        
        this_sf = SubsetFolder(tag)
        # output file
        csv_filename = self.make_filename(year, tag)
        self.output_csv = this_sf.filepath(csv_filename)
        # inn lists
        self.inn_list_tuple = (this_sf.filepath("include.csv"), 
                               this_sf.filepath("exclude.csv"))
        
    def get_output_csv(self):
        return self.output_csv      
        
    def get_inn_paths(self):    
        return self.inn_list_tuple
        
if __name__ == "__main__":
    d = Folder.__all__()
    a = ParsedCSV(2015)
    k = SubsetFolder(TEST_SUBSET)
    c1 = SubsetFiles(2015, TEST_SUBSET).get_output_csv()
    c2 = SubsetFiles(2015, TEST_SUBSET).get_inn_paths()
