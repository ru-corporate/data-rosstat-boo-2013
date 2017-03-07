# -*- coding: utf-8 -*-
"""Directory structure and file locations."""

import os

# folder tree for rar archives and csv's
root = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(root, "data")
TREE = dict(rar   = os.path.join(data_dir, "downloaded_rar")
        , raw_csv = os.path.join(data_dir, "downloaded_csv")
        , errors  = os.path.join(data_dir, "parsing_errors")
        , csv     = os.path.join(data_dir, "parsed_csv")
        )        
        
        
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

# used in remote.py
RarFolder = Folder('rar')        
RawCsvFolder = Folder('raw_csv')        
   
class ParsedCSV():

   def __init__(self, year):
      fn = "parsed_" + str(year) + ".csv"  
      self._path = Folder("csv").filepath(fn)
      
   def filepath(self):
      return self._path 
      
class ErrorLog(ParsedCSV):

   def __init__(self, year): 
        filename = "errors_{}.txt".format(str(year))
        self._path = Folder("errors").filepath(filename)                  

       
if __name__ == "__main__":
    d = Folder.__all__()
    a = ParsedCSV(2015)
    b = ErrorLog(2015)