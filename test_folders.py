import os
import pytest

import folders
from folders import TEST_SUBSET

def test_config_folders():
    set1 = set(folders.TREE.keys())
    set2 = set(['rar', 'raw_csv', 'csv', 'errors', 'subset'])
    assert set1==set2
    

def test_folders_exist():
    for f in folders.Folder.__all__():
        assert os.path.exists(f)
        

def test_wrong_tag():
    with pytest.raises(ValueError):
        _ = folders.Folder("blue")

def test_is_valid():
    with pytest.raises(ValueError):
         folders.is_valid_subset_name('blue')         
    with pytest.raises(ValueError):
         folders.is_valid_folder_tag('blue')        
        
def test_ParsedCSV():
    d = folders.ParsedCSV(2015).filepath()
    assert isinstance(d, str)
    assert '2015' in d
    assert 'parsed' in d
    assert '.csv' in d
   
def test_subset_folders():
    k = folders.SubsetFolder(TEST_SUBSET)
    assert os.path.exists(k.base_path)
    assert os.path.exists(k.path())

class SubsetParsedCSV(ParsedCSV):
   def __init__(self, year, tag):
      fn = tag + "_" + str(year) + ".csv"
      self._path = os.path.join(SubsetFolder(tag).path(), fn)
      
      
class SubsetIncludeINNs(ParsedCSV):
    def __init__(self, tag):        
        self._path = SubsetFolder(tag).filepath(INCLUDE_INNS_FILENAME)

        
class SubsetExcludeINNs(ParsedCSV):    
    
    
    
def test_subset_files():    
    c1 = folders.SubsetFiles(2015, TEST_SUBSET).get_output_csv()
    c2 = folders.SubsetFiles(2015, TEST_SUBSET).get_inn_paths() 
    assert isinstance(c1, str)
    assert isinstance(c2, tuple)
    