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

    
def test_subset_files():    
    c1 = folders.SubsetIncludeINNs(TEST_SUBSET).filepath()
    c2 = folders.SubsetExcludeINNs(TEST_SUBSET).filepath()
    c3 = folders.SubsetParsedCSV(2015, TEST_SUBSET).filepath()
    assert isinstance(c1, str)
    assert isinstance(c2, str)
    assert isinstance(c3, str)
    