#
#   Testing remote.py 
#

import os
from config import VALID_YEARS, FOLDERS 
from remote import RawDataset

def test_config_folders():
    set1 = set(FOLDERS.keys()) 
    set2 = set(['rar', 'raw_csv', 'csv', 'error_log', 'inn_subsets', 'test', 'user_slices'])
    assert set1==set2

def test_download_and_unrar():
    for year in VALID_YEARS:
        if year == 2015:
           fn1 = RawDataset(year).download().unrar()
           fn2 = RawDataset(year).get_filename()
           assert fn1 == fn2  
           assert os.path.exists(fn1)

def test_rar_content():    
    for fn in [RawDataset(year)._rar_content() for year in VALID_YEARS]:
        assert isinstance(fn, str) 