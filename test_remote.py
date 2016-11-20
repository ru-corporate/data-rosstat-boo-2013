#
#   Testing remote.py 
#

import os
from config import VALID_YEARS 
from remote import RemoteDataset

def test_download_and_unrar():
    for year in VALID_YEARS:
        fn = RemoteDataset(year).download().unrar()
        assert os.path.exists(fn)

def test_rar_content():    
    for fn in [RemoteDataset(year).rar_content() for year in VALID_YEARS]:
        assert isinstance(fn, str) 