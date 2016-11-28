#
#   config.py
#

import config
import os


def test_config_folders():
    set1 = set(config.FOLDERS.keys())
    set2 = set(['rar', 'raw_csv', 'csv', 'errors', 'subset'])
    assert set1==set2
    
    
def test_folder_funcs():  
    for path_func in [config.Path('').in_rar, config.Path('').in_raw_csv]:
        assert os.path.exists(path_func())
        

def test_csv_output_files():          
    for file in [config.ParsedCSV(year).get_filename() for year in config.VALID_YEARS]:
        assert isinstance(file, str)   
