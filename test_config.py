#
#   config.py
#

import config
import os


def test_config_folders():
    set1 = set(config.FOLDERS.keys())
    set2 = set(['rar', 'raw_csv', 'csv', 'error_log', 'subset'])
    assert set1==set2

def test_make_path():
    assert "data\\csv\\123.csv" == config._make_path("123.csv", "csv")
    
    
def test_folder_funcs():  
    for path_func in [config.get_subset_root_folder, config.get_raw_csv_folder, config.get_rar_folder]:
        assert os.path.exists(path_func())
        

def test_csv_output_files():          
    for file in [config.make_path_parsed_csv(year) for year in config.VALID_YEARS]:
        assert isinstance(file, str)   
