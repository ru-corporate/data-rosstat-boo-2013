#
#   config.py
#

from config import _make_path

def test_make_path():
    assert "data\\csv\\123.csv" == _make_path("123.csv", "csv")
    
    
def test_folder_funcs():  
    for path_func in [get_subset_root_folder, get_raw_csv_folder, get_rar_folder]:
        assert os.path.exists(path_func())

    for file in [make_path_parsed_csv(year) for year in VALID_YEARS]:
        assert isinstance(file, str)   
