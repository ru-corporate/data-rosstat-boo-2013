"""


- download and unpach raw csv file 
- create error-free raw csv file by line and log 'broken lines' in separate error file
- create a slice of error-free raw csv file by company inn numbers 
- save base csv file: 
 - subset columns in error-free raw csv file 
 - multiply numeric values 
 - add new columns (okved, dequote, year, region)
""" 

year = 2015
path_dirty_csv = download_unrar(year)
path_clean_csv, path_error_log = inspect_raw_csv(path_dirty_csv)
path_sliced_csv, path_error_log = inspect_raw_csv(path_dirty_csv, inn_list=[])

data:
- 2015
--- 2015.rar
--- temp_2015.csv
--- source_2015.csv
--- errors_2015.csv
--- base_2015.csv
--- inn1
----- inn1.txt
----- _source_inn_2015.csv
----- base_veb_2015.csv

