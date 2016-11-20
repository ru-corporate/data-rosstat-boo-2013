# -*- coding: utf-8 -*-
"""Read Rosstat CSV file and adjust data and write to clean CSV files.

Dataset(year).create_clean_copy()
df = Dataset(year).read_df()

"""

import csv
import os

import pandas as pd

from cleaner import Cleaner
import csv_access

from row_parser import adjust_row, adjust_columns
from config import make_path, make_path_clean_csv, make_path_base_csv, from_test_folder
import config
from common import print_elapsed_time
from columns import VALID_SOURCE_CSV_ROW_WIDTH, RENAMER, COLNAMES

SAMPLE_OUTPUT_CLEAN_CSV = make_path("sample.txt", dir_type="test")

from csv_access import DELIM

#@print_elapsed_time
def custom_df_reader(file):
    if os.path.exists(file):
        print("Reading file:", file)            
        # dtype on all columns improves reading time (from 120 sec to 87 sec on my machine)
        # todo: Column 11 mixed types error
        dtype_dict = {k:int for k in adjust_columns()}.update({'name':str, 'inn':str, 'region':str})
        return pd.read_csv(file, sep=DELIM, dtype=dtype_dict)
    else:
        raise FileNotFoundError(file) 
        
class Dataset():
    
    test_clean_csv = SAMPLE_OUTPUT_CLEAN_CSV 
    
    def __init__(self, year, custom_spec=None):        
        self.columns=adjust_columns()
        self.year=year
        if custom_spec:            
            self.input_csv=custom_spec['inc']
            self.adjusted_csv=custom_spec['out']
            self.sliced_csv=custom_spec['df']
        else:
            self.input_csv=Cleaner(year).run()            
            self.adjusted_csv=make_path_clean_csv(year)
            self.sliced_csv=make_path_base_csv(year)
            
    # Read by INN functionality        

    def add_inn_filter(self, inn_csv_filepath):
        self.inn_list = [r[0] for r in csv_access.csv_stream(inn_csv_filepath, sep=",")]
        return self
        
    def filter_raw_rows(self, n=None, skip=0):        
        for r in self.raw_rows(n, skip):
            inn = extract_inn(r)  
            if inn in self.inn_list:
                print("Found inn", inn)
                yield r
            else:
                pass            

    def filter_raw_rows_to_csv(self, filename):
        gen = self.filter_raw_rows()
        csv_access.to_csv(filename, gen, COLNAMES, sep="\t")
        return 1 # success code 
        
    # END -- Read by INN functionality        
        
    def raw_rows(self, n=None, skip=0):
        if n:
            gen = csv_access.csv_block(self.input_csv, n, skip)
        else:
            gen = csv_access.csv_stream(self.input_csv)            
        for r in csv_access.indicate_progress_by_chunk(gen):
            yield r
        
    def parsed_rows(self, n=None, skip=0):
        for r in self.raw_rows(n, skip):
            ar = adjust_row(r, self.year)
            if ar: 
               yield ar
             
    @print_elapsed_time            
    def adjust_by_row(self, overwrite=False):
        file_exists = os.path.exists(self.adjusted_csv)
        if file_exists and overwrite is False:
            print("Adjusted CSV already exists:", self.adjusted_csv)
        if not file_exists or overwrite is True:
            print("Year:", self.year)
            print("Writing adjusted CSV:", self.adjusted_csv)            
            gen = self.parsed_rows()
            csv_access.to_csv(self.adjusted_csv, gen, self.columns)
        return self.adjusted_csv # success code 
    
    # local copies  
    def create_local_files(self):
        self.adjust_by_row()
        self.make_df()
        return 1
    
    def demo(self, m=10):    
        print("\nSample contents") 
        print("Year:", self.year)
        for row in self.parsed_rows(m,skip=0):        
            print(" ".join(str(x) for x in row[0:4]))
        gen=self.parsed_rows(m,skip=0)
        path = csv_access.to_csv(self.test_clean_csv, gen, self.columns)
        return path
        
    def peek(self, skip=0):
        return next(self.parsed_rows(1,skip))
     

    # Dataframe functionality
    
    def read_fullcolumn_df(self):
        print("Reading {} full column dataframe...".format(str(self.year)))
        return custom_df_reader(self.adjusted_csv)
    
    def read_df(self): 
        print("Reading {} base dataframe...".format(str(self.year)))
        return pd.read_csv(self.sliced_csv)
        
    @print_elapsed_time    
    def make_df(self): 
       print("\nCreating base dataframe and dumping it to csv") 
       df = custom_df_reader(self.adjusted_csv)         
       new_cols= ['year', 'title', 'inn', 'ok1', 'ok2', 'ok3', 'region'] \
                  + list(RENAMER.keys())                     
       df2 = df[new_cols].rename(columns=RENAMER)
       print("Writing to csv:", self.sliced_csv)
       df2.to_csv(self.sliced_csv, encoding="utf-8", index = False)
       return 1

    # maybe - make slicved df from source csv without dump?    
    
if __name__=="__main__":
    TEST_RAW_CSV = from_test_folder("raw_csv_test.csv")    
    spec = dict(inc=TEST_RAW_CSV,
                out=from_test_folder("brushed_csv_test.csv"),
                df=from_test_folder("df_test.csv")
                )
    ds = Dataset(2000, custom_spec=spec)
    assert 1 == ds.create_local_files()
    ds.make_df()
    ds.peek()
    ds.demo()
    df = ds.read_df()        
    print(df[0:4].transpose())