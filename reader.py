# -*- coding: utf-8 -*-
"""Read Rosstat CSV file and adjust data and write to clean CSV files.

Dataset(year).create_clean_copy()
df = Dataset(year).read_df()

"""

import csv
import os

import pandas as pd

import config
import csv_access
from common import print_elapsed_time
from row_parser import adjust_row, adjust_columns
from columns import RENAMER
from cleaner import Cleaner
from csv_access import DELIM

SAMPLE_OUTPUT_CLEAN_CSV = config.from_test_folder("sample.txt")

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
    
    def __init__(self, year):        
        self.columns=adjust_columns()
        self.year=year
        self.input_csv=Cleaner(year).get_filename()            
        self.adjusted_csv=config.make_path_adjusted_csv(year)
        self.sliced_csv=config.make_path_sliced_csv(year)
        
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

    # demo and testing        
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
     
            
    # main job to build two csv files
    def rebuild_local_files(self):
        self.make_adjusted_csv()
        self.make_df_dump()
        return 1

    @print_elapsed_time    
    def make_adjusted_csv(self):
        print("\nYear:", self.year)
        print("Writing adjusted CSV:", self.adjusted_csv)            
        gen = self.parsed_rows()
        csv_access.to_csv(self.adjusted_csv, gen, self.columns)
        return self.adjusted_csv  

    @print_elapsed_time    
    def make_df_dump(self): 
       print("\nYear:", self.year)
       print("Slicing dataframe by column and saving to CSV...") 
       df = custom_df_reader(self.adjusted_csv)         
       new_cols= ['year', 'title', 'inn', 'ok1', 'ok2', 'ok3', 'region'] \
                  + list(RENAMER.keys())                     
       df2 = df[new_cols].rename(columns=RENAMER)
       print("Writing to csv:", self.sliced_csv)
       df2.to_csv(self.sliced_csv, encoding="utf-8", index = False)
       return df2       
        
    # dataframe functionality
    @print_elapsed_time
    def read_fullcolumn_df(self):
        print("Reading {} full column dataframe...".format(str(self.year)))
        return custom_df_reader(self.adjusted_csv)
    
    @print_elapsed_time    
    def read_df(self): 
        print("Reading {} dataframe...".format(str(self.year)))
        return pd.read_csv(self.sliced_csv)
       
if __name__=="__main__":
    df = Dataset(2015).read_df()