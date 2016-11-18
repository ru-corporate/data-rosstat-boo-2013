# -*- coding: utf-8 -*-
"""Read Rosstat CSV file and adjust data and write to clean CSV files.

Dataset(year).create_clean_copy()
df = Dataset(year).read_df()

"""

import csv
import os

import pandas as pd

from remote import RemoteDataset
from row_parser import adjust_row, adjust_columns
from config import make_path, make_path_clean_csv, make_path_base_csv, from_test_folder
import config
from common import print_elapsed_time
from columns import VALID_SOURCE_CSV_ROW_WIDTH, RENAMER, COLNAMES

DELIM = ";"
SAMPLE_OUTPUT_CLEAN_CSV = make_path("sample.txt", dir_type="test")

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
    
def csv_stream(filename, sep=DELIM):
    with open(filename, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=sep) # may need to use encoding="cp1251"
        for row in spamreader:
            yield row
 
def get_csv_lines(filename, sep=DELIM):
    for row in csv_stream(filename, sep):
       if len(row) == VALID_SOURCE_CSV_ROW_WIDTH: #avoid reading last empty row and rows like ''Товарищество собственников жилья "Большевистская 111 "Б"',' 
           yield row
       else:
           print("Skipped row:", row)

def csv_block(filename, count, skip=0):
    k = 0 
    for i, row in enumerate(get_csv_lines(filename)):
        if i<skip:
            continue
        if k<count: 
            yield row
            k+=1
        else:
            break

def to_csv(path, gen, cols=None, sep=DELIM):    
    with open(path, 'w', encoding = "utf-8") as file:
        writer = csv.writer(file, delimiter=sep, lineterminator="\n", 
                            quoting=csv.QUOTE_MINIMAL)
        if cols:                    
            writer.writerow(cols)
        writer.writerows(gen)
    print("Saved file:", path)    
    return path 

def indicate_progress_by_chunk(gen, chunk, echo=True):
    i=1; k=0
    for r in gen:
        yield r        
        i+=1
        if i==chunk:
            i=0; k+=1
            if echo:
                print(chunk*k)

inn_position_in_row = 5
def extract_inn(row):
    return row[inn_position_in_row]

class Dataset():
    
    chunk = 100 * 1000 
    test_clean_csv = SAMPLE_OUTPUT_CLEAN_CSV 
    
    
    def add_inn_filter(self, inn_csv_filepath):
        self.inn_list = [r[0] for r in csv_stream(inn_csv_filepath, sep=",")]
        return self
    
    def __init__(self, year, custom_spec=None):        
        self.columns=adjust_columns()
        self.year=year
        if custom_spec:            
            self.input_csv=custom_spec['inc']
            self.clean_csv=custom_spec['out']
            self.sliced_csv=custom_spec['df']
        else:
            self.input_csv=RemoteDataset(year, silent=True).download().unrar()            
            self.clean_csv=make_path_clean_csv(year)
            self.sliced_csv=make_path_base_csv(year)

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
        to_csv(filename, gen, COLNAMES, sep="\t")
        return 1 # success code 

    def raw_rows(self, n=None, skip=0):
        if n:
            gen = csv_block(self.input_csv, n, skip)
        else:
            gen = get_csv_lines(self.input_csv)            
        for r in indicate_progress_by_chunk(gen, self.chunk):
            yield r
        
    def parsed_rows(self, n=None, skip=0):
        for r in self.raw_rows(n, skip):
            ar = adjust_row(r, self.year)
            if ar: 
               yield ar
             
    @print_elapsed_time            
    def create_clean_copy(self, overwrite=False):
        file_exists = os.path.exists(self.clean_csv)
        if file_exists and overwrite is False:
            print("CSV already exists:", self.clean_csv)
        if not file_exists or overwrite is True:
            print("\nCleaning csv")
            print("Year:", self.year)
            print("Writing file:", self.clean_csv)            
            gen = self.parsed_rows()
            to_csv(self.clean_csv, gen, self.columns)
        return 1 # success code 
        
    def demo(self, m=10):    
        print("\nSample contents") 
        print("Year:", self.year)
        for row in self.parsed_rows(m,skip=0):        
            print(" ".join(str(x) for x in row[0:4]))
        gen=self.parsed_rows(m,skip=0)
        path = to_csv(self.test_clean_csv, gen, self.columns)
        return path
        
    def peek(self, skip=0):
        return next(self.parsed_rows(1,skip))
     

    def _read_fullcolumn_df(self):
        print("Reading {} full column dataframe...".format(str(self.year)))
        return custom_df_reader(self.clean_csv)
    
    @print_elapsed_time    
    def read_df(self, subset='columns.RENAMER'): 
        print("Reading {} dataframe...".format(str(self.year)))
        return pd.read_csv(self.sliced_csv)
        
    @print_elapsed_time    
    def make_df(self): 
       print("\nCreating base dataframe and dumping it to csv") 
       df = custom_df_reader(self.clean_csv)         
       new_cols= ['year', 'title', 'inn', 'ok1', 'ok2', 'ok3', 'region'] \
                  + list(RENAMER.keys())                     
       df2 = df[new_cols].rename(columns=RENAMER)
       print("Writing to csv:", self.sliced_csv)
       df2.to_csv(self.sliced_csv, encoding="utf-8", index = False)
       return df2

    # maybe - make slicved df from source csv without dump?

if __name__=="__main__":
    # from config import TEST_RAW_CSV 
    # spec = dict(year='2015',
                # inc=TEST_RAW_CSV, 
                # out=from_test_folder("brushed_csv_test.csv"),
                # df=from_test_folder("df_test.csv")
                # )
    # ds = Dataset(custom_spec=spec)
    # assert 1 == ds.create_clean_copy(True)
    # ds.make_df()
    # ds.peek()
    # ds.demo()
    # df = ds.read_df()        
    # print(df[0:4].transpose())
    
    # fn = from_test_folder("inn.txt")
    # fn2 = from_test_folder("rows.txt")
    # fn3 = from_test_folder("rows.xlsx")
    # ds = Dataset(2015).add_inn_filter(fn)
    # ds.filter_raw_rows_to_csv(fn2)
    # df = ds.make_df()
    # df.to_excel(fn3)
    
    #fn = config.from_inn_folder("inn.txt")
    #ds = Dataset(2015).add_inn_filter(fn)
    #gen = list(ds.filter_raw_rows())
    
    fn2 = config.from_inn_folder("inn_rows.txt")
    #to_csv(fn2, gen, cols=COLNAMES)
    fn3 = config.from_inn_folder("inn_rows_clean.txt")
    fn4 = config.from_inn_folder("inn_rows_df.txt")
    spec = dict(inc=fn2, 
                out=fn3,
                df=fn4)        
    ds2=Dataset(2015, custom_spec=spec)
    ds2.create_clean_copy(overwrite=True)
    df=ds2.make_df()
    fn4 = config.from_inn_folder("projects.xlsx")
    df.to_excel(fn4)
     