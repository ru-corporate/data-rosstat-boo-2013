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
from config import make_path, make_path_clean_csv, make_path_base_csv
from common import print_elapsed_time
from columns import VALID_SOURCE_CSV_ROW_WIDTH, RENAMER

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
    

def get_csv_lines(filename):
    with open(filename, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=DELIM) # may need to use encoding="cp1251"
        for row in spamreader:
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

def to_csv(path, gen, cols):    
    with open(path, 'w', encoding = "utf-8") as file:
        writer = csv.writer(file, delimiter=DELIM, lineterminator="\n", 
                            quoting=csv.QUOTE_MINIMAL)
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

class Dataset():
    
    chunk = 100 * 1000 
    test_clean_csv = SAMPLE_OUTPUT_CLEAN_CSV 
    
    def __init__(self, year=None, custom_spec=None):        
        self.columns=adjust_columns()
        if year:        
            self.year=year
            self.input_csv=RemoteDataset(year, silent=True).download().unrar()            
            self.output_csv=make_path_clean_csv(year)
            self.sliced_csv=make_path_base_csv(year)        
        elif custom_spec:            
            self.year=custom_spec['year']
            self.input_csv=custom_spec['inc']
            self.output_csv=custom_spec['out']
            self.sliced_csv=custom_spec['df']
        else:
            raise ValueError("Must specify 'year' or 'custom_spec' for Dataset() instance")
                
    def parsed_rows(self, n=None, skip=0):
        if n:
            gen = csv_block(self.input_csv, n, skip)
        else:
            gen = get_csv_lines(self.input_csv)            
        for r in indicate_progress_by_chunk(gen, self.chunk):
            ar = adjust_row(r, self.year)
            if ar: 
               yield ar
             
    @print_elapsed_time            
    def create_clean_copy(self, overwrite=False):
        file_exists = os.path.exists(self.output_csv)
        if file_exists and overwrite is False:
            print("CSV already exists:", self.output_csv)
        if not file_exists or overwrite is True:
            print("\nCleaning csv")
            print("Year:", self.year)
            print("Writing file:", self.output_csv)            
            gen = self.parsed_rows()
            to_csv(self.output_csv, gen, self.columns)
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
        
    @print_elapsed_time    
    def read_df(self, subset='columns.RENAMER'): 
        print("\nReading dataframe...")
        if subset == 'all':
            return custom_df_reader(self.output_csv)         
        elif subset == 'columns.RENAMER':
            return pd.read_csv(self.sliced_csv)
        else:
            raise ValueError('Use .read_df(subset=\'all\') for full dataset,' +
                                ' .read_df() otherwise')
    
    @print_elapsed_time    
    def make_df(self): 
       print("\nCreating base dataframe and dumping it to csv") 
       df = custom_df_reader(self.output_csv)         
       new_cols= ['year', 'title', 'inn', 'ok1', 'ok2', 'ok3', 'region'] \
                  + list(RENAMER.keys())                     
       df2 = df[new_cols].rename(columns=RENAMER)
       print("Writing to csv:", self.sliced_csv)
       df2.to_csv(self.sliced_csv, encoding="utf-8", index = False)
       return df2

    # maybe - make slicved df from source csv without dump?

if __name__=="__main__":
    from config import TEST_RAW_CSV 
    #    self.year=custom_spec['year']
    #    self.input_csv=custom_spec['in']
    #    self.output_csv=custom_spec['out']
    #    self.sliced_csv=custom_spec['df']            
    spec = dict(year='2015',
                inc=TEST_RAW_CSV, #make_path("raw_csv_test.csv", dir_type='test'),
                out=make_path("brushed_csv_test.csv", dir_type='test'),
                df=make_path("df_test.csv", dir_type='test'))
    ds = Dataset(custom_spec=spec)
    ds.create_clean_copy(True)
    ds.make_df()
    ds.peek()
    ds.demo()
    df = ds.read_df()        
    print(df[0:4].transpose())
    
    
#Uncomment below to create rosstat datasets
    #Dataset(2015).save()
    #Dataset(2014).save()
    #Dataset(2013).save()
    #Dataset(2012).save()
    
# peek into dataset 
    #dataset = Dataset(2015)
    #dataset.demo()
    #print(dataset.peek(361))