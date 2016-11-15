# -*- coding: utf-8 -*-
"""Read Rosstat CSV file and adjust data and write to clean CSV files

boo_rosstat_2012.csv"

"""

import csv
import os
import time

import pandas as pd

from remote import RemoteDataset
from row_parser import adjust_row, adjust_columns
from config import get_local_path, get_clean_csv_path #, print_elapsed_time
from columns import VALID_SOURCE_CSV_ROW_WIDTH

DELIM = ";"
CLEAN_SAMPLE_CSV = get_local_path("sample.txt", dir_type="test")

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

def indicate_progress_by_chunk(gen, chunk=1000, echo=True):
    i=1; k=0
    for r in gen:
        yield r        
        i+=1
        if i==chunk:
            i=0; k+=1
            if echo:
                print(chunk*k)

# dtype on all columns improves reading time from 120 sec to 87 sec
DTYPE_DICT = {k:int for k in adjust_columns()}.update({'name':str, 'inn':str, 'region':str})

def custom_df_reader(file):
    return pd.read_csv(file, sep=DELIM, dtype=DTYPE_DICT)
    
class Dataset():
    
    chunk = 100 * 1000 
    test_clean_csv = CLEAN_SAMPLE_CSV
    
    def __init__(self, year=None, source_csv=''):
        if year:        
            self.year=year            
        if os.path.exists(source_csv):
            self.input_csv=source_csv
        else:            
            self.input_csv=RemoteDataset(year, silent=True).download().unrar()
        self.output_csv = get_clean_csv_path(year)
        self.columns=adjust_columns()
        
        
    def parsed_rows(self, n=None, skip=0):
        if n:
            gen = csv_block(self.input_csv, n, skip)
        else:
            gen = get_csv_lines(self.input_csv)            
        for r in indicate_progress_by_chunk(gen, self.chunk):
            ar = adjust_row(r, self.year)
            if ar: 
               yield ar
             
    #todo: make print_elapsed_time a decorator           
    #@print_elapsed_time            
    def create_clean_copy(self, overwrite=False):
        file_exists = False; verb="Writing file:"
        if os.path.exists(self.output_csv):
            print("CSV already exists:", self.output_csv)
            file_exists = True
            verb="Overwriting file:"
        if not file_exists or overwrite is True:
            print(verb, self.output_csv)
            start_time = time.time()
            gen = self.parsed_rows()
            to_csv(self.output_csv, gen, self.columns)
            print("Time elapsed: %.1f seconds" % (time.time()-start_time))            
        return self   
        
    #@print_elapsed_time
    def demo(self, m=10):        
        for row in self.parsed_rows(m,skip=0):        
            print(" ".join(str(x) for x in row[0:4]))
        gen=self.parsed_rows(m,skip=0)
        to_csv(self.test_clean_csv, gen, self.columns)
        return self.test_clean_csv
        
    def peek(self, skip=0):
        return next(self.parsed_rows(1,skip))
        
    #@print_elapsed_time    
    def read_df(self):
        if os.path.exists(self.output_csv):
            start_time = time.time()
            print("Reading file:", self.output_csv)
            # custom pandas reader for clean CSV files
            df = custom_df_reader(self.output_csv)
            print("Time elapsed: %.1f seconds" % (time.time()-start_time))         
            return df
        else:
            raise FileNotFoundError(self.output_csv) 
        
        
if __name__=="__main__":
    pass

#Uncomment below to create rosstat datasets
    #Dataset(2015).save()
    #Dataset(2014).save()
    #Dataset(2013).save()
    #Dataset(2012).save()
    
# peek into dataset 
    #dataset = Dataset(2015)
    #dataset.demo()
    #print(dataset.peek(361))