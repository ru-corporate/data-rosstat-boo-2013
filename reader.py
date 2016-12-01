# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 12:50:03 2016

@author: Евгений
"""
import csv
import os
from itertools import islice
from collections import OrderedDict

import pandas as pd

import config

from inspect_columns import Columns
from remote import RawDataset
from row_parser import parse_row, get_parsed_colnames, get_colname_dtypes
from folders import ParsedCSV
from common import pipe, print_elapsed_time

COLUMNS = Columns.COLUMNS
RAW_CSV_FORMAT = dict(enc='windows-1251', sep=";")

#
# file location wrappers
#

def get_parsed_csv_path(year):
   return ParsedCSV(year).filepath()

   
def get_raw_csv_path(year):
   return RawDataset(year).get_filename()   

#
# row validation 
#

VALID_ROW_WIDTH = len(COLUMNS)
INN_POSITION = COLUMNS.index('inn')

def is_valid(row):
    """Return True if row is valid."""
    if len(row) != VALID_ROW_WIDTH:
        # todo: may use Logger
        # reason = "Invalid row length {}".format(str(len(row)))
        return False
    elif not row[INN_POSITION]:
        # reason = "Skipped row with empty INN field"
        return False
    else:
        return True


#
# column names
#

def emit_raw_colnames():
    """Column names corresponding to emit_raw_rows()."""
    return ['year'] + COLUMNS


def emit_parsed_colnames():
    return get_parsed_colnames()


#
# read, filter and parse raw csv
#

def csv_stream(filename, enc='utf-8', sep=','):
    """Emit CSV rows by filename."""
    if enc not in ['utf-8', 'windows-1251']:
        raise ValueError("Encoding not supported: " + str(enc))
    with open(filename, 'r', encoding=enc) as csvfile:
        spamreader = csv.reader(csvfile, delimiter=sep)
        for row in spamreader:
            yield row


def emit_raw_rows(year):
    """Emit raw rows by year."""
    fn = get_raw_csv_path(year)
    raws = filter(is_valid, csv_stream(fn, **RAW_CSV_FORMAT))
    add_year = lambda row: [year] + row
    return map(add_year, raws)


def emit_raw_dicts(year):
    columns = emit_raw_colnames()    
    # lambda func allow to make inline fucntion with one arguement
    as_dict = lambda row: OrderedDict(zip(columns, row))
    return map(as_dict, emit_raw_rows(year))


def emit_rows(year):
    gen = emit_raw_dicts(year)
    return map(parse_row, gen)


def emit_dicts(year):
    columns = emit_parsed_colnames()
    # lambda func allow to make inline fucntion with one arguement
    as_dict = lambda row: OrderedDict(zip(columns, row))
    return map(as_dict, emit_rows(year))


#
# output to csv file, read dataframe functions
#
@print_elapsed_time
def to_csv(path, stream, cols=None):
    with open(path, 'w', encoding="utf-8") as file:
        writer = csv.writer(file, lineterminator="\n",
                            quoting=csv.QUOTE_MINIMAL)
        if cols:
            writer.writerow(cols)
        writer.writerows(stream)
    print("Saved file:", path)
    return path


def custom_df_reader(file):
    """Read dataset as pandas dataframe using dtypes for faster import."""
    if os.path.exists(file):
        print("Reading file:", file)
        # dtype on all columns shortens reading time
        return pd.read_csv(file, dtype=get_colname_dtypes())
    else:
        raise FileNotFoundError(file)


#
# end user class for dataset access
#

class Dataset():

    def __init__(self, year):
        self.year = year
        self.output_csv = get_parsed_csv_path(year)

    def __colnames__(self):
        return get_parsed_colnames()

    def __get_stream__(self):
        return pipe(emit_rows(self.year))

    def to_csv(self, force=False):
        if not os.path.exists(self.output_csv) or force is True:
            msg = "\nSaving %s dataset..." % self.year            
            print(msg)
            to_csv(path=self.output_csv,
                   stream=self.__get_stream__(),
                   cols=self.__colnames__())
        else:
            print('{} dataset'.format(self.year),
                  'already saved as:', self.output_csv, "\n")

    @print_elapsed_time
    def read_df(self):
        print("Reading {} dataframe...".format(self.year))
        return custom_df_reader(self.output_csv)
        
    def nth(self, n=0):
        return next(islice(self.__get_stream__(), n, n + 1))
        
            

        
if __name__ == "__main__":
     #Subset(2015, 'test1').to_csv()
    z = next(emit_raw_rows(2015))

    columns = ['year'] + Columns.COLUMNS
    K=9    
    assert columns.index('report_type') + 1 == K
    bal_col = [x for x in columns if x[0] in ['1','2','4']]
    ix = [columns.index(x) for x in bal_col]    

    def split_row(x, k=K):
        date = x.pop(-1)
        text = x[:k] + [date]
        data = [x for i,x in enumerate(x) if i in ix]
        return text, data
         
    def to_tag(colname):
        return 'b'+colname
        
    text_colnames, data_colnames = split_row(columns)
    data_colnames = [to_tag(n) for n in data_colnames]  
    
    assert text_colnames[-1] == 'date'
    assert text_colnames[-2] == 'report_type'
    assert data_colnames[0] == 'b1110'
     
    from collections import namedtuple

    text_vals, data_vals = split_row(z, k=K)
    
    Text = namedtuple("Text", text_colnames)
    Data = namedtuple("Data", data_colnames)
    text = Text._make(text_vals)
    data = Data._make(data_vals)
    
    class Row():        
        def __init__(self, incoming_row):
            text_vals, data_vals = split_row(incoming_row)
            self.text = Text._make(text_vals)
            self.data = Data._make(data_vals)
        def get_data_value(self, colname):
            return getattr (self.data, to_tag(colname))
        def data_subset(self, fields):
            return [int(getattr(self.data, fld)) for fld in fields]
            
        
            
    row = Row(z)
    assert row.get_data_value('2200') == '-49052'
            
            
            
            

     
     
     
     
     