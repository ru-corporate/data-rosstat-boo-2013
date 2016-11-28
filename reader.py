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
from common import pipe, print_elapsed_time

COLUMNS = Columns.COLUMNS
RAW_CSV_FORMAT = dict(enc='windows-1251', sep=";")

#
# row validation function
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
    fn = RawDataset(year).get_filename()
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
        self.output_csv = config.make_path_parsed_csv(year)

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
        
            


#
# filter by INN
#


def read_inns(path):
    gen = csv_stream(path)
    return list(r[0].replace("\ufeff", "") for r in gen 
                if not r[0].startswith("#"))

class SubsetLocation():
   
    ROOT = config.get_subset_root_folder()
    SUBSETS = ['test1']     

    def __init__(self, year, tag):
        
        if tag not in self.SUBSETS:
            msg = "\nSubset name not allowed: " + tag + \
                  "\nAllowed name(s): " + ", ".join(self.SUBSETS)
            raise ValueError(msg)
            
        folder = os.path.join(self.ROOT, tag)
        if not os.path.exists(folder):
            os.makedirs(folder)
            
        def in_dir(fn):    
            return os.path.join(folder, fn)
             
        self.output_csv = in_dir(tag+"_"+str(year)+".csv")
        include_csv_path = in_dir("include.csv")
        exclude_csv_path = in_dir("exclude.csv")

        if os.path.exists(include_csv_path):
            self.includes = read_inns(include_csv_path)
        else:
            self.includes = []
            
        if os.path.exists(exclude_csv_path):
            self.excludes = read_inns(exclude_csv_path)
        else:
            self.excludes = []            

    def get_inn_lists(self):
        return self.includes, self.excludes

    def get_output_csv(self):
        return self.output_csv  


class Subset(Dataset):
    
    def __init__(self, year, tag):
        self.year=year    
        loc = SubsetLocation(year, tag)
        self.output_csv=loc.get_output_csv()
        self._inc, self._exc = loc.get_inn_lists()
            
    def __get_stream__(self):
        return pipe(emit_rows_by_inn(self.year, include=self._inc,
                                                exclude=self._exc))
                                                
    def include(self, inc):
        self._inc = inc  
        return self                                        
    
    def exclude(self, ex):
        self._exc = ex
        return self

def emit_rows_by_inn(year, include, exclude):
    gen = emit_raw_dicts(year)
    print("INNs to include:", include)
    print("INNs to exclude:", exclude)
    gen = inn_mask(include, exclude).apply(gen) 
    return map(parse_row, gen)

class inn_mask():
    
    def ok_to_include(self, row):
        inn = row['inn']
        if inn in self.inns:
            print("Found INN:", inn)
            return True
        else:
            return False
            
    def ok_to_exclude(self, row):
        inn = row['inn']
        if inn in self.inns:
            print("Rejected INN:", inn)
            return False
        else:
            return True      
    
    def __init__(self, il=None, el=None):
        if el and il:
            self.inns = [x for x in il if x not in el]            
            self.f = self.ok_to_include
        elif il:
            self.inns = il
            self.f = self.ok_to_include
        elif el:
            self.inns = el
            self.f = self.ok_to_exclude
        else:
            raise ValueError() 
            
    def apply(self, gen):
        return filter(self.f, pipe(gen))
      
if __name__ == "__main__":
     Subset(2015, 'test1').to_csv()
     z = next(emit_rows(2015))

    