#
# filter by INN
#
import os

from common import pipe
from folders import SubsetIncludeINNs, SubsetExcludeINNs, SubsetParsedCSV
from reader import csv_stream, emit_raw_dicts, Dataset
from row_parser import parse_row


def emit_rows_by_inn(year, include, exclude):
    gen = emit_raw_dicts(year)
    if include:
        print("INNs to include:", include)
        f = lambda row: row_in_list(row, include, "Found INN:")
        gen = filter(f, gen)
    if exclude:
        print("INNs to exclude:", exclude)
        f = lambda row: not row_in_list(row, exclude, "Rejected INN:") 
        gen = filter(f, gen) 
    return map(parse_row, gen)
    
    
def row_in_list(row, inn_list, found_msg):
    inn = row['inn']
    if inn in inn_list:
       print(found_msg, inn)
       return True
    else:
       return False

       
def read_inns(path):
    return list(r[0].replace("\ufeff", "") for r in csv_stream(path)
                if not r[0].startswith("#"))


def read_if_exists(path):
    if os.path.exists(path):
        return read_inns(path)
    else:
        return []  
       
       
class Subset(Dataset):
    
    def __init__(self, year, tag):
        self.year = year            
        self.output_csv = SubsetParsedCSV(year, tag).filepath()
        self.inc = read_if_exists(SubsetIncludeINNs(tag).filepath())
        self.exc = read_if_exists(SubsetExcludeINNs(tag).filepath())      
            
    def __get_stream__(self):
        return pipe(emit_rows_by_inn(year=self.year, 
                                     include=self.inc,
                                     exclude=self.exc))
    def include(self, inc):
        self.inc = inc  
        return self
    
    def exclude(self, exc):
        self.exc = exc
        return self
        