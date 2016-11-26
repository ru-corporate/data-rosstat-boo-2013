# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 12:50:03 2016

@author: Евгений
"""
import csv
from itertools import islice 
from collections import OrderedDict

from remote import RawDataset
from row_parser import parse_row, get_parsed_colnames

#from inspect_columns import Columns
COLUMNS=['name', 'okpo', 'okopf', 'okfs', 'okved', 'inn', 'unit', 'report_type', '1110', '1110_lag', '1120', '1120_lag', '1130', '1130_lag', '1140', '1140_lag', '1150', '1150_lag', '1160', '1160_lag', '1170', '1170_lag', '1180', '1180_lag', '1190', '1190_lag', '1100', '1100_lag', '1210', '1210_lag', '1220', '1220_lag', '1230', '1230_lag', '1240', '1240_lag', '1250', '1250_lag', '1260', '1260_lag', '1200', '1200_lag', '1600', '1600_lag', '1310', '1310_lag', '1320', '1320_lag', '1340', '1340_lag', '1350', '1350_lag', '1360', '1360_lag', '1370', '1370_lag', '1300', '1300_lag', '1410', '1410_lag', '1420', '1420_lag', '1430', '1430_lag', '1450', '1450_lag', '1400', '1400_lag', '1510', '1510_lag', '1520', '1520_lag', '1530', '1530_lag', '1540', '1540_lag', '1550', '1550_lag', '1500', '1500_lag', '1700', '1700_lag', '2110', '2110_lag', '2120', '2120_lag', '2100', '2100_lag', '2210', '2210_lag', '2220', '2220_lag', '2200', '2200_lag', '2310', '2310_lag', '2320', '2320_lag', '2330', '2330_lag', '2340', '2340_lag', '2350', '2350_lag', '2300', '2300_lag', '2410', '2410_lag', '2421', '2421_lag', '2430', '2430_lag', '2450', '2450_lag', '2460', '2460_lag', '2400', '2400_lag', '2510', '2510_lag', '2520', '2520_lag', '2500', '2500_lag', '32003', '32004', '32005', '32006', '32007', '32008', '33103', '33104', '33105', '33106', '33107', '33108', '33117', '33118', '33125', '33127', '33128', '33135', '33137', '33138', '33143', '33144', '33145', '33148', '33153', '33154', '33155', '33157', '33163', '33164', '33165', '33166', '33167', '33168', '33203', '33204', '33205', '33206', '33207', '33208', '33217', '33218', '33225', '33227', '33228', '33235', '33237', '33238', '33243', '33244', '33245', '33247', '33248', '33253', '33254', '33255', '33257', '33258', '33263', '33264', '33265', '33266', '33267', '33268', '33277', '33278', '33305', '33306', '33307', '33406', '33407', '33003', '33004', '33005', '33006', '33007', '33008', '36003', '36004', '4110', '4111', '4112', '4113', '4119', '4120', '4121', '4122', '4123', '4124', '4129', '4100', '4210', '4211', '4212', '4213', '4214', '4219', '4220', '4221', '4222', '4223', '4224', '4229', '4200', '4310', '4311', '4312', '4313', '4314', '4319', '4320', '4321', '4322', '4323', '4329', '4300', '4400', '4490', '6100', '6210', '6215', '6220', '6230', '6240', '6250', '6200', '6310', '6311', '6312', '6313', '6320', '6321', '6322', '6323', '6324', '6325', '6326', '6330', '6350', '6300', '6400', 
         'date']
VALID_ROW_WIDTH = len(COLUMNS)
INN_POSITION = COLUMNS.index('inn')

    
def is_valid(row):
    """Return True if row is valid.""" 
    if len(row) != VALID_ROW_WIDTH:
       #reason = "Invalid row length {}".format(str(len(row)))
       return False
    elif not row[INN_POSITION]:
       #reason = "Skipped row with empty INN field"
       return False
    else:
       return True   

# read and filter raw csv
def csv_stream(filename, enc='utf-8', sep=','):
   """Emit CSV rows by filename."""
   if enc not in ['utf-8', 'windows-1251']:
       raise ValueError("Encoding not supported: " + str(enc))
   with open(filename, 'r', encoding=enc) as csvfile:
      spamreader = csv.reader(csvfile, delimiter=sep)   
      for row in spamreader:
         yield row

def emit_raw_colnames():
    """Column names corresponding to emit_raw_rows()."""  
    return ['year']+COLUMNS

def emit_parsed_colnames():
    return get_parsed_colnames()

def emit_raw_rows(year):
    """Emit raw rows by year."""
    fn = RawDataset(year).get_filename()
    fmt = dict(enc='windows-1251', sep=";")
    raws = filter(is_valid, csv_stream(fn, **fmt)) 
    add_year = lambda row: [year] + row
    return map(add_year, raws)

def emit_raw_dicts(year):
    columns = emit_raw_colnames() 
    as_dict = lambda row: OrderedDict(zip(columns,row)) 
    return map(as_dict, emit_raw_rows(year))    

def emit_parsed_rows(year):
    gen = emit_raw_dicts(year)
    return map(parse_row, gen)

def emit_parsed_dicts(year):
    columns = emit_parsed_colnames()
    as_dict = lambda row: OrderedDict(zip(columns,row)) 
    return map(as_dict, emit_parsed_rows(year))

def cut(year,gen_func,skip):
    return next(islice(gen_func(year),skip,skip+1))
    
def parsed(year,n):
    return next(islice(emit_parsed_dicts(2015),n,n+1))    

#for i,pd in enumerate(islice(emit_parsed_dicts(2015),0,1)):    
#    if pd['cash_out_investment_of']>500*1000:
#        pass
        #print(i, "%12d" % pd['cash_out_investment_of'], pd['region'], pd ['title'])
   
# Каргилл
# http://kommersant.ru/doc/2735389   

# Русполимет
pd = parsed(2015, 25471)    
pprint(pd)

is_valid([1])
