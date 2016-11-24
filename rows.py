# -*- coding: utf-8 -*-
"""
"""
import csv
import os
from collections import OrderedDict
from itertools import islice 
import pandas as pd

import config
from common import print_elapsed_time, pipe
from remote import RawDataset


EMPTY = int('0')
QUOTE_CHAR = '"'

DATE_COL=['date']
COLUMNS=['name', 'okpo', 'okopf', 'okfs', 'okved', 'inn', 'unit', 'report_type', '1110', '1110_lag', '1120', '1120_lag', '1130', '1130_lag', '1140', '1140_lag', '1150', '1150_lag', '1160', '1160_lag', '1170', '1170_lag', '1180', '1180_lag', '1190', '1190_lag', '1100', '1100_lag', '1210', '1210_lag', '1220', '1220_lag', '1230', '1230_lag', '1240', '1240_lag', '1250', '1250_lag', '1260', '1260_lag', '1200', '1200_lag', '1600', '1600_lag', '1310', '1310_lag', '1320', '1320_lag', '1340', '1340_lag', '1350', '1350_lag', '1360', '1360_lag', '1370', '1370_lag', '1300', '1300_lag', '1410', '1410_lag', '1420', '1420_lag', '1430', '1430_lag', '1450', '1450_lag', '1400', '1400_lag', '1510', '1510_lag', '1520', '1520_lag', '1530', '1530_lag', '1540', '1540_lag', '1550', '1550_lag', '1500', '1500_lag', '1700', '1700_lag', '2110', '2110_lag', '2120', '2120_lag', '2100', '2100_lag', '2210', '2210_lag', '2220', '2220_lag', '2200', '2200_lag', '2310', '2310_lag', '2320', '2320_lag', '2330', '2330_lag', '2340', '2340_lag', '2350', '2350_lag', '2300', '2300_lag', '2410', '2410_lag', '2421', '2421_lag', '2430', '2430_lag', '2450', '2450_lag', '2460', '2460_lag', '2400', '2400_lag', '2510', '2510_lag', '2520', '2520_lag', '2500', '2500_lag', '32003', '32004', '32005', '32006', '32007', '32008', '33103', '33104', '33105', '33106', '33107', '33108', '33117', '33118', '33125', '33127', '33128', '33135', '33137', '33138', '33143', '33144', '33145', '33148', '33153', '33154', '33155', '33157', '33163', '33164', '33165', '33166', '33167', '33168', '33203', '33204', '33205', '33206', '33207', '33208', '33217', '33218', '33225', '33227', '33228', '33235', '33237', '33238', '33243', '33244', '33245', '33247', '33248', '33253', '33254', '33255', '33257', '33258', '33263', '33264', '33265', '33266', '33267', '33268', '33277', '33278', '33305', '33306', '33307', '33406', '33407', '33003', '33004', '33005', '33006', '33007', '33008', '36003', '36004', '4110', '4111', '4112', '4113', '4119', '4120', '4121', '4122', '4123', '4124', '4129', '4100', '4210', '4211', '4212', '4213', '4214', '4219', '4220', '4221', '4222', '4223', '4224', '4229', '4200', '4310', '4311', '4312', '4313', '4314', '4319', '4320', '4321', '4322', '4323', '4329', '4300', '4400', '4490', '6100', '6210', '6215', '6220', '6230', '6240', '6250', '6200', '6310', '6311', '6312', '6313', '6320', '6321', '6322', '6323', '6324', '6325', '6326', '6330', '6350', '6300', '6400', 
         'date']         
ORIGINAL_TEXT_COLUMNS=['name', 'okpo', 'okopf', 'okfs', 'okved', 'inn', 'unit', 'report_type']
RENAMER = OrderedDict([('1150', 'of'),
             ('1100',     'ta_fix'),
             ('1200',     'ta_nonfix'),
             ('1600',     'ta'),
             ('1410',     'debt_long'),
             ('1510',     'debt_short'),
             ('1300',     'tp_cap'),
             ('1400',     'tp_long'),
             ('1500',     'tp_short'),
             ('1700',     'tp'),
             ('2110',     'sales'),
             ('2200',     'profit_operational'),
             ('2330',     'exp_interest'),
             ('2300',     'profit_before_tax'),
             ('1150_lag', 'of_lag'),
             ('1100_lag', 'ta_fix_lag'),
             ('1200_lag', 'ta_nonfix_lag'),
             ('1600_lag', 'ta_lag'),
             ('1300_lag', 'tp_cap_lag'),
             ('1410_lag', 'debt_long_lag'),
             ('1400_lag', 'tp_long_lag'),
             ('1510_lag', 'debt_short_lag'),
             ('1500_lag', 'tp_short_lag'),
             ('1700_lag', 'tp_lag'),
             ('2110_lag', 'sales_lag'),
             ('2200_lag', 'profit_operational_lag'),
             ('2330_lag', 'exp_interest_lag'),
             ('2300_lag', 'profit_before_tax_lag'),
             ('4110', 'cash_oper_inflow'),
             ('4111', 'cash_oper_inflow_sales'),
             ('4121', 'paid_to_supplier'),
             ('4122', 'paid_to_worker'),
             ('4123', 'cash_interest'),
             ('4124', 'paid_profit_tax'),
             ('4129', 'paid_other_cost'),
             ('4221', 'cash_investment_of')])

PRESET_VALID_ROW_WIDTH = len(COLUMNS)
PRESET_INN_POSITION = COLUMNS.index('inn')

DATACOLS = list(RENAMER.keys())
RENAMED_DATACOLS = list(RENAMER.values()) 
TEXTCOLS = ORIGINAL_TEXT_COLUMNS+DATE_COL

def is_valid(row):
    """Return True if row is valid.""" 
    if len(row) != PRESET_VALID_ROW_WIDTH:
       #reason = "Invalid row length {}".format(str(len(row)))
       return False
    elif not extract_inn(row):
       #reason = "Skipped row with empty INN field"
       return False
    else:
       return True

def extract_inn(row):
    """Access INN field in row."""
    return row[PRESET_INN_POSITION]
    
# Read, filter and make_tuples raw csv
def csv_stream(filename, enc='utf-8', sep=','):
   """Emit CSV rows by filename."""
   if enc not in ['utf-8', 'windows-1251']:
       raise ValueError("Encoding not supported: " + str(enc))
   with open(filename, 'r', encoding=enc) as csvfile:
      spamreader = csv.reader(csvfile, delimiter=sep)   
      for row in spamreader:
         yield row     

def emit_raw_rows(year):
    """Emit CSV rows by year."""
    fn = RawDataset(year).get_filename()
    fmt = dict(enc='windows-1251', sep=";")
    return filter(is_valid, csv_stream(fn, **fmt))    
    
def emit_raw_dicts(year): 
    """Represent each row as (text values, data values) tuple."""
    raw = emit_raw_rows(year)
    f = lambda row: dict(zip(COLUMNS,row))
    return map(f, raw)

def filter_by_inn(rowd, inn_list):
    inn = rowd['inn']
    if inn in inn_list:
        print("Found INN: ", inn)
        return True
    else:
        return False
    
def emit_rows(year,inn_list=[]):
    gen = emit_raw_dicts(year)
    
    # warning: will read entire file even if all inn are found
    if inn_list:
       f = lambda rowd: filter_by_inn(rowd, inn_list)   
       gen = filter(f, gen)

    g = lambda x: trans(x, year)
    return map(g, gen)

def emit_dicts(year,inn_list=[]):
    zen = emit_rows(year,inn_list)
    cols = get_colnames()
    to_dict = lambda x: OrderedDict(zip(cols,x))
    return map(to_dict, zen)    
    
# row tranformations
def get_adjust_func(unit):
    """Adjust values in *num_vec* to '000 rub"""  
    if unit == '384': 
        # no adjustment
        return lambda x: int(x)
    elif unit == '383':
        # adjust rub to thousand rub
        return lambda x: int(round(0.001*float(x))) 
    elif unit == '385':
        # adjust mln rub to thousand rub 
        return lambda x: 1000*int(x)
    else:
        raise ValueError("Unit not supported: " + unit)     

def trans(rowd, year):  
    ok1, ok2, ok3 = okved3(rowd['okved'])
    org, title = dequote(rowd['name'])
    region = rowd['inn'][0:2]
    date_reviewed = rowd['date']
    # assemble new text row
    text = [year, date_reviewed, ok1, ok2, ok3, 
            org, title, region, rowd['inn'],
            rowd['okpo'], rowd['okopf'], rowd['okfs']]

    func = get_adjust_func(rowd['unit'])
    data = [func(rowd[k]) for k in DATACOLS] 
    return text+data
    
def get_colnames():
    """Return colnames corresponding to *emit_rows(year)* rows."""

    return ['year', 'date', 'ok1', 'ok2', 'ok3', 
            'org', 'title', 'region', 'inn',
            'okpo', 'okopf', 'okfs'] + RENAMED_DATACOLS  

def get_colname_dtypes():
    """Return types correspoding to colmanes. 
       Used to speed up csv import in *custom_df_reader()*. """
    dtype_dict = {k:int for k in get_colnames()}
    string_cols = ['date', 'org', 'title', 'region', 'inn',
                   'okpo', 'okopf', 'okfs']     
    dtype_dict.update({k:str for k in string_cols})
    return dtype_dict 
    
# stateless transformations              
def okved3(code_string): 
    """Get 3 levels of OKVED codes from *code_string*."""
    codes = [int(x) for x in code_string.split(".")]
    return codes + [EMPTY] * (3-len(codes))        

def dequote(name):
    """Split company name to organisation and title."""
    parts = name.split(QUOTE_CHAR)
    org = parts[0].strip()
    cnt = name.count(QUOTE_CHAR)    
    if cnt==2:
       title = parts[1].strip()
    elif cnt>2:
       title = QUOTE_CHAR.join(parts[1:])
       # warning: will not work well on company names with more than 4 quotechars 
    else:
       title = name
    return org, title.strip() 

# output to csv file 

@print_elapsed_time
def to_csv(path, stream, cols=None):    
    with open(path, 'w', encoding = "utf-8") as file:
        writer = csv.writer(file, lineterminator="\n", 
                            quoting=csv.QUOTE_MINIMAL)
        if cols:                    
            writer.writerow(cols)
        writer.writerows(stream)
    print("Saved file:", path)    
    return path     

def to_csv2(path, gen):
    return to_csv(path, gen, cols=get_colnames()) 

def save(year):
    gen = pipe(emit_rows(year))            
    print ("Saving %s dataset..." % year)        
    fn = config.make_path_parsed_csv(year)
    to_csv2(fn, gen)    

def custom_df_reader(file):
     if os.path.exists(file):
        print("Reading file:", file)            
        # dtype on all columns shortens reading time (from 120 sec to 87 sec on my machine)        
        dtype_dict=get_colname_dtypes()        
        return pd.read_csv(file, dtype=dtype_dict)
     else:
        raise FileNotFoundError(file)
        
        

class Dataset():
    
    def __init__(self, year):        
        self.year = year
        self.df_csv = config.make_path_parsed_csv(year)
            
    def to_csv(self, force=False):
        if not os.path.exists(self.df_csv) or force is True:
            gen=pipe(emit_rows(self.year))            
            print ("Saving %s dataset..." % self.year)        
            to_csv2(self.df_csv, gen) 
        else:
            print("""Dataset for year {} already saved:""".format(self.year),
                 self.df_csv, "\n")

    @print_elapsed_time    
    def read_df(self): 
        print("Reading {} dataframe...".format(self.year))
        return custom_df_reader(self.df_csv)         
         

class INN_Subset(Dataset):
    
    def get_inn_list(self):
        inn_path = config.get_inn_list_path()
        return [r[0] for r in csv_stream(inn_path)]
    
    def __init__(self, year, inn_list):        
        super().__init__(year)        
        self.output_csv=config.make_path_inn_csv(year)
        self.inn_list=self.get_inn_list()
            
    # overload method using *inn_list* filter
    def to_csv(self):
        gen = pipe(emit_rows(self.year, self.inn_list))            
        print ("Saving %s dataset with INN filter..." % self.year)        
        to_csv2(self.output_csv, gen) 
        
if __name__=="__main__":
    gen1=emit_raw_dicts(2015)
    gen2=emit_dicts(2015, ['3435900517'])
    m = next(gen1)
    assert m['1600'] == '171989'
    assert m['1700'] == '171989'
    
    u = next(gen2)
    assert u['ta']== 171989
    assert u['ta']-u['ta_fix']-u['ta_nonfix'] == 0
    #print(u)