# -*- coding: utf-8 -*-
"""
"""
import csv
from collections import  OrderedDict
from itertools import islice 

import config
from common import print_elapsed_time
from remote import RawDataset


EMPTY = '0'   
QUOTE_CHAR = '"'

DATE_COL=['date']
COLUMNS=['name', 'okpo', 'okopf', 'okfs', 'okved', 'inn', 'unit', 'report_type', '1110', '1110_lag', '1120', '1120_lag', '1130', '1130_lag', '1140', '1140_lag', '1150', '1150_lag', '1160', '1160_lag', '1170', '1170_lag', '1180', '1180_lag', '1190', '1190_lag', '1100', '1100_lag', '1210', '1210_lag', '1220', '1220_lag', '1230', '1230_lag', '1240', '1240_lag', '1250', '1250_lag', '1260', '1260_lag', '1200', '1200_lag', '1600', '1600_lag', '1310', '1310_lag', '1320', '1320_lag', '1340', '1340_lag', '1350', '1350_lag', '1360', '1360_lag', '1370', '1370_lag', '1300', '1300_lag', '1410', '1410_lag', '1420', '1420_lag', '1430', '1430_lag', '1450', '1450_lag', '1400', '1400_lag', '1510', '1510_lag', '1520', '1520_lag', '1530', '1530_lag', '1540', '1540_lag', '1550', '1550_lag', '1500', '1500_lag', '1700', '1700_lag', '2110', '2110_lag', '2120', '2120_lag', '2100', '2100_lag', '2210', '2210_lag', '2220', '2220_lag', '2200', '2200_lag', '2310', '2310_lag', '2320', '2320_lag', '2330', '2330_lag', '2340', '2340_lag', '2350', '2350_lag', '2300', '2300_lag', '2410', '2410_lag', '2421', '2421_lag', '2430', '2430_lag', '2450', '2450_lag', '2460', '2460_lag', '2400', '2400_lag', '2510', '2510_lag', '2520', '2520_lag', '2500', '2500_lag', '32003', '32004', '32005', '32006', '32007', '32008', '33103', '33104', '33105', '33106', '33107', '33108', '33117', '33118', '33125', '33127', '33128', '33135', '33137', '33138', '33143', '33144', '33145', '33148', '33153', '33154', '33155', '33157', '33163', '33164', '33165', '33166', '33167', '33168', '33203', '33204', '33205', '33206', '33207', '33208', '33217', '33218', '33225', '33227', '33228', '33235', '33237', '33238', '33243', '33244', '33245', '33247', '33248', '33253', '33254', '33255', '33257', '33258', '33263', '33264', '33265', '33266', '33267', '33268', '33277', '33278', '33305', '33306', '33307', '33406', '33407', '33003', '33004', '33005', '33006', '33007', '33008', '36003', '36004', '4110', '4111', '4112', '4113', '4119', '4120', '4121', '4122', '4123', '4124', '4129', '4100', '4210', '4211', '4212', '4213', '4214', '4219', '4220', '4221', '4222', '4223', '4224', '4229', '4200', '4310', '4311', '4312', '4313', '4314', '4319', '4320', '4321', '4322', '4323', '4329', '4300', '4400', '4490', '6100', '6210', '6215', '6220', '6230', '6240', '6250', '6200', '6310', '6311', '6312', '6313', '6320', '6321', '6322', '6323', '6324', '6325', '6326', '6330', '6350', '6300', '6400', 'date']
TEXT_COLUMNS=['name', 'okpo', 'okopf', 'okfs', 'okved', 'inn', 'unit', 'report_type']
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
PRESET_UNIT_POSITION = COLUMNS.index('unit')
K=len(TEXT_COLUMNS)
DATE_COL=['date']
PRESET_NEW_TEXT_COLUMNS=TEXT_COLUMNS+DATE_COL

             
def is_valid(row):
    """Return True if row is valid""" 
    if len(row) != PRESET_VALID_ROW_WIDTH:
       #reason = "Invalid row length {}".format(str(len(row)))
       return False
    elif not extract_inn(row):
       #reason = "Skipped row with empty INN field"
       return False
    else:
       return True

def make_tuples(row, k=K):    
    """Returns (text values, data values) tuple."""
    text_cols = row[0:k] + [row[-1]]
    data_cols = row[k:-1] 
    return text_cols, data_cols

_, b = make_tuples(COLUMNS)
PRESET_INDEX=[i for i,x in enumerate(b) if x in RENAMER.keys()]

# Read, filter and make_tuples raw csv
def extract_inn(row):
    """Access INN field in row."""
    return row[PRESET_INN_POSITION]

def extract_unit(row):
    """Access unit field in row."""
    return row[PRESET_UNIT_POSITION]   

def csv_stream(filename, enc, sep):
   """Emit CSV rows by filename"""
   if enc not in ['utf-8', 'windows-1251']:
       raise ValueError("Encoding not supported: " + str(enc))
   with open(filename, 'r', encoding=enc) as csvfile:
      spamreader = csv.reader(csvfile, delimiter=sep)   
      for row in spamreader:
         yield row     

def emit_raw_rows(year):
    """Emit CSV rows by year"""
    fn = RawDataset(year).get_filename()
    fmt = dict(enc='windows-1251', sep=";")
    return csv_stream(fn, **fmt)    
    
def emit_filtered_tuples(year): 
    """Represent each row as (text values, data values) tuple."""
    raw = emit_raw_rows(year)   
    return map(make_tuples, filter(is_valid, raw))

def emit_rows(year,inn_list=[]):
    gen = emit_filtered_tuples(year)
    if inn_list:
       f = lambda row: extract_inn(row) in inn_list    
       gen = filter(f, gen)
    return transform(gen, year)
    
def transform(gen, year):    
    text_mod = lambda x: modify_text_columns(x, year)
    for f in [slice_columns, adjust_units, text_mod]:
        gen = map(f, gen)
    return gen 

# row tranformations

def slice_columns(row, ix=PRESET_INDEX):
    """Reduce number of data columns accrding to RENAMER.
       RENAMER is used to derive PRESET_INDEX."""
    text, data = row
    return text, [data[i] for i in ix]

def adjust_units(row):
    """Adjust data values to '000 rub"""
    text, data = row    
    unit = extract_unit(text)
    data = adjust_vec(unit, data)
    return text, data

def adjust_vec(unit, num_vec):
    """Adjust values in *num_vec* to '000 rub"""  
    if unit == '384': 
        # no adjustment
        return [int(x) for x in num_vec]
    elif unit == '383':
        # adjust rub to thousand rub
        return [int(round(0.001*float(x))) for x in num_vec]            
    elif unit == '385':
        # adjust mln rub to thousand rub 
        return [1000*int(x) for x in num_vec]            
    else:
        raise ValueError("Unit not supported: " + unit)     
    
def make_var_dict(text, cols=PRESET_NEW_TEXT_COLUMNS):
    """Get text values as dict with variable names as keys."""
    return dict(zip(cols, text))    
    
def modify_text_columns(row, year):
    """Add new text columns (okved, name split, region) and reorder."""
    text, data = row 
    # variable names
    var =  make_var_dict(text)
    # text data
    ok1, ok2, ok3 = okved3(var['okved'])
    org, title = dequote(var['name'])
    region = var['inn'][0:2]
    date_reviewed = var['date']    
    # assemble new text row
    text = [str(year), date_reviewed, ok1, ok2, ok3, 
            org, title, region, var['inn'],
            var['okpo'], var['okopf'], var['okfs']]
    return text + data 
    
def get_colnames():
    """Return colnames corresponding to *emit_rows(year)* rows."""
    return ['year', 'date', 'ok1', 'ok2', 'ok3', 
            'org', 'title', 'region', 'inn',
            'okpo', 'okopf', 'okfs'] + list(RENAMER.values())  
    
def okved3(code_string): 
    """Get 3 levels of OKVED codes from *code_string*."""
    codes = [x for x in code_string.split(".")]
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

# output

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

def save(year):
    columns=get_colnames()
    fn = config.make_path_sliced_csv(year)
    gen = emit_rows(year)    
    print ("Saving %s dataset..." % year)        
    to_csv(fn, gen, columns)
    return 1

if __name__ == "__main__": 
    _, d = slice_columns(make_tuples(COLUMNS))        
    assert len(d) == len(RENAMER)
    assert COLUMNS[:K]==TEXT_COLUMNS
    assert next(emit_rows(2013)) == ['2013', '20140617', '93', '04', '0', 'Общество с ограниченной ответственностью', 'СОК "Эдельвейс"', '24', '2454021005', '67645404', '0', '16', 20, 94, 20, 94, 1087, 1152, 1106, 1246, 889, 746, 0, 0, 0, 0, 0, 0, 216, 500, 1106, 1246, 13027, 8511, 291, 1258, 0, 0, 220, 1197, 0, 0, 0, 0, 0, 0, 0, 0] 
    assert next(islice(emit_rows(2014),55,56)) == ['2014', '20150706', '52', '11', '2', 'Товарищество на вере', 'Мялкин и компания', '52', '5231002660', '43012350', '11064', '16', 55, 59, 55, 331, 2662, 2990, 2717, 3321, 2528, 2130, 0, 0, 0, 0, 0, 0, 189, 1191, 2717, 3321, 8274, 5506, 0, 36, 0, 0, -56, -17, 0, 0, 0, 0, 0, 0, 0, 0]
    assert next(emit_rows(2015)) == ['2015', '20160624', '28', '11', '0', 'Открытое акционерное общество', 'Энерготехмаш', '34', '3435900517', '00110467', '10000', '16', 23616, 27492, 47666, 42114, 124323, 347639, 171989, 389753, -250123, -32497, 223076, 223076, 227579, 233014, 33574, 4806, 194533, 189236, 171989, 389753, 39311, 335342, -49052, -30226, 29000, 25572, -229430, -62270, 26572, 16333, 3123, 23721, 0, 0, 0, 0]

    import random 
        
    for year in config.VALID_YEARS:
        #assert 1 == save(year)        
        n = random.randint(1, 1000)
        gen = islice(emit_rows(year), n, n+1)
        print(next(gen))
    
        
#todo:
# delete unused files
# make one file? less files? common.py + columns.py
# tests to test_rows.py
# change file structure at config.py        
# test resulting frames
# parser        