# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 03:23:56 2016

@author: Евгений
"""

from collections import namedtuple
from inspect_columns import Columns
from numpy import int64 

INT_TYPE = int64
EMPTY = int('0')
QUOTE_CHAR = '"'

columns = ['year'] + Columns.COLUMNS
K=9    
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
renamed_data_col_selection = [to_tag(x) for x in Columns.DATACOLS]

Text = namedtuple("Text", text_colnames)
Data = namedtuple("Data", data_colnames)


class Row():        
    def __init__(self, incoming_row):
        text_vals, data_vals = split_row(incoming_row)
        self.text = Text._make(text_vals)
        self.data = Data._make(data_vals)
    def data_values(self, fields):
        return [int(getattr(self.data, fld)) for fld in fields]       

    
# row transformation
def rub_to_thousand(x):
    return int(round(0.001*float(x)))


def mln_to_thousand(x):
    return 1000*int(x)
    
  
def parse_row(row_as_list):
    """Return modified *rowd* dictionary."""
    row = Row(row_as_list)
    # assemble new text cols
    t = row.text
    ok1, ok2, ok3 = okved3(t.okved)
    org, title = dequote(t.name)
    region = t.inn[0:2]
    
    text = [t.year, t.date, ok1, ok2, ok3,
            org, title, region, t.inn,
            t.okpo, t.okopf, t.okfs,
            t.unit]

    # adjust values to '000 rub 
    unit = t.unit
    if unit == '383':
        func = rub_to_thousand        
    elif unit == '385':
        func = mln_to_thousand
    elif unit == '384':
        func = lambda x: x
    else:
        raise ValueError("Unit not supported: %s" % unit)
    data = [func(x) for x in row.data_values(renamed_data_col_selection )]

    return text+data


def get_parsed_colnames():
    """Return colnames corresponding to parse_row(). """
    return ['year', 'date', 'ok1', 'ok2', 'ok3',
            'org', 'title', 'region', 'inn',
            'okpo', 'okopf', 'okfs',
            'unit'] + Columns.RENAMED_DATACOLS


def get_colname_dtypes():
    """Return types correspoding to get_colnames().
       Used to speed up CSV import in custom_df_reader(). """
    dtype_dict = {k: INT_TYPE for k in get_parsed_colnames()}
    string_cols = ['date', 'org', 'title', 'region', 'inn',
                   'okpo', 'okopf', 'okfs']
    dtype_dict.update({k: str for k in string_cols})
    return dtype_dict


# stateless transformations
def okved3(code_string):
    """Get 3 levels of OKVED codes from *code_string*."""
    codes = [int(x) for x in code_string.split(".")]
    return codes + [EMPTY] * (3-len(codes))


def dequote(name):
    """Split company *name* to organisation and title."""
    # Warning: will not work well on company names with more than 4 quotechars
    parts = name.split(QUOTE_CHAR)
    org = parts[0].strip()
    cnt = name.count(QUOTE_CHAR)
    if cnt == 2:
        title = parts[1].strip()
    elif cnt > 2:
        title = QUOTE_CHAR.join(parts[1:])
    else:
        title = name
    return org, title.strip()

    
z = [2015, 'Открытое акционерное общество "Энерготехмаш" (открыто конкурсное производство)', '00110467', '10000', '16', '28.11', '3435900517', '384', '2', '25', '34', '0', '0', '0', '0', '0', '0', '23616', '27492', '0', '0', '0', '0', '23849', '14267', '176', '321', '47666', '42114', '43765', '248551', '121', '129', '79251', '97505', '627', '627', '36', '346', '523', '481', '124323', '347639', '171989', '389753', '18', '18', '0', '0', '16713', '16831', '0', '0', '0', '0', '-266854', '-49346', '-250123', '-32497', '223076', '223076', '4503', '9938', '0', '0', '0', '0', '227579', '233014', '33574', '4806', '160300', '183025', '0', '0', '659', '1405', '0', '0', '194533', '189236', '171989', '389753', '39311', '335342', '56241', '307156', '-16930', '28186', '1456', '10840', '30666', '47572', '-49052', '-30226', '0', '0', '87', '110', '29000', '25572', '42792', '56664', '194257', '63246', '-229430', '-62270', '0', '0', '0', '0', '0', '0', '0', '0', '-11804', '-10181', '-217626', '-52089', '-118', '-3113', '0', '0', '-217744', '-55202', '18', '0', '16831', '0', '-49346', '-32497', '0', '0', '0', '0', '118', '118', '118', '118', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '118', '0', '217626', '217744', '217626', '217626', '118', '0', '118', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '18', '0', '16714', '0', '-266854', '-250122', '-250123', '-32497', '26572', '16333', '5610', '0', '4629', '26882', '3123', '23721', '0', '0', '0', '-310', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '25', '25', '0', '0', '0', '0', '25', '25', '0', '0', '0', '0', '-310', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0']    
assert text_colnames[-1] == 'date'
assert text_colnames[-2] == 'report_type'
assert data_colnames[0] == 'b1110'
assert columns.index('report_type') + 1 == K
text_vals, data_vals = split_row(z, k=K)
text = Text._make(text_vals)
data = Data._make(data_vals)
row = Row(z)
assert row.data_values(['b2200']) == [-49052]
