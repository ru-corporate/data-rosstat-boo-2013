# -*- coding: utf-8 -*-
"""Parse CSV row using adjust_row(vec, year)"""

from columns import COLNAMES, TEXT_COLS
VALID_SOURCE_CSV_ROW_WIDTH = len(COLNAMES)
EMPTY = '0'   
QUOTE_CHAR = '"' 
K = COLNAMES.index('1110')
INN_POSITION_IN_ROW = 5
UNIT_POSITION_IN_ROW = 6
K = len(TEXT_COLS) # 8

assert K == len(TEXT_COLS)
LINE1_2013_PARSED = [2013, '20140617', 'Общество с ограниченной ответственностью', 'СОК "Эдельвейс"', '24', 93, 4, 0, '67645404', '0', '16', '93.04', '2454021005', '384', '1', 0, 0, 0, 0, 0, 0, 0, 0, 20, 94, 0, 0, 0, 0, 0, 0, 0, 0, 20, 94, 161, 64, 0, 0, 467, 876, 0, 0, 459, 212, 0, 0, 1087, 1152, 1106, 1246, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 889, 746, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 216, 500, 0, 0, 0, 0, 0, 0, 216, 500, 1106, 1246, 13027, 8511, 12736, 7253, 291, 1258, 0, 0, 0, 0, 291, 1258, 0, 0, 0, 0, 0, 0, 0, 0, 71, 61, 220, 1197, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 220, 1196, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
LINE1_2013_RAW = ['Открытое акционерное общество "Российское акционерное общество по производству цветных и драгоценных металлов "Норильский никель"', '00002565', '12247', '16', '65.23.1', '2457009983', '384', '2', '150', '150', '0', '0', '0', '0', '0', '0', '21', '56', '0', '0', '3129154', '3129154', '35332', '18558', '0', '0', '3164657', '3147918', '23', '23', '0', '0', '3728', '1951', '2970548', '2900387', '31526', '13763', '0', '0', '3005825', '2916124', '6170482', '6064042', '47250', '47250', '0', '0', '0', '0', '2266991', '2266991', '7087', '7087', '3848006', '3741048', '6169334', '6062376', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '226', '360', '0', '0', '922', '1306', '0', '0', '1148', '1666', '6170482', '6064042', '2245363', '2951506', '2100227', '2770211', '145136', '181295', '0', '0', '31315', '52939', '113821', '128356', '13748', '29792', '5230', '1364', '0', '0', '42', '58', '3508', '12216', '129333', '147354', '40038', '27104', '-18723', '-18867', '0', '0', '32894', '16500', '15231', '14258', '106958', '122492', '0', '0', '0', '0', '106958', '122492', '47250', '0', '2266991', '7087', '3741048', '6062376', '0', '0', '0', '0', '106958', '106958', '106958', '106958', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '47250', '0', '2266991', '7087', '3848006', '6169334', '6169334', '3062376', '2249685', '0', '57', '0', '2249628', '2245685', '8132', '20362', '0', '154', '2217037', '4000', '13748', '0', '0', '0', '13748', '0', '0', '0', '0', '0', '0', '0', '13748', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '17748', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', 
                  '20140617']
LINE1_2013_SPLITTED = (['Открытое акционерное общество "Российское акционерное общество по производству цветных и драгоценных металлов "Норильский никель"', 
'00002565', '12247', '16', '65.23.1', '2457009983', '384', '2', '20140617'], 
['150', '150', '0', '0', '0', '0', '0', '0', '21', '56', '0', '0', '3129154', '3129154', '35332', '18558', '0', '0', '3164657', '3147918', '23', '23', '0', '0', '3728', '1951', '2970548', '2900387', '31526', '13763', '0', '0', '3005825', '2916124', '6170482', '6064042', '47250', '47250', '0', '0', '0', '0', '2266991', '2266991', '7087', '7087', '3848006', '3741048', '6169334', '6062376', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '226', '360', '0', '0', '922', '1306', '0', '0', '1148', '1666', '6170482', '6064042', '2245363', '2951506', '2100227', '2770211', '145136', '181295', '0', '0', '31315', '52939', '113821', '128356', '13748', '29792',  '5230', '1364', '0', '0', '42', '58', '3508', '12216', '129333', '147354', '40038', '27104', '-18723', '-18867', '0', '0', '32894', '16500', '15231', '14258', '106958', '122492', '0', '0', '0', '0', '106958', '122492', '47250', '0', '2266991', '7087', '3741048', '6062376', '0', '0', '0', '0', '106958', '106958', '106958', '106958', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '47250', '0', '2266991', '7087', '3848006', '6169334', '6169334', '3062376', '2249685', '0', '57',  '0', '2249628', '2245685', '8132', '20362', '0', '154', '2217037', '4000', '13748', '0', '0', '0', '13748', '0', '0', '0', '0', '0', '0', '0', '13748', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '17748', '15', '0', '0', '0',  '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'])
                  
                  
def make_raw_iter():
    return iter([LINE1_2013_RAW, LINE1_2013_RAW+['error']])

def get_rows(gen, funcs=None):
    gen = filter(is_valid, gen)
    if funcs:
       for f in funcs:
           gen = map(f, gen)
    return gen
  
def _mock_rows(funcs=None):
    return get_rows(make_raw_iter(), funcs) 
    
def everything(x):
    return LINE1_2013_PARSED

assert everything(LINE1_2013_RAW) == LINE1_2013_PARSED

def extract_inn(row):
    """Access INN field in row."""
    return row[INN_POSITION_IN_ROW]

def extract_unit(row):
    """Access INN field in row."""
    return row[UNIT_POSITION_IN_ROW]    
    
def is_valid(row):
    """Return true if row is valid""" 
    if len(row) != VALID_SOURCE_CSV_ROW_WIDTH:
       reason = "Invalid row length {}".format(str(len(row)))
       return False
    elif not extract_inn(row):
       reason = "Skipped row with empty INN field"
       return False
    else:
       return True

assert list(filter(is_valid, make_raw_iter())) == [LINE1_2013_RAW]

def split(row):
    text_cols = row[0:K] + [row[-1]]
    data_cols = row[K:-1] 
    return (text_cols, data_cols)

INDEX=[12,13,14,15]    
def slice_data_columns(row_tuple):
    text, data = row_tuple
    data = [data[i] for i in INDEX]
    return text, data

def adjust_vec(unit, num_vec):
    """Adjust values in *num_vec* to thousand rub"""  
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

def adjust_units(row_tuple):
    text, data = row_tuple
    unit = extract_unit(text)
    data = adjust_vec(unit, data)
    return text, data
    
YEAR = 2013
def add_text_columns(row_tuple, year=YEAR):
    text, data = row_tuple   
    
    # variable names
    var = dict(zip(TEXT_COLS, text))
            
    # text data
    ok1, ok2, ok3 = okved3(var['okved'])
    org, title = dequote(var['name'])
    region = var['inn'][0:2]    
    date_reviewed = text[-1]
    
    # assemble text new row
    text = [year, date_reviewed, org, title, region, ok1, ok2, ok3]
    return text, data

def as_list(row_tuple):
    text, data = row_tuple   
    return text + data   

# NEW_TEXT_COLS = [x for x in TEXT_COLS if x != 'name']
         
def okved3(code_string): 
    """Get 3 levels of OKVED codes from *code_string* """
    codes = [x for x in code_string.split(".")]
    return codes + [EMPTY] * (3-len(codes))        

def dequote(name):
    """Split company name to organisation and title"""
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
    
if __name__=="__main__":
    assert LINE1_2013_SPLITTED == split(LINE1_2013_RAW)
    gen = make_raw_iter()   
    gen0 = filter(is_valid, make_raw_iter())
    gen1 = map(split, gen0)
    gen2 = map(slice_data_columns, gen1)
    gen3 = map(adjust_units, gen2)
    gen4 = map(add_text_columns, gen3)
    gen5 = map(as_list, gen4)
    z = list(gen5)[0]
    #[2013, '20140617', 'Открытое акционерное общество', 'Российское акционерное общество по производству цветных и драгоценных металлов "Норильский никель"', 
    #       '24', 65, 23, 1, 150, 0, 0]]