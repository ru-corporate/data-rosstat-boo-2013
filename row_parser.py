# -*- coding: utf-8 -*-
"""Parse CSV row using adjust_row(vec, year)"""

from columns import COLNAMES, TEXT_COLS

EMPTY = int(0)   
QUOTE_CHAR = '"' 

K = COLNAMES.index('1110')
assert K == len(TEXT_COLS)
NEW_TEXT_COLS = [x for x in TEXT_COLS if x != 'name']

def adjust_columns():
    return ['year', 'reviewed', 'org', 'title', 'region', 'ok1', 'ok2', 'ok3'] + \
            NEW_TEXT_COLS + COLNAMES[K:-1]  


def adjust_row(vec, year):
   
    #variable names
    var = dict(zip(TEXT_COLS, vec[0:K]))
            
    # numeric data without last date value
    num_vec = vec[K:-1]  
    numeric_data = adjust_units(var['unit'], num_vec)
    
    # text data
    ok1, ok2, ok3 = okved3(var['okved'])
    org, title = dequote(var['name'])
    if var['inn']:
         region = var['inn'][0:2]
    else:
         region = ''
         print("Warning: INN not specified")
         print(var['inn'])
         print(vec)
         return None 
    
    # cutting 'name' from text_data
    text_data = [var[key] for key in NEW_TEXT_COLS]
    
    # bring date from back of vector
    date_reviewed = vec[-1]
    
    # assemble new row
    return [year, date_reviewed, org, title, region, ok1, ok2, ok3] + \
           text_data + numeric_data 
         

def adjust_units(unit, num_vec):
    """Adjust values in *num_vec* to thousand rub"""  
    if unit == '384': 
        #no adjustment
        return [int(x) for x in num_vec]
    elif unit == '383':
        #adjust rub to thousand rub
        return [int(round(0.001*float(x))) for x in num_vec]            
    elif unit == '385':
        #adjust mln rub to thousand rub 
        return [1000*int(x) for x in num_vec]            
    else:
        raise ValueError("Unsupported unit: " + unit) 

def okved3(code_string): 
    """Get 3 levels of OKVED codes from *code_string* """
    codes = [int(x) for x in code_string.split(".")]
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
    pass