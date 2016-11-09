# -*- coding: utf-8 -*-
"""Use column specification to parse rows."""

from column_names import COLNAMES_2013

EMPTY = ''    
QUOTE_CHAR = '"' 

class RowParser():
    
    def __init__(self, year=2013):
        self.year = year
        if year == 2013:
            incoming_columns = COLNAMES_2013
            self.k = incoming_columns.index('1110')
            self.char_cols = incoming_columns[0:self.k]           
            self.new_char_cols = [x for x in incoming_columns[0:self.k] if x != 'name']
            self.columns = ['year', 'org', 'title', 'region', 'ok1', 'ok2', 'ok3'] + \
                           self.new_char_cols + incoming_columns[self.k:]            
        else: 
            raise ValueError("Year not supported: " +  str(year))
    
    def adjust_row(self, vec):
        # split vector
        num_vec = vec[self.k:]  
        vars = dict(zip(self.char_cols, vec[0:self.k]))
        
        # numeric data 
        numeric_data = adjust_units(vars['unit'], num_vec)

        # text data
        ok1, ok2, ok3 = okved3(vars['okved'])
        org, title = dequote(vars['name'])
        region = int(vars['inn'][0:2])
        
        # cutting 'name' from text_data
        text_data = [vars[k] for k in self.new_char_cols]
        
        # assemble row back
        return [self.year, org, title, region, ok1, ok2, ok3] + text_data + numeric_data 
        
    def columns(self):
        return self.columns    

def adjust_units(unit, num_vec):
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

def dequote(line):
    """Split company name to organisation and title"""
    parts = line.split(QUOTE_CHAR)
    org = parts[0].strip()
    cnt = line.count(QUOTE_CHAR)    
    if cnt==2:
       title = parts[1].strip()
    elif cnt>2:
       title = QUOTE_CHAR.join(parts[1:])
       # warning: will not work well on company names with more than 4 quotechars 
    else:
       title = line         
    return [org, title.strip()]  
    
    
if __name__=="__main__":
    pass