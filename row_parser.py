# -*- coding: utf-8 -*-
"""Use column specification to adjust rows."""

from column_names import COLNAMES

EMPTY = ''    
QUOTE_CHAR = '"' 

class Parser():
    
    def __init__(self, year=2013):
        self.year = year
        if year == 2013:
            incoming_columns = COLNAMES
            self.k = incoming_columns.index('11103')
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

        # text data vec
        ok1, ok2, ok3 = okved3(vars['okved'])
        org, title = dequote(vars['name'])
        region = int(vars['inn'][0:2])
        
        # cutting 'name' from text_data
        text_data = [vars[k] for k in self.new_char_cols]
        
        # assemble vector back
        # Note: corresponding colnames are Parser(year).columns or parse_col()
        return [self.year, org, title, region, ok1, ok2, ok3] + text_data + numeric_data 
        
    def parse_col(self):
        return self.columns    

def adjust_units(unit, num_vec):
    if unit == '384': 
        #no adjustment
        return num_vec
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
    v = ['Открытое акционерное общество "Российское акционерное общество по производству цветных и драгоценных металлов "Норильский никель"', '00002565', '12247', '16', '65.23.1', '2457009983', '384', '2', '150', '150', '0', '0', '0', '0', '0', '0', '21', '56', '0', '0', '3129154', '3129154', '35332', '18558', '0', '0', '3164657', '3147918', '23', '23', '0', '0', '3728', '1951', '2970548', '2900387', '31526', '13763', '0', '0', '3005825', '2916124', '6170482', '6064042', '47250', '47250', '0', '0', '0', '0', '2266991', '2266991', '7087', '7087', '3848006', '3741048', '6169334', '6062376', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '226', '360', '0', '0', '922', '1306', '0', '0', '1148', '1666', '6170482', '6064042', '2245363', '2951506', '2100227', '2770211', '145136', '181295', '0', '0', '31315', '52939', '113821', '128356', '13748', '29792', '5230', '1364', '0', '0', '42', '58', '3508', '12216', '129333', '147354', '40038', '27104', '-18723', '-18867', '0', '0', '32894', '16500', '15231', '14258', '106958', '122492', '0', '0', '0', '0', '106958', '122492', '47250', '0', '2266991', '7087', '3741048', '6062376', '0', '0', '0', '0', '106958', '106958', '106958', '106958', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '47250', '0', '2266991', '7087', '3848006', '6169334', '6169334', '3062376', '2249685', '0', '57', '0', '2249628', '2245685', '8132', '20362', '0', '154', '2217037', '4000', '13748', '0', '0', '0', '13748', '0', '0', '0', '0', '0', '0', '0', '13748', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '17748', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0']
    p = Parser(2013)
    v2 = p.adjust_row(v)   
    print(v2)