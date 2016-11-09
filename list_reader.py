"""Read source CSV file and adjust numeric units."""

import csv

from remote import RemoteDataset
from column_names import COLNAMES  

# check if file is downloaded and get filenames
year = 2013
CSV_FILENAME = RemoteDataset(year).download().unrar()
CSV_FILENAME_2 = RemoteDataset(year).get_new_csv_filename()

# locate variable positions in csv row
unit_pos = COLNAMES.index('unit')
assert unit_pos == 6 
numeric_start_pos = COLNAMES.index('11103')
assert numeric_start_pos == 8
okved_pos = COLNAMES.index('okved')
assert okved_pos == 4
name_pos = COLNAMES.index('name')
assert name_pos == 0 

#string_columns  = COLNAMES[0:numeric_start_pos] # ['name', 'okpo', 'okopf', 'okfs', 'okved', 'inn', 'unit', 'report_type' ]
#numeric_columns = COLNAMES[numeric_start_pos:]  # ['11103', '11104', '11203'...

def get_csv_lines(filename=CSV_FILENAME):
    with open(filename, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';') # encoding="cp1251"
        for row in spamreader:
           yield row           

def lines(count, skip=0):
    k = 0 
    for i, row in enumerate(get_csv_lines()):
        if i<skip:
            continue
        if k<count: 
            yield row
            k+=1
        else:
            break
    
def get_numeric_vector(vec):
    unit = vec[unit_pos]
    num_vec = vec[numeric_start_pos:0]
    if unit == '384': 
        #no adjustment
        return num_vec
    elif unit == '383':
        #adjust rub to thousand rub
        return [int(round(0.001*float(x))) for x in num_vec]            
    elif unit == '385':
        #adjust mln rub to thousand rub 
        return [1000*int(x) for x in v[numeric_start_pos:0]]            
    else:
        raise ValueError("Unsupported unit: " + unit) 

EMPTY = ''    
QUOTE_CHAR = '"' 
def get_okved_by_level(code_string): 
    codes = [int(x) for x in code_string.split(".")]
    return codes + [EMPTY] * (3-len(codes))        

def dequote(line):
   """Split company name to organisation and title"""
   parts = line.split(QUOTE_CHAR)
   org_type = parts[0].strip()
   new_line = QUOTE_CHAR.join(parts[1:-1])
   if new_line.count(QUOTE_CHAR)==1:
       new_line = new_line + QUOTE_CHAR
   if not new_line:
       new_line = line         
   return [org_type, new_line.strip()]    

if __name__=="__main__":
    v = next(lines(1))
    text_data = v[0:numeric_start_pos]
    numeric_data = get_numeric_vector(v)  
    okved_list = get_okved_by_level(code_string=v[okved_pos])
    
    h = []
    for x in lines(1000, skip=0):
        tn = x[name_pos]
        if tn.count(QUOTE_CHAR) != 2:
            print (tn, dequote(tn))
            h += [{tn:dequote(tn)}]
    
    assert dequote('Открытое акционерное общество "База отдыха "Энергетик"') == \
           ['Открытое акционерное общество', 'База отдыха "Энергетик"']
    assert dequote ('Общество с ограниченной ответственностью "РИОНИ"') == \    
           ['Общество с ограниченной ответственностью', 'РИОНИ']