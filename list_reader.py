# -*- coding: utf-8 -*-
"""Read source CSV file and adjust numeric units."""

import csv
from remote import RemoteDataset

from column_names import COLNAMES

EMPTY = ''    
QUOTE_CHAR = '"' 

YEAR = 2013
CSV_FILENAME = RemoteDataset(YEAR).source_csv_path

def get_specs(cols):
    # numeric data start index
    K = cols.index('11103')
    
    # text columns
    CHAR_COLUMNS = dict(original=cols[0:K], modified=[x for x in cols[0:K] if x != 'name']) 
    return K, CHAR_COLUMNS
    
K, CHAR_COLUMNS = get_specs(COLNAMES)

def get_csv_lines(filename=CSV_FILENAME):
    with open(filename, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';') # encoding="cp1251"
        for row in spamreader:
           if len(row)>1: #avoids reading last rows 
                yield row           

def csv_block(count, skip=0, filename=CSV_FILENAME):
    k = 0 
    for i, row in enumerate(get_csv_lines(filename)):
        if i<skip:
            continue
        if k<count: 
            yield row
            k+=1
        else:
            break

def last_rows():
    for x in csv_block(100000, skip=1742400):
        yield parsed(x)

def parsed_rows(n=None, skip=0, filename=CSV_FILENAME):     
    chunk=10000
    if n:
        gen = csv_block(n, skip, filename)
    else:
        gen = get_csv_lines(filename)
        
    i=1; k=0
    for r in gen:
        yield parse_row(r)
        i+=1
        if i==chunk:
            i=0; k+=1
            print(chunk*k) 

            
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
       # warning: will not work well on titles with more than 4 quotechars 
    else:
       title = line         
    return [org, title.strip()]    

def split_row(vec, k=K, cols=CHAR_COLUMNS['original']):    
    char_dict = dict(zip(cols,vec[0:k]))
    return char_dict, vec[k:]
    
def parse_row(vec, year=YEAR, cols=CHAR_COLUMNS['modified']):    
    # split vector
    vars, num_vec = split_row(vec)      
    
    # numeric data 
    numeric_data = adjust_units(vars['unit'], num_vec)

    # text data vec
    ok1, ok2, ok3 = okved3(vars['okved'])
    org, title = dequote(vars['name'])
    region = int(vars['inn'][0:2])
    
    # cutting 'name' from text_data
    text_data = [vars[k] for k in cols]
    
    # assemble vector back
    return [year, org, title, region, ok1, ok2, ok3] + text_data + numeric_data 
       
    
def parse_colnames(col=COLNAMES):
    return ['year', 'org', 'title', 'region', 'ok1', 'ok2', 'ok3'] + [x for x in col if x!='name']


def to_csv(path, gen, cols):    
    with open(path, 'w', encoding = "utf-8") as file:
        writer = csv.writer(file, delimiter=";", lineterminator="\n", 
                              quoting=csv.QUOTE_MINIMAL)
        writer.writerow(cols)
        writer.writerows(gen)
    print("Saved file:", path)    
        
def update(year):
    input_csv = RemoteDataset(year).download().unrar()
    output_csv = RemoteDataset(year).get_new_csv_filename()
    # colnames, K      
    gen = parsed_rows(filename=input_csv)
    cols = parse_colnames()    
    to_csv(output_csv, gen, cols)
    
if __name__=="__main__":
    update(2013)