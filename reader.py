"""Generate current and previous year CSV files based on source CSV.
Lists forms 1 (balance), 2 (p&l) and 4(cash flow)."""

import csv
import os

QUOTE_CHAR = '"'
from names import COLNAMES

# http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20150707t000000-structure-20131231t000000.rar
CSV_PATH = os.path.join("rosstat", "G2013.csv")
YEAR = 2013


OKVED_KEYS = ['okved1','okved2','okved3']

def _okved_tuple(code_string): 
    codes = [int(x) for x in str(code_string).split(".")]
    return codes + [None] * (3-len(codes))

def get_okved_dicts(code_string):
    """Extract 3 levels of okved code from code_string line 80.10.02"""
    return dict(zip(OKVED_KEYS, _okved_tuple(code_string)))

def dequote(line):
    """Split company name to orgaisation and tiile"""
    parts = line.split(QUOTE_CHAR)
    org_type = parts[0].strip()
    new_line = QUOTE_CHAR.join(parts[1:-1])
    if new_line.count(QUOTE_CHAR)==1:
        new_line = new_line + QUOTE_CHAR
    if not new_line:
       new_line = line         
    return org_type, new_line.strip()    
    
def get_csv_lines(filename=CSV_PATH, cols=COLNAMES):
    """Read CSV file"""
    with open(filename) as f:
        for line in f:
            if ";" in line:
               text_values = line.strip().split(";")
               yield dict(zip(cols,text_values))                


from names import firm, firm_int_fields, current, prev

data_fields = [x[0:-1] for x in current]
data_labels = current+prev
new = ['year'] + OKVED_KEYS + ['region', 'org', 'title']   
mapper=dict(zip(data_labels,[x[0:-1] for x in data_labels]))

OUTPUT_CSV_COLUMNS = new + firm + data_fields

def lines_as_dicts(filename=CSV_PATH, cols=COLNAMES, year=YEAR, 
                   yield_previous_year=False):

    unit_multipliers={'383':0.001, '384':1, '385':1000}
    
    if yield_previous_year:
        year=year-1
        ix = prev
    else:        
        ix = current

    for d in get_csv_lines(filename, cols):       
           
        r = {'year':year}        
        
        # split okved to 3 numbers            
        r.update(get_okved_dicts(d['okved']))
        
        # add region by INN
        inn_region = int(str(d['inn'])[0:2])
        r.update({'region':inn_region})
        
        # extract org type and title   
        org, title = dequote(d['name'])
        r.update({'org':org, 'title':title})

        # adjust units - standard unit is 384 (thousands)
        # 383 is rubles, must mult by 10^-3, 385 is mln rubles for RJD                 
        m = unit_multipliers[d['unit']]
        r.update((mapper[k], m*int(d[k])) for k in ix)
        r.update((k, int(d[k])) for k in firm_int_fields)
        
        r['name']=d['name']
        r['okved']=d['okved']
        
        yield r


def to_csv(gen, filename, folder="output", cols=OUTPUT_CSV_COLUMNS):

    path = os.path.join(folder, filename)
    
    with open(path, 'w', encoding = "utf-8") as output_file:    
        dict_writer = csv.DictWriter(output_file, cols, delimiter=';', 
                                     lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
        dict_writer.writeheader()
        for d in gen:            
            dict_writer.writerow(d)

if __name__=="__main__":
    
    to_csv(gen=lines_as_dicts(),
           filename="all2013.csv")   
    
    to_csv(gen=lines_as_dicts(yield_previous_year=True),
           filename="all2012.csv")