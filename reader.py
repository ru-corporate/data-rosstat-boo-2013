"""

   Read source CSV file - forms 1(balance), 2(p&l) and 4(cash flow).
   
"""

import csv

from column_names import COLNAMES  
from column_names import ap, opu, cf
from downloader import SOURCE_CSV_DIR, SOURCE_CSV_PATH

YEAR = 2013
QUOTE_CHAR = '"'
OKVED_KEYS = ['okved1','okved2','okved3']

def get_csv_lines(filename=SOURCE_CSV_PATH, cols=COLNAMES):
    """Read CSV file"""
    with open(filename) as csvfile:
        spamreader = csv.reader(csvfile, delimiter=";")
        for row in spamreader:
            yield dict(zip(cols,row))    

# 
# 
#   String transformation functions 
#           
#            

def get_okved_dict(code_string):
    """Extract 3 levels of okved code from code_string line 80.10.02"""
    def _okved_tuple(code_string): 
        codes = [int(x) for x in str(code_string).split(".")]
        return codes + [None] * (3-len(codes))        
    return dict(zip(OKVED_KEYS, _okved_tuple(code_string)))

def dequote(line):
    """Split company name to organisation and tiile"""
    parts = line.split(QUOTE_CHAR)
    org_type = parts[0].strip()
    new_line = QUOTE_CHAR.join(parts[1:-1])
    if new_line.count(QUOTE_CHAR)==1:
        new_line = new_line + QUOTE_CHAR
    if not new_line:
       new_line = line         
    return org_type, new_line.strip()    
    
#   Manipulate labels
cur_year_data_labels  = [x for x in ap+opu+cf if x.endswith("3")]
prev_year_data_labels = [x for x in ap+opu+cf if x.endswith("4")]
# cut last digit off the code
mapper=dict(zip(data_labels,[x[0:-1] for x in ap+opu+cf]))
numeric_fields = ap+opu+cf  
unit_multipliers={'383':0.001, '384':1, '385':1000}

def adjust_numeric_values(d):
    # adjust units - standard unit is 384 (thousands)
    # 383 is rubles, must mult by 10^-3, 
    # 385 is mln rubles, must multiply by 10**3     
    m = unit_multipliers[d['unit']]
    d.update((k, m*int(d[k])) for k in numeric_fields)
    return d

assert {'unit':'383'}.update(dict(zip(numeric_fields, 1000)))['21103'] == 1 
    
    
def get_company_attributes(d):            
    # split okved to 3 numbers            
    result_dict = get_okved_dict(d['okved'])       
    # add region by INN
    result_dict['region'] = int(str(d['inn'])[0:2])})        
    # extract org type and title   
    org, title = dequote(d['name'])
    result_dict.update({'org':org, 'title':title})    
    #see if inn must be turned to int
    result_dict['inn'] = d['inn']    
    # int fields
    result_dict.update({k:int(d(k)) for k in ['okpo', 'okopf', 'okfs', 'unit', 'report_type']})    
    return result_dict    

    
def get_codes(d, labels):
    d.update((mapper[k],d[k]) for k in dict.keys())
    return {k:v for k, v in d.items() if k in cols}
        
def refine(d):
    d = adjust_numeric_values(d)
    ky = get_codes(d, cur_year_data_labels).update(YEAR)
    kp = get_codes(d, prev_year_data_labels).update(YEAR-1)
    return(YEAR)
    
def lines_as_dicts(filename=SOURCE_CSV_PATH, cols=COLNAMES, year=YEAR, 
                   yield_previous_year=False):
                       
    """Yield lines from CSV file as dictionary."""

    for d in get_csv_lines(filename, cols):       
        r = {'year':year}        
        
    
            



def to_csv(gen, filename, folder=SOURCE_CSV_DIR, cols=OUTPUT_CSV_COLUMNS):
    path = os.path.join(folder, filename)
    with open(path, 'w', encoding = "utf-8") as output_file:    
        dict_writer = csv.DictWriter(output_file, cols, delimiter=';', 
                                     lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
        dict_writer.writeheader()
        for d in gen:            
            dict_writer.writerow(d)
            `   0123
            
             
#data_fields = [x[0:-1] for x in current]
#data_labels = current+prev


new = ['year'] + OKVED_KEYS + ['region', 'org', 'title']   
firm = ['okpo', 'okopf', 'okfs', 'unit', 'report_type']
OUTPUT_CSV_COLUMNS = new + firm + data_fields

            
            

if __name__=="__main__":
    to_csv(gen=lines_as_dicts(), filename="all2013.csv")   
    to_csv(gen=lines_as_dicts(yield_previous_year=True), filename="all2012.csv")