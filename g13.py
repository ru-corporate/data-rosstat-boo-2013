import csv
import pandas as pd

BLN = 10**6
QUOTE_CHAR = '"'

from names import COLNAMES
CSV_PATH = "G2013.csv"

OKVED_KEYS = ['okved1','okved2','okved3']

def okved_tuple(code_string): 
    code_string = str(code_string)
    codes = [int(x) for x in code_string.split(".")]
    return codes + [None] * (3-len(codes))

tests = {"01": [1, None, None]
    , "44.20": [44,20,None]
  , "1.10.11": [1, 10,  11]}
  
for k, v in tests.items():
    assert okved_tuple(k) == v  

def okved_dict(code_string):
    return dict(zip(OKVED_KEYS, okved_tuple(code_string)))

def _dequote(line):
     parts = line.split(QUOTE_CHAR)
     org_type = parts[0].strip()
     new_line = QUOTE_CHAR.join(parts[1:-1])
     if new_line.count(QUOTE_CHAR)==1:
         new_line = new_line + QUOTE_CHAR
     return org_type, new_line    
    
def to_int(x):
   try:
      return int(x)
   except:
      return x   
      
def lines_as_dicts(filename=CSV_PATH, cols=COLNAMES):
    with open(filename) as f:
        for line in f:
        
            text_values = line.strip().split(";")
            values = [to_int(x) for x in text_values]            
            d = dict(zip(cols,values))   

            # split okved to 3 numbers            
            d.update(okved_dict(d['okved']))
            
            # add region ny INN
            inn_region = int(str(d['inn'])[0:2])
            d.update({'region':inn_region})
            
            # extract org type and title   
            org, title = _dequote(d['name'])
            d.update({'org':org, 'title':title})
  
            # 383 and 385, standard unit is 384 (thousands)
            unit_multipliers={383:10**-3, 384:1, 385:1}
            m = unit_multipliers[d['unit']]
            d.update({'mult':m})   

            # todo/warning - will not write all new vakues to file!!!               
            
            yield d            
      
      
def filtered_dicts(sales_treshold=0):
    for d in lines_as_dicts():        
        if '21103' in d.keys() and d['21103'] > sales_treshold:
            yield d 

          
def truncate(iter, max_count):
    """Shorter reader, max count."""  
    i = 1
    while i <= max_count:
         try:
             yield next(iter)     
             i += 1             
         except IndexError:
              break              
         except StopIteration:
              break           

def to_csv(gen, filename):
    with open(filename, 'w', encoding = "utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, COLNAMES, delimiter=';', 
                                     lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
        dict_writer.writeheader()
        for i, d in enumerate(gen):
            print (i)
            dict_writer.writerow(d)

            
# ==================================================
#
# Excercise 1:  compare data to real company report 
#
# ==================================================

gen = filtered_dicts()
z = [next(gen) for _ in range(75)]  

  
# as seen at http://www.vng.com.ru/files/2013/2013.Forma.N.1.buhgalterskij.balans.pdf    
d = z[56]
assert d['name'] == 'Открытое акционерное общество "Волгограднефтегеофизика"'
assert d['inn'] == 3446006100
# 3 is current year = 2013
assert d['11503'] == 189705
# 4 is previous year = 2012
assert d['11504'] == 288365

# ==================================================
#
# Excercise 2:  get 10 dicts
#
# ==================================================

gen = filtered_dicts()
for x in truncate(gen, 10):
    print(x['org'], x['title'])
    print(x['okved'], x['okved1'], x['okved2'], x['okved3'])
    print(x['region'], x['inn']) 
    print(x['mult']) 
    print("---------------------------")


# ==================================================
#
# Excercise 3:  write to csv
#
# ==================================================


#to_csv(filtered_dicts(sales_treshold=BLN)
#       , "bln.csv")

#to_csv(filtered_dicts(sales_treshold=0.5*BLN)
#       , "half.csv")

#to_csv(filtered_dicts(sales_treshold=0)
#       , "all.csv")    


# ==================================================
#
# make dataframe, gets out of memory
#
# ==================================================

#df = pd.DataFrame(gen)   
#df = pd.read_csv(file,sep=';', header=0,names=COLNAMES)
#pandas.read_csv(filepath_or_buffer, sep=', ', delimiter=None, header='infer', names=None, index_col=None, usecols=None, squeeze=False, prefix=None, mangle_dupe_cols=True, dtype=None, engine=None, converters=None, true_values=None, false_values=None, skipinitialspace=False, skiprows=None, skipfooter=None, nrows=None, na_values=None, keep_default_na=True, na_filter=True, verbose=False, skip_blank_lines=True, parse_dates=False, infer_datetime_format=False, keep_date_col=False, date_parser=None, dayfirst=False, iterator=False, chunksize=None, compression='infer', thousands=None, decimal='.', lineterminator=None, quotechar='"', quoting=0, escapechar=None, comment=None, encoding=None, dialect=None, tupleize_cols=False, error_bad_lines=True, warn_bad_lines=True, skip_footer=0, doublequote=True, delim_whitespace=False, as_recarray=False, compact_ints=False, use_unsigned=False, low_memory=True, buffer_lines=None, memory_map=False, float_precision=None)
    
#In [47]: data2 = [{'a': 1, 'b': 2}, {'a': 5, 'b': 10, 'c': 20}]
#In [48]: pd.DataFrame(data2)
    
    
