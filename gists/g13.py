import csv

BLN = 10**6
QUOTE_CHAR = '"'

from names import COLNAMES, COLDICT

# http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20150707t000000-structure-20131231t000000.rar
CSV_PATH = "G2013.csv"
YEAR=2013

#
# Extract 3 levels of okved code from string
#

OKVED_KEYS = ['okved1','okved2','okved3']

def okved_tuple(code_string): 
    codes = [int(x) for x in str(code_string).split(".")]
    return codes + [None] * (3-len(codes))

tests = {   1: [1, None, None]
       , "01": [1, None, None]
    , "44.20": [44,20,None]
  , "1.10.11": [1, 10,  11]}
  
for k, v in tests.items():
    assert okved_tuple(k) == v  

def get_okved_dict(code_string):
    return dict(zip(OKVED_KEYS, okved_tuple(code_string)))

#
# Split company name to orgaisation and tiile
#

def _dequote(line):
     parts = line.split(QUOTE_CHAR)
     org_type = parts[0].strip()
     new_line = QUOTE_CHAR.join(parts[1:-1])
     if new_line.count(QUOTE_CHAR)==1:
         new_line = new_line + QUOTE_CHAR
     if not new_line:
        new_line = line         
     return org_type, new_line.strip()    
    
#
# Read file
#

def get_csv_lines(filename=CSV_PATH, cols=COLNAMES):
    with open(filename) as f:
        for line in f:
            if ";" in line:
               text_values = line.strip().split(";")
               yield dict(zip(cols,text_values)) 

new = OKVED_KEYS + ['region', 'org', 'title']
firm = COLDICT['firm']
current = COLDICT['current']
true_current = COLDICT['true_current']
prev = COLDICT['previous']
true_prev = COLDICT['true_prev']   
firm_int = COLDICT['firm_int']

def lines_as_dicts(filename=CSV_PATH, cols=COLNAMES, 
                   year=YEAR, yield_previous_year=False):
    
    for d in get_csv_lines(filename, cols):

        # adjust units
        # standard unit is 384 (thousands)
        # 383 is rubles, mus mult by 10^-3
        # 385 is roubles for RJD                 
        unit_multipliers={'383':10**-3, '384':1, '385':1}
        m = unit_multipliers[d['unit']]
            
        d.update((k, m*int(d[k])) for k in current + prev)
        d.update((k, int(d[k])) for k in firm_int)
        
        # split okved to 3 numbers            
        d.update(get_okved_dict(d['okved']))
        
        # add region by INN
        inn_region = int(str(d['inn'])[0:2])
        d.update({'region':inn_region})
        
        # extract org type and title   
        org, title = _dequote(d['name'])
        d.update({'org':org, 'title':title})
        
        if yield_previous_year:
            prev_d = {'year':year-1}
            prev_d.update((k, d[k]) for k in firm + new + prev)
            for ok, nk in zip(prev, true_prev):
                prev_d["_"+nk]=prev_d.pop(ok) 
            yield prev_d                   
                
        else:
            cur_d = {'year':year} 
            cur_d.update((k, d[k]) for k in firm + new + current)
            for ok, nk in zip(current, true_current):
                cur_d["_"+nk]=cur_d.pop(ok)
            yield cur_d
                     
           

OUTPUT_CSV_COLUMNS = ['year'] + new + firm  + ["_"+x for x in true_current]

def to_csv(gen, filename, cols=OUTPUT_CSV_COLUMNS):
    with open(filename, 'w', encoding = "utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, cols, delimiter=';', 
                                     lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
        dict_writer.writeheader()
        for i, d in enumerate(gen):
            print (i)
            dict_writer.writerow(d)


def filtered_dicts(sales_treshold=0):
    i = 0
    for d in lines_as_dicts():        
        if '_2110' in d.keys() and d['_2110'] > sales_treshold:
            print(i, d['title'], d['_2110'])
            i = i + 1
            yield d 

def filtered_dicts2():
    i = 0
    for d in lines_as_dicts():
        flag1 = '_2110' in d.keys() and d['_2110'] > BLN
        flag2 = '_1410' in d.keys() and d['_1410'] > BLN
        #print(flag1, flag2)
        if flag1 or flag2:
            print(i, d['title'], d['_2110'])
            i = i + 1
            yield d 
            

if __name__=="__main__":  
    #to_csv(filtered_dicts(sales_treshold=100*BLN), "_100bln2013.csv") 
    #to_csv(filtered_dicts(sales_treshold=BLN), "_bln2013.csv")    
    #to_csv(gen = lines_as_dicts(), filename="_all2013.csv")

#sales_treshold = BLN
#debt_treshold = 0 
#for d in lines_as_dicts(): 
#        flag1 = '2110' in d.keys() and d['2110'] > sales_treshold
#        flag2 = '1410' in d.keys() and d['1410'] > debt_treshold
#        if flag1 or flag2:
#            print (d['title']) 

 
#          
#def truncate(iter, max_count):
#    """Shorter reader, max count."""  
#    i = 1
#    while i <= max_count:
#         try:
#             yield next(iter)     
#             i += 1             
#         except IndexError:
#              break              
#         except StopIteration:
#              break           
#
#def get(n):
#    gen = filtered_dicts() 
#    return [x for x in truncate(gen, n)] 
#
#
#def to_csv(gen, filename):
#    with open(filename, 'w', encoding = "utf-8") as output_file:
#        dict_writer = csv.DictWriter(output_file, COLNAMES, delimiter=';', 
#                                     lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
#        dict_writer.writeheader()
#        for i, d in enumerate(gen):
#            print (i)
#            dict_writer.writerow(d)
#
#            
## ==================================================
##
## Excercise 1:  compare data to real company report 
##
## ==================================================
#
#gen = filtered_dicts()
#z = [next(gen) for _ in range(75)]  
#
#  
## as seen at http://www.vng.com.ru/files/2013/2013.Forma.N.1.buhgalterskij.balans.pdf    
#d = z[56]
#assert d['name'] == 'Открытое акционерное общество "Волгограднефтегеофизика"'
#assert d['inn'] == 3446006100
## 3 is current year = 2013
#assert d['11503'] == 189705
## 4 is previous year = 2012
#assert d['11504'] == 288365
#
## ==================================================
##
## Excercise 2:  get 10 dicts
##
## ==================================================
#
#gen = filtered_dicts()
#for x in truncate(gen, 10):
#    print(x['org'], x['title'])
#    print(x['okved'], x['okved1'], x['okved2'], x['okved3'])
#    print(x['region'], x['inn']) 
#    print(x['mult']) 
#    print("---------------------------")
#
#
## ==================================================
##
## Excercise 3:  write to csv
##
## ==================================================
#
#
##to_csv(filtered_dicts(sales_treshold=BLN)
##       , "bln.csv")
#
##to_csv(filtered_dicts(sales_treshold=0.5*BLN)
##       , "half.csv")
#
##to_csv(filtered_dicts(sales_treshold=0)
##       , "all.csv")    
#
#
## ==================================================
##
## make dataframe, gets out of memory
##
## ==================================================
#
##df = pd.DataFrame(gen)   
##df = pd.read_csv(file,sep=';', header=0,names=COLNAMES)
##pandas.read_csv(filepath_or_buffer, sep=', ', delimiter=None, header='infer', names=None, index_col=None, usecols=None, squeeze=False, prefix=None, mangle_dupe_cols=True, dtype=None, engine=None, converters=None, true_values=None, false_values=None, skipinitialspace=False, skiprows=None, skipfooter=None, nrows=None, na_values=None, keep_default_na=True, na_filter=True, verbose=False, skip_blank_lines=True, parse_dates=False, infer_datetime_format=False, keep_date_col=False, date_parser=None, dayfirst=False, iterator=False, chunksize=None, compression='infer', thousands=None, decimal='.', lineterminator=None, quotechar='"', quoting=0, escapechar=None, comment=None, encoding=None, dialect=None, tupleize_cols=False, error_bad_lines=True, warn_bad_lines=True, skip_footer=0, doublequote=True, delim_whitespace=False, as_recarray=False, compact_ints=False, use_unsigned=False, low_memory=True, buffer_lines=None, memory_map=False, float_precision=None)
#    
##In [47]: data2 = [{'a': 1, 'b': 2}, {'a': 5, 'b': 10, 'c': 20}]
##In [48]: pd.DataFrame(data2)