import csv
import pandas as pd

BLN = 10**6

from names import COLNAMES
CSV_PATH = "G2013.csv"

def to_int(x):
   try:
      return int(x)
   except:
      return x

def lines_as_dicts(filename=CSV_PATH, cols=COLNAMES):
    i = 0 
    with open(filename) as f:
        for line in f:
            text_values = line.strip().split(";")
            values = [to_int(x) for x in text_values] 
            values[4]=text_values[4].replace(".","_")
            d = dict(zip(cols,values))
            yield d               
      
def filtered_dicts(sales_treshold=0):
    for d in lines_as_dicts():        
        if '21103' in d.keys() and d['21103'] > sales_treshold:
            yield d 
            
            


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
# Excercise 2:  get sales form all companies 
#
# ==================================================

gen = filtered_dicts(sales_treshold=BLN*.5)
#lst = [next(gen) for _ in range(10)]



with open("half.csv", 'w', encoding = "utf-8") as output_file:
    dict_writer = csv.DictWriter(output_file, COLNAMES, delimiter=';', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    dict_writer.writeheader()
    for i, d in enumerate(gen):
        print (i)
        dict_writer.writerow(d)
        
        
df = pd.read_csv("test.csv", delimiter=";")
    



#df = pd.DataFrame([x['21103'] for x in lines(sales_treshold=10000000)])

#BLN = 10**6


#s = pd.DataFrame([x['21103'] for x in filtered_dicts(sales_treshold=50*BLN)])

# for e, x in enumerate(filtered_dicts(sales_treshold=1000*BLN)):
    # try:
        # print (e, round(x['21103']/BLN,1), x['name'])   
    # except:
        # print (e, round(x['21103']/BLN,1), "Name not read.")   
# gen = lines(sales_treshold=10*10^6)


#r = [x['21103'] for x in lines(sales_treshold=1000*BILLION)]


#for x in lines(sales_treshold=50 * BILLION):
#    print (round(x['21103'] / BILLION, 1), x['Наименование']) 





# ==================================================
#
# Excercise 0: make dataframe, gets out of memory
#
# ==================================================

#df = pd.DataFrame(gen)   
#df = pd.read_csv(file,sep=';', header=0,names=COLNAMES)
#pandas.read_csv(filepath_or_buffer, sep=', ', delimiter=None, header='infer', names=None, index_col=None, usecols=None, squeeze=False, prefix=None, mangle_dupe_cols=True, dtype=None, engine=None, converters=None, true_values=None, false_values=None, skipinitialspace=False, skiprows=None, skipfooter=None, nrows=None, na_values=None, keep_default_na=True, na_filter=True, verbose=False, skip_blank_lines=True, parse_dates=False, infer_datetime_format=False, keep_date_col=False, date_parser=None, dayfirst=False, iterator=False, chunksize=None, compression='infer', thousands=None, decimal='.', lineterminator=None, quotechar='"', quoting=0, escapechar=None, comment=None, encoding=None, dialect=None, tupleize_cols=False, error_bad_lines=True, warn_bad_lines=True, skip_footer=0, doublequote=True, delim_whitespace=False, as_recarray=False, compact_ints=False, use_unsigned=False, low_memory=True, buffer_lines=None, memory_map=False, float_precision=None)
    
#In [47]: data2 = [{'a': 1, 'b': 2}, {'a': 5, 'b': 10, 'c': 20}]
#In [48]: pd.DataFrame(data2)
    
    
