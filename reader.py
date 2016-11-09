"""
   Read source CSV file and adjust numeric units.
   
"""

import pandas as pd

from downloader import Downloader
from config import TARGET_CSV_PATH
from column_names import COLNAMES  

# check if file is downloaded 
SOURCE_CSV_PATH = Downloader().download().unrar()


numeric_columns = COLNAMES[8:]  # ['11103', '11104', '11203'...
string_columns  = COLNAMES[0:8] # ['name', 'okpo', 'okopf', 'okfs', 'okved', 'inn', 'unit', 'report_type' ]

# write headers to csv file 
pd.DataFrame(columns=COLNAMES).to_csv(TARGET_CSV_PATH, encoding='utf-8')

# parameters of source csv file import
cre = dict(filepath_or_buffer=SOURCE_CSV_PATH, 
           sep=';', 
           encoding="cp1251",
           names=COLNAMES, 
           dtype={'inn':str})

chunks = pd.read_csv(**cre,chunksize=100000)
for i, df in enumerate(chunks):

    # uncomment when debugging
    #if i > 0: break
   
    # delete last line with nulls
    # Note: I was trying to get rid of this line with 'skipfooter=1',
    #       but then I was getting a conflict with 'dtype' specification
    if any(df.iloc[-1:].isnull()):
       df=df[:-1]
       # better use drop
    
    # adjust numeric units
    # we need to multiply numeric values in several rows by a factor of .001 or 1000
    multipliers={383:0.001,  385:1000}
    for k, m in multipliers.items(): 
        index = (df.unit==int(k)) | (df.unit==str(k)) 
        df.loc[index,numeric_columns] = df.loc[index,numeric_columns].multiply(m).round(0)
    
    df.to_csv(TARGET_CSV_PATH, mode='a', encoding='utf-8', header = False)


# Problem 1: adjusting numeric units in lines 42-46 
#     (a) seems very slow, 
#     (b) I'm using iloc, but still getting a SettingWithCopyWarning: 
#         A value is trying to be set on a copy of a slice from a DataFrame.
#         Try using .loc[row_indexer,col_indexer] = value instead  

# Problem 2: 
#     for numeric values the data type is integer, not float
#     in my source file I have to last line of NA values, so I cannot use dtype=int, nor skipfooter=1 
#     in a csv file I want to keep all numeric columns ('numeric_columns') as int type
#     the resulting file TARGET_CSV_PATH is about 2 time bigger than original SOURCE_CSV_PATH file,
#     which is very upsetting 


# Below is not todo.


#ff = pd.read_csv(OUTPUT)

## 
##   String transformation functions 
##           

#def get_okved_dict(code_string):
#    """Extract 3 levels of okved code from code_string line 80.10.02"""
#    def _okved_tuple(code_string): 
#        codes = [int(x) for x in str(code_string).split(".")]
#        return codes + [None] * (3-len(codes))        
#    return dict(zip(OKVED_KEYS, _okved_tuple(code_string)))
#
#def dequote(line):
#    """Split company name to organisation and tiile"""
#    parts = line.split(QUOTE_CHAR)
#    org_type = parts[0].strip()
#    new_line = QUOTE_CHAR.join(parts[1:-1])
#    if new_line.count(QUOTE_CHAR)==1:
#        new_line = new_line + QUOTE_CHAR
#    if not new_line:
#       new_line = line         
#    return org_type, new_line.strip()    
#    
