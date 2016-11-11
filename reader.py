"""

   Download + unpack files + generate current and previous year CSV files based on source CSV.
   Lists forms 1 (balance), 2 (p&l) and 4(cash flow).

   
"""

import requests
import os
import platform
import subprocess
import csv


from column_names import firm, firm_int_fields, current, prev, COLNAMES

QUOTE_CHAR = '"'

SOURCE_CSV_DIR = os.path.join("data")
SOURCE_CSV_PATH = os.path.join(SOURCE_CSV_DIR, "G2013.csv")
YEAR = 2013
SOURCE_URL = "http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20150707t000000-structure-20131231t000000.rar"

IS_WINDOWS = (platform.system() == 'Windows')

if IS_WINDOWS:
    UNPACK_RAR_EXE = os.path.join('bin', 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'

# 
# 
#   Download and unrar
#           
#            
    
class Downloader():
    
    def __init__(self, url, folder=SOURCE_CSV_DIR):
        self.url = url
        filename = url.split('/')[-1]
        self.path = os.path.join(folder, filename)     

    @staticmethod    
    def _download(url, path):
        r = requests.get(url.strip(), stream=True)
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        return path

    def download(self, overwrite=False):
        if not os.path.exists(self.path) or overwrite:
            print("Downloading:", self.url)
            self._download(self.url, self.path)    
            print("Saved as:", self.path)
        else:
            print("Already downloaded:", self.path)
        return self
    
    @staticmethod
    # COMMENT: Having a subprocess works quite good
    # I looked at python solutions, they all seem to need
    # unrar.exe to be in the path anyway
    # For linux, if unrar is not installed subprocess will fail.
    # I would suggest to use .zip files instead and then install
    # the zipfile package (which is a python package)
    # so no external calls are needed
    def _unrar(path, folder=SOURCE_CSV_DIR):        
        subprocess.check_call([
            UNPACK_RAR_EXE,
            'e', path,
            folder,
            '-y'
        ]) 

    def unrar(self, overwrite = False):
        if not os.path.exists(SOURCE_CSV_PATH) or overwrite:
            self._unrar(self.path) 
        else:
            print("Already unpacked:", SOURCE_CSV_PATH)    
 
# 
# 
#   String transformation functions 
#           
#            

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
    
def get_csv_lines(filename=SOURCE_CSV_PATH, cols=COLNAMES):
    """Read CSV file"""
    with open(filename) as f:
        for line in f:
            if ";" in line:
               text_values = line.strip().split(";")
               yield dict(zip(cols,text_values))                

# 
# 
#   Manipulate labels
#           
#            

# COMMENT: This logic here is used to initialize the OUTPUT_CSV_COLUMNS
# but is a bit confusing - perhaps it can be put in a function, like:
# OUPUT_CSV_COLUMNS = generate_output_csv_columns()
data_fields = [x[0:-1] for x in current]
data_labels = current+prev
new = ['year'] + OKVED_KEYS + ['region', 'org', 'title']   
# cut last digit off the code
mapper=dict(zip(data_labels,[x[0:-1] for x in data_labels]))

OUTPUT_CSV_COLUMNS = new + firm + data_fields

# 
# 
#   Read csv
#           
# 

# COMMENT: I suppose here you are not using pandas because of memory concerns?
# Because getting the rows as dictionaries is very easy in a pandas DataFrame


def lines_as_dicts(filename=SOURCE_CSV_PATH, cols=COLNAMES, year=YEAR, 
                   yield_previous_year=False):
                       
    """Yield lines from csv file as dictionary."""

    unit_multipliers={'383':0.001, '384':1, '385':1000}
    
    # COMMENT: Here, prev and current looks like it's a variable, whereas it's really a constant 
    # imported from a module. You can rename to PREV_YEAR_INDEX and CURR_YEAR_INDEX (or something else
    # that is an informative name)
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


# COMMENT:
# I would rename this to: output_to_csv
# COMMENT:
# Reading/Writing the data one row at a time is good for memory usage
# But there's a hybrid solution where you read N lines and write N lines together
# which can have better performance (because writing takes the same time if you write a small chunk of a data
# vs a slightly bigger one. So, if writing takes a lot of time, writing multiple rows at once is something to consider
def to_csv(gen, filename, folder=SOURCE_CSV_DIR, cols=OUTPUT_CSV_COLUMNS):
    path = os.path.join(folder, filename)
    with open(path, 'w', encoding = "utf-8") as output_file:    
        dict_writer = csv.DictWriter(output_file, cols, delimiter=';', 
                                     lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
        dict_writer.writeheader()
        for d in gen:            
            dict_writer.writerow(d)
            


if __name__=="__main__":
    # COMMENT: This is the entrypoint to the whole thing. I would actually put explicitly in the keyword arguments
    # because without them it's not clear what you are getting and where you are saving it
    Downloader(SOURCE_URL).download().unrar()
    to_csv(gen=lines_as_dicts(), filename="all2013.csv")   
    to_csv(gen=lines_as_dicts(yield_previous_year=True), filename="all2012.csv")
