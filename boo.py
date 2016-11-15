from reader import Dataset
#from slicer import Slices
from config import VALID_YEARS

for year in VALID_YEARS:
    # Download, unrar, parse and save clean dataset as local CSV file
    Dataset(year).create_clean_copy() 
    
    # Save a family of slices from Rosstat dataset as csv/xlsx files:
    #     boo_*.csv           - all companies, selected variables (~300 Mb)
    #     main_*.csv          - excludes 'micro' enterprises (sales < 120 mln rub), 
    #                           but includes companies with assets above 30(?) mln
    #     bln_*.csv and .xlsx - companies above 1 bln rub in sales
    
    # Slices(year).create_files() 
    # check if os.path.exists()
    # raise FilePathError: file not found. Use RosstatCSV

