from pandas import DataFrame
from reader import Dataset
#from slicer import Slices
from config import VALID_YEARS


for year in VALID_YEARS:
    # Download, unrar, parse and save clean dataset as local CSV file
    assert 1 == Dataset(year).create_clean_copy(overwrite=True) 
    
for year in VALID_YEARS:
    # Create base_*.csv - all companies with fewer columns
    assert isinstance(Dataset(year).make_df(), DataFrame)        
        
# Save a family of slices from Rosstat dataset as csv/xlsx files:
    #     main_*.csv          - excludes 'micro' enterprises (sales < 120 mln rub), 
    #                           but includes companies with assets above 30(?) mln
    #     bln_*.csv and .xlsx - companies above 1 bln rub in sales
