import pandas as pd
from remote import RemoteDataset
from cleaner import Cleaner
from reader import Dataset
from config import VALID_YEARS


for year in VALID_YEARS:    

    # Download and unrar
    RemoteDataset(year).download().unrar()
    
    # Purge broken lines in raw CSV
    Cleaner(year).run()     
    
    # Parse and save end-use dataset
    Dataset(year).create_clean_copy(overwrite=True) 
    
    # Create base_*.csv - all companies with fewer columns
    df = Dataset(year).make_df()
    assert isinstance(df, DataFrame)        
        
# Save a family of slices from Rosstat dataset as csv/xlsx files:
    #     main_*.csv          - excludes 'micro' enterprises (sales < 120 mln rub), 
    #                           but includes companies with assets above 30(?) mln
    #     bln_*.csv and .xlsx - companies above 1 bln rub in sales
