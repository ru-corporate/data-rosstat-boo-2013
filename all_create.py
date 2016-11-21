import pandas as pd
from remote import RemoteDataset
from cleaner import Cleaner
from reader import Dataset
from config import VALID_YEARS

dfs = dict()


for year in VALID_YEARS:    
    pass

    # Download and unrar
    RemoteDataset(year).download().unrar()
    
    # Purge broken lines in raw CSV
    Cleaner(year).run()     
    
    # Adjust rows and slice by columns
    # Dataset(year).rebuild_local_files() 
    # equals to 
    Dataset(year).make_adjusted_csv()
    Dataset(year).make_df_dump()
    
    # read dataframe by year
    # dfs[year] = Dataset(year).read_df() 

        
# TODO:        
# Save a family of slices from Rosstat dataset as csv/xlsx files:
    #     main_*.csv          - excludes 'micro' enterprises (sales < 120 mln rub), 
    #                           but includes companies with assets above 30(?) mln
    #     bln_*.csv and .xlsx - companies above 1 bln rub in sales
