import config
from reader import Dataset, to_csv 

if __name__ == "__main__":
    # read inn list
    #fn = config.from_inn_folder("inn.txt")
    #ds = Dataset(2015).add_inn_filter(fn)
    #gen = list(ds.filter_raw_rows())
    
    # todo list csvs not found
    # Dataset(2015).get_inn_list()
    
    # save raw file sliced by inn list 
    fn2 = config.from_inn_folder("inn_rows.txt")
    #to_csv(fn2, gen)
    
    #WARNING: must change encoding in "inn_rows.txt" to ANSI to prevent UnicodeDecodeError    
    
    
    # make dataset instance based on new raw file 
    fn3 = config.from_inn_folder("inn_rows_clean.txt")
    fn4 = config.from_inn_folder("inn_rows_df.txt")
    spec = dict(inc=fn2, 
                out=fn3,
                df=fn4)
    ds2 = Dataset(2015, custom_spec=spec)    
    
    # create final csv of sliced dataset 
    ds2.create_clean_copy(overwrite=True)
        
    # save slice to Excel file 
    fn4 = config.from_inn_folder("projects.xlsx")
    df = ds2.make_df()
    df.to_excel(fn4)