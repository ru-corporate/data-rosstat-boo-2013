import config
import csv_access 
from reader import Dataset
from cleaner import extract_inn
from columns import COLNAMES

class CustomDataset(Dataset):

    def __init__(self, year, custom_spec=None):        
        super().__init__(year)
        if custom_spec:
            self.input_csv=custom_spec['inc']
            self.adjusted_csv=custom_spec['out']
            self.sliced_csv=custom_spec['df']
        
    def add_inn_filter(self, inn_csv_filepath):
        self.inn_list = [r[0] for r in csv_access.csv_stream(inn_csv_filepath, sep=",")]
        return self
        
    def filter_raw_rows(self, n=None, skip=0):        
        for r in self.raw_rows(n, skip):
            inn = extract_inn(r)  
            if inn in self.inn_list:
                print("Found inn", inn)
                yield r
            else:
                pass            

    def filter_raw_rows_to_csv(self, filename):
        gen = self.filter_raw_rows()
        csv_access.to_csv(filename, gen, COLNAMES, sep="\t")
        return 1 # success code 
        
    # END -- Read by INN functionality 


if __name__ == "__main__":
    TEST_RAW_CSV = config.from_test_folder("raw_csv_test.csv")    
    spec = dict(inc=TEST_RAW_CSV,
                out=config.from_test_folder("brushed_csv_test.csv"),
                df=config.from_test_folder("df_test.csv")
                )
    ds = Dataset(2015, custom_spec=spec)
    assert 1 == ds.create_local_files()
    ds.make_df()
    ds.peek()
    ds.demo()
    df = ds.read_df()        
    print(df[0:4].transpose())

    # read inn list
    fn = config.from_inn_folder("inn.txt")
    ds = CustomDataset(2015).add_inn_filter(fn)
    gen = list(ds.filter_raw_rows())
    
    ### todo: list csvs not found
    ### Dataset(2015).get_inn_list()
    
    # save raw file sliced by inn list 
    fn2 = config.from_inn_folder("inn_rows.txt")
    csv_access.to_csv(fn2, gen)
    
    #WARNING: must change encoding in "inn_rows.txt" to ANSI or UTF-8 to prevent UnicodeDecodeError        
    
    # make dataset instance based on new raw file 
    fn3 = config.from_inn_folder("inn_rows_clean.txt")
    fn4 = config.from_inn_folder("inn_rows_df.txt")
    spec = dict(inc=fn2, 
                out=fn3,
                df=fn4)
    ds2 = CustomDataset(2015, custom_spec=spec)    
    
    # create final csv of sliced dataset 
    ds2.create_clean_copy(overwrite=True)
        
    # save slice to Excel file 
    fn4 = config.from_inn_folder("projects.xlsx")
    df = ds2.make_df()
    #df.to_excel(fn4)