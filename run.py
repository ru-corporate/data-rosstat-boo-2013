from config import make_dirs, VALID_YEARS
from remote import RawDataset
from rows import Dataset

make_dirs()

df = dict()

for year in VALID_YEARS:
    #RawDataset(year).download().unrar()
    #Dataset(year).to_csv(force=True)
    pass

for year in VALID_YEARS:
    #df[year] = Dataset(year).read_df()
    pass
    
Dataset(2015).use_inn_filtering().to_csv()   
    
    
    