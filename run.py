from config import make_dirs, VALID_YEARS
from remote import RawDataset
from rows import Dataset

make_dirs()

for year in VALID_YEARS:
     RawDataset(year).download().unrar()
     Dataset(year).to_csv(force=False) 

df = Dataset(2012).read_df()