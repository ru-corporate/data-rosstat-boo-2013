from config import VALID_YEARS
from remote import RawDataset
from reader import Dataset, DatasetByINN


for year in VALID_YEARS:
    RawDataset(year).download().unrar()
    Dataset(year).to_csv(True) #to overwrite use .csv(force=True)


# df = dict()
# for year in VALID_YEARS:
#     df[year] = Dataset(year).read_df()

# DatasetByINN(2015).to_csv()
