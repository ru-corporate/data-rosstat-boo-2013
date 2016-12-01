from config import VALID_YEARS
from remote import RawDataset
from reader import Dataset

Dataset(2015).to_csv(True)

#for year in VALID_YEARS:
#    RawDataset(year).download().unrar()
#    Dataset(year).to_csv(True) #to overwrite use .csv(force=True)


# df = dict()
# for year in VALID_YEARS:
#     df[year] = Dataset(year).read_df()
