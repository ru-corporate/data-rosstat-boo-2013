"""Run parts of boo package."""
from boo import VALID_YEARS
from boo import Dataset, RawDataset

print(VALID_YEARS)
RawDataset(2015).download()
#Dataset(2015).to_csv


# from remote import RawDataset
# from reader import Dataset

#Dataset(2015).to_csv(True)

#for year in VALID_YEARS:
#    RawDataset(year).download().unrar()
#    Dataset(year).to_csv(True) #to overwrite use .csv(force=True)


# df = dict()
# for year in VALID_YEARS:
#     df[year] = Dataset(year).read_df()