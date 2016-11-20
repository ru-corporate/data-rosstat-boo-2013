# -*- coding: utf-8 -*-
from remote import RemoteDataset 
from reader import Dataset 


RemoteDataset(2013).download().unrar()

fn = from_inn_folder("inn.txt")
ds = Dataset(2015).add_inn_filter(fn)
gen = list[ds.filter_raw_rows()]

#fn2 = from_inn_folder("inn_rows.txt")
#ds.filter_raw_rows_to_csv(fn2)

# fn3 = from_test_folder("rows.xlsx")
# df = ds.make_df()
# df.to_excel(fn3)