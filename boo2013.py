from reader import Downloader, to_csv, lines_as_dicts
from slicer import make_merged

Downloader(SOURCE_URL).download().unrar()
to_csv(gen=lines_as_dicts(), filename="all2013.csv")   
to_csv(gen=lines_as_dicts(yield_previous_year=True), filename="all2012.csv")
make_merged()