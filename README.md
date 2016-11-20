#Machine-readable dataset of 2012-2015 Russian enterprises financial reports

*Current development branch*: [csv-reader](https://github.com/epogrebnyak/data-rosstat-boo-2013/tree/csv_reader)

The code allows to collect corporate data from Rosstat statistics office web site and store full or sliced CSV 
files for further analysis in R/pandas/Eviews.

Final uses
==========
- students can download smaller subsets of Rosstat data as csv/xlsx files (fewer variables, less companies, 3-5Mb to 10-20Mb) 
- a more experienced user can reproduce a clean version of full Rosstat dataset on a local computer (300Mb-1.3Gb)

Source data
===========
- For every year in 2012-2015 we have a file with column names and archived CSV with data. Column names are the same for all 4 years. 
- Each data file 1-2 Gb when unpacked, >250 columns, 1 to 2 mln rows. 
- Source dataset is a bit dirty: 
 -- a small part of rows uses different monetary units (rub and mln run instead of thousand rub). this is main data 
    transformation issue
 -- several rows are corrupted in source files (see "Known bugs" below)
 

Entry points
============
```python
from reader import RawDataset
from slicer import Dataset

RawDataset(year).save_clean_copy() 
# Download, unrar, parse and save clean dataset as local CSV file

Dataset(year).create_files() 
#Saves a family of slices from Rosstat dataset as csv/xlsx files:
# boo_*.csv - all companies, selected variables (~300 Mb)
# main_*.csv - excludes 'micro' enterprises (sales < 120 mln rub), but includes companies with assets above 30(?) mln
# bln_*.csv, bln_*.xlsx - companies above 1 bln rub in sales

# check if os.path.exists()
# raise FilePathError: file not found. Use RosstatCSV

```
Notes:
- Определения микро, малых и средних предприятий по выручке: [(60-400-1000)](https://rg.ru/2015/07/17/vyruchka-site-dok.html), 
[2015 (120-800-2000)](https://rg.ru/2013/02/15/tovar-dok.html), [комментарий](http://glavkniga.ru/situations/k500967).
- Как изменение критерия 'ММСП' в 2015 году повлияло на состав этих групп?
- Определения по численности (в ред. Федерального закона от [23.06.2016 N 222-ФЗ](http://www.consultant.ru/document/cons_doc_LAW_52144/)): (а) от ста одного до двухсот пятидесяти человек для средних предприятий; (б) до ста человек для малых предприятий; среди малых предприятий выделяются микропредприятия - до пятнадцати человек.


Data flow
=========
- check column names, assert they are same for all years. use hardcoded version of column names (<columns.py>)
- download and unpack datafiles (<remote.py>)
- read datafile by row, adjust row, yield it + save stream of adjusted rows to cleaned full dataset csv files 
  ('clean full-length csv, all columns') (<row_parser.py> and <reader.py>)
- read 'clean source csv' files, slice and rename columns and save as csv ('clean full-length csv, fewer columns')
  in <slicer.py>
- cut number of rows (exlude smaller companies) and save as csv/xlsx ('student csv') in <slicer.py>

Development 
===========

Not todo
-------
- save everything in SQL database (sqlite/mysql) using sqlalchemy
- better backend to provide this data to students, AWS S3 maybe (large files now saved to git repo, not nice). 
- review: make csv reader/writer work in chunks (after profiler, tests)
- stricter typing in read_csv(...) using 'dtype'

Considering
-----------
- '--force' option to overwrite files
- make <slicer.py> work in chunks, maybe it is faster rather than downloading all data to memory. use existing code from S3.
- review: https://github.com/epogrebnyak/data-rosstat-boo-2013/pull/3
- use profiler to analyse program <https://pymotw.com/2/profile/>
- zip sliced CSVs on the fly, do not store main CSVs itself  in <slicer.py> https://docs.python.org/3/library/zipfile.html
- provide R/Pandas reader funcs for the files as <dataset.r>, <dataset.py>

Todo
----
- print list of variables
- separate 'rosstat' and 'data' directory +  def get_path(filename, dir_type): in config.py
- job sequencing using RosstatCSV().downloаd().unrar().save_clean_copy() and Dataset(year).create_files() 
- provide data import examples in README.md (see above)
- review: [change assert to Exception](https://github.com/epogrebnyak/data-rosstat-boo-2013/blob/csv_reader/columns.py#L800
- order of [module imports](http://stackoverflow.com/questions/22722976/import-order-coding-standard)
- unit tests including peek 
- requirement.txt to allow replicate code 

Todo2 (new feature)
-------------------
- allow subsetting CSV by list of INN keys (implemented in other repo)


Known issues
============

1\. Key field INN must be 10 digits, but sometimes starts with 0, trying to keep it as string, not int. 
Alternatively, push all to INNs to int. In practice when doing df.merge(on='inn') I loose some matches,
probably due to typing of inns.
 
2\. Reading source csv file:
  - one line with elements exceeding number of columns  
  - one line without INN field 
  - CSV may have last empty row

3\. Full-length datasets are out of memory in pandas on many computers.
