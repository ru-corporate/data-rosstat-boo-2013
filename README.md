#Machine-readable public dataset of 2012-2015 Russian enterprises financial reports

The code allows to collect corporate data from Rosstat statistics office web site and store full or sliced CSV
files for further analysis in R/pandas/Eviews.

Final uses
==========
- students can download smaller subsets of Rosstat data as csv/xlsx files (fewer variables, less companies, 3-5Mb to 10-20Mb per year)
- a more experienced user can reproduce a clean version of full Rosstat dataset on a local computer (300Mb-1.3Gb per year)

Source data
===========
- For every year in 2012-2015 we have a file with column names and archived CSV with data. Column names are the same for all 4 years.
- Each data file 1-2 Gb when unpacked, >250 columns, 1 to 2 mln rows.
- Source dataset is a bit dirty:
 -- a small part of rows uses different monetary units (rub and mln run instead of thousand rub). this is main data
    transformation issue
 -- several rows are corrupted in source files (see "Known bugs" below)


User files
============
```
Dataset(year).create_files()
Dataset(year).info()
#Saves a family of slices from Rosstat dataset as csv/xlsx files:
# boo_*.csv - all companies, selected variables (~300 Mb)
# main_*.csv - excludes 'micro' enterprises (sales < 120 mln rub), but includes companies with assets above 30(?) mln
# bln_*.csv, bln_*.xlsx - companies above 1 bln rub in sales
```

Usage
=====
Use code below to obtain 2012 dataset. Supported years are 2012-2015
but older files are smaller, try running 2012 or 2013 before 2015.

```python
from remote import RawDataset
from rows import Dataset

year = 2012
RawDatatset(year).download().unpack()
Dataset(year).to_csv()
df = Dataset(year).read_df()
```

Note: you will be operating with large datasets, creating files may take 2-3 mins on a fast computer
and much longer on laptops and older machines. Consider downloading smaller datasets [here], if this code
hangs on your machine.

Download and unrar raw csv
--------------------------
- Download rar file  
- Unpack raw csv from rar file  

```python
from remote import RawDataset
RawDataset(2012).download().unpack()
raw_csv_path = RawDataset(2012).get_filename()
```

Make local csv file  
-------------------
- Purge broken lines from raw csv (company has no INN field, wrong number of columns)
- Transform data:
  - adjust numeric values to '000 rub
  - produce file with fewer columns (controlled by columns.RENAMER)
  - add new text columns (okved levels, title, year, region by inn)
- Keep INN and region codes as strings
- Add headers
- Save as local CSV file

```python  
from rows import Dataset
csv_path = Dataset(2013).to_csv()
```

Read local csv file as pandas dataframe
---------------------------------------
- Read dataframe using ```pd.read_csv``` with dtypes (it loads file faster)

```python  
df = Dataset(2012).read_df()
```

Subset dataset by INN
----------------------
```python  
Dataset(2012).add_inn_filtering().to_csv()
```


"Альбом российской корпоративной отчетности-2015"
=================================================
- as PDF file / as article
- title:
 - "CMF/HSE. Russian corporations 2015: large dataset overview."
 - "CMF/HSE. Альбом российской корпоративной отчетности-2015."
- corresponding address
- data source
  - Rosstat
  - who provides commercial interfaces. list-companies.
- coverage
  - % as of total, % as of macro
  - what we like about reporting
  - what we do not like about reporting
- interesting facts
  - by industry
  - high-growth companies
  - ...
  - in focus: Sports and cinema
- proposed research areas
- classroom topics
- advertising:
  - in HSE
  - Rosstat
  - SPARK, etc
  - facebook/VK

Groups of repos
===============
- parser
- user datasets
- excercises
- album

Development
===========

Current development branch
--------------------------
[stream_dicts](https://github.com/epogrebnyak/data-rosstat-boo-2013/tree/stream_dicts)

- uses funcs over generators, very slim implementation
- no intermediate csv files, just raw and final csv
- based on dicts, it is a bit slower, but columns are on their places

Not todo
-------
- error messages in is_filter
- generate documentation
- save everything in SQL database (sqlite/mysql) using sqlalchemy
- use profiler to analyse program <https://pymotw.com/2/profile/>
- provide R/Pandas reader funcs for the files as <dataset.r>, <dataset.py>
- as package
- chunks
  - review: make csv reader/writer work in chunks (after profiler, tests)
  - make <slicer.py> work in chunks, maybe it is faster rather than downloading all data to memory. use existing code from S3.

Considering
-----------
- better backend to provide this data to students, AWS S3 maybe (large files now saved to git repo, not nice) + AWS library
- zip sliced CSVs on the fly, do not store main CSVs itself  in <slicer.py> https://docs.python.org/3/library/zipfile.html

Todo
----
- reports album
- move notes.md

Slicing:
- test right companies as largest
- slice bln and larger companies
- Dataset(year).info()

Data import with inn:
- test subsetting CSV by list of INN keys
- provide good inn.txt example
- exclide inn.txt from git_ignore

Todo later
----------
- print list of variables / put on sheet
- requirement.txt to allow replicate code
- more unit tests

Done or scrapped
----------------
- review: https://github.com/epogrebnyak/data-rosstat-boo-2013/pull/3
- stricter typing in read_csv(...) using 'dtype'
- '--force' option to overwrite files
- saving clean csv does not have progress bar
- review: [change assert to Exception](https://github.com/epogrebnyak/data-rosstat-boo-2013/blob/csv_reader/columns.py#L800
- job sequencing using RosstatCSV().downloаd().unrar().save_clean_copy() and Dataset(year).create_files()
- order of [module imports](http://stackoverflow.com/questions/22722976/import-order-coding-standard)
- provide data import examples in README.md (see above)
- separate 'rosstat' and 'data' directory + def get_path(filename, dir_type): in config.py
- make ```config.make_dirs()``` work in the background

Known issues
============

1\. Key field INN must be 10 digits, but sometimes starts with 0, trying to keep it as string, not int.
Alternatively, push all to INNs to int. In practice when doing df.merge(on='inn') I loose some matches,
probably due to typing of inns.

2\. Reading source csv file:
  - one line with elements exceeding number of columns  
  - several lines without INN field
  - CSV may have last empty row

3\. Full-length datasets are out of memory in pandas on many computers.

4\. Latest revisions of dataset wrongly mix units, there are fake large companies.
