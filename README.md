#Machine-readable dataset of 2012-2015 Russian enterprises financial reports + some teaching assignments

*Current development branch*: [csv-reader](https://github.com/epogrebnyak/data-rosstat-boo-2013/tree/csv_reader)

The code allows to collect and transform corporate data from Rosstat statistics office web site 
and store full or sliced CSV files for further analysis in R/pandas/Eviews.

Source data
===========
- For every year in 2012-2015 we have a file with column names and archived CSV with data. Column names are the same for all 4 years. 
- Each data file 1-2 Gb when unpacked, >250 columns, 1 to 2 mln rows. 
- Source dataset is a bit dirty: 
 -- a small part of rows uses different monetary units (rub and mln run instead of thousand rub). this is main data 
    transformation issue
 -- several rows are corrupted in source files (see "Known bugs" below)

Final uses
==========
- the students download relatively small subsets of data as csv/xlsx files (fewer variables, less companies, 3-5Mb to 10-20Mb) 
- a more experienced user and myself can reproduce a clean version of full Rosstat dataset on local computer

Data flow
=========
- check column names, assert they are same for all years. use hardcoded version of column names (<columns.py>)
- download and unpack datafiles (<remote.py>)
- read datafile by row, adjust row, yield it + save stream of adjusted rows to cleaned full dataset csv files 
  ('clean full-length csv, all columns') (<row_parser.py> and <reader.py>)
- read 'clean source csv' files, slice and rename columns and save as csv ('clean full-length csv, fewer columns')
  in <slicer.py>
- cut number of rows (exlude smaller companies) and save as csv/xlsx ('student csv') in <slicer.py>

Remaining questions / ideas
===========================
- your comments on current implementation
- save everything in SQL database (sqlite/mysql)
- better backend to provide this data to students (large files now saved to git repo, not nice). AWS S3?
- job sequencing as in luigi, but simpler (data does not change often)

Minor todos (in development)
============================
- make <slicer.py> work in chunks, maybe it is faster rather than downloading all data to memory
- requirement.txt to allow replicate code more securely 
- separate 'rosstat' and 'data' directory
- zip sliced CSVs on the fly, do not store CSVs itself (<slicer.py>)
- provide data import examples in README.md 
- allow subsetting CSV by list of INN keys (implemented in other repo)
- stricter typing in read_csv(...) using 'dtype'

Known issues
============

1\. Key field INN must be 10 digits, but sometimes starts with 0, trying to keep it as string, not int. 
Alternatively, push all to INNs to int. In practice when doing df.merge(on='inn') I loose some matches,
probably due to typing of inns.
 
2\. Reading source csv file:
  - one line with elements exceeding number of columns  
  - one line without INN field 
  - CSV may have last empty row

3\. Full-length datasets are out of memory in pandas in on many computers.
