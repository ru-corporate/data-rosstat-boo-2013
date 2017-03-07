# Annual financial reports of Russian enterprises (2012-2015) 


The code allows to collect data from Rosstat web site 
and store all or parts of it as local CSV files 
for further analysis in R/pandas/Eviews.


Full dataset weights from 300 Mb (2012) to 1.3 Gb (2015). 
Smaller subsets have fewer variables and less companies 
weight 3-5Mb to 10-20Mb. You can perform analysis based on 
smaller data subset for example in Excel and then explore 
full dataset using R or pandas. 


Source data
===========
- For every year in 2012-2015 we have a file with column names and archived 
  CSV with data. Column names are the same for all 4 years.
- Each data file 1-2 Gb when unpacked, >250 columns, 1 to 2 mln rows.
- Source dataset is a bit dirty:
 -- a small part of rows uses different monetary units (rub and mln run instead of thousand rub). this is main data
    transformation issue
 -- several rows are corrupted in source files (see "Known bugs" below)



Usage
=====
Use code below to obtain 2012 dataset. Supported years are 2012-2015
but older files are smaller, try running 2012 before later years.

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


Make local csv file  
-------------------
- Purge broken lines from raw csv (company has no INN field, wrong number of columns)
- Transform data:
  - adjust numeric values to '000 rub
  - produce file with fewer columns (controlled by columns.RENAMER)
  - add new text columns (okved levels, title, year, region by inn)
- Keep INN and region codes as strings
- Add headers, datacolumns as in Columns().RENAMER
- Save as local CSV file

Read local csv file as pandas dataframe
---------------------------------------
- Read dataframe using ```pd.read_csv``` with dtypes (it loads file faster)


Subsets: parts dataset
----------------------
- Dataframe like ```df=Dataset(year).read_df()``` still very big, a lot of noise and slow to explore  
- Subsets allow creating row slices of dataset, column names stay the same acr 
- use subsets 

```python  
from reader import Subset
Subset(2015, 'test1').to_csv()
```

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