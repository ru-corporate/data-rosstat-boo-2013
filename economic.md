Program flow
============

Download and unrar
------------------
- Download rar file  
- Unpack rar file into raw csv 

```python 
from remote import RemoteDatatset
RemoteDatatset(2013).download().unpack()
```

Make readable raw csv file  
--------------------------
- Purge 'broken lines' from raw csv (company has no INN field, wrong number of columns)
- Write error log in separate file 
- Write column names row at the start of raw csv file

```python  
RemoteDatatset(2013).clean()
```

Transform data
--------------
- Adjust units in numeric values   
- Produce file with fewer columns
- Add new columns (okveds, title dequote, year, region by inn)
- Must keep INN and region codes as strings and data columns as integers

Make subsets by row  
--------------------
- make subset by INN field  
- make subset by assets / sales
