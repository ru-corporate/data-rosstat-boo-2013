import pandas as pd

from folders import ParsedCSV
from row_parser import get_colname_dtypes

file = ParsedCSV(2015).filepath()
chunksize = 100*1000
chunks = pd.read_csv(file, dtype=get_colname_dtypes(), chunksize=chunksize, iterator=True)

def nlargest(df0, n=100):
    max_vals = sorted(df0.ta.tolist(), reverse=True)[:n]
    ix = df0['ta'].isin(max_vals)    
    return df0[ix]

def process_chunks(chunks):
    result = pd.DataFrame()    
    for i, ch in enumerate(chunks):        
        print (i)
        #if i > 1: break
        z = nlargest(ch)
        result = pd.concat([result, z])#, ignore_index=True)
    return nlargest(result, n=1000).sort('ta', ascending=[False]) 

df = process_chunks(chunks)   
df.to_csv("data//subset//largest//largest.csv")
print(df[['title','ta','ta_lag']].head())   
