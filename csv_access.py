import csv
import os

DELIM = ";"
CHUNKSIZE = 100*1000

   
def csv_stream(filename, sep=DELIM):
    with open(filename, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=sep) # may need to use encoding="cp1251"
        for row in spamreader:
            yield row
 
def to_csv(path, stream, cols=None, sep=DELIM):    
    with open(path, 'w', encoding = "utf-8") as file:
        writer = csv.writer(file, delimiter=sep, lineterminator="\n", 
                            quoting=csv.QUOTE_MINIMAL)
        if cols:                    
            writer.writerow(cols)
        writer.writerows(stream)
    print("Saved file:", path)    
    return path 

def csv_block(filename, count, skip=0, sep=DELIM):
    k = 0 
    for i, row in enumerate(csv_stream(filename,sep)):
        if i<skip:
            continue
        if k<count: 
            yield row
            k+=1
        else:
            break    
    
def indicate_progress_by_chunk(gen, chunk=CHUNKSIZE, echo=True):
    i=1; k=0
    for r in gen:
        yield r        
        i+=1
        if i==chunk:
            i=0; k+=1
            if echo:
                print(chunk*k)