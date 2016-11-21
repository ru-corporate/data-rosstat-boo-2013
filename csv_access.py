import csv

DELIM = ";"
CHUNKSIZE = 100*1000


def csv_stream(filename, enc='utf-8', sep=DELIM):
   if enc not in ['utf-8', 'windows-1251']:
       raise ValueError("Wrong encoding: " + str(enc))
   print("CSV encoding:", enc) 
   with open(filename, 'r', encoding=enc) as csvfile:
      spamreader = csv.reader(csvfile, delimiter=sep)   
      for row in spamreader:
         yield row 

def csv_stream(filename, sep=DELIM):
   try:
       enc='utf-8'  
       print("CSV encoding:", enc) 
       with open(filename, 'r', encoding=enc) as csvfile:
          spamreader = csv.reader(csvfile, delimiter=sep)   
          for row in spamreader:
             yield row      
   except UnicodeDecodeError:
       enc='windows-1251'  
       print("CSV encoding:", enc) 
       with open(filename, 'r', encoding=enc) as csvfile:
          spamreader = csv.reader(csvfile, delimiter=sep)   
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