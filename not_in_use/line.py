# todo:
# - generate list of names
# - add more tests with many " in company name 
# - replace dequote()

line1='aaa"ccc  "bbb"  zzz"'
line2='aaa"bbb"'

QUOTE_CHAR = '"'

def get_final_name(line):
    cnt = line.count(QUOTE_CHAR)
    spt = line.split(QUOTE_CHAR)
    if cnt == 2:
        return(spt[-1-1])         
    elif cnt == 4:
        return(spt[-2-1]) 
    else:
        return line 
        
assert get_final_name(line1) == "bbb"
assert get_final_name(line2) == "bbb"

def dequote(name):
    """Split company *name* to organisation and title."""
    # Warning: will not work well on company names with more than 4 quotechars
    parts = name.split(QUOTE_CHAR)
    org = parts[0].strip()
    cnt = name.count(QUOTE_CHAR)
    if cnt == 2:
        title = parts[1].strip()
    elif cnt > 2:
        title = QUOTE_CHAR.join(parts[1:])
    else:
        title = name
    return org, title.strip()    

def split_name(line):
    cnt = line.count(QUOTE_CHAR)
    parts = line.split(QUOTE_CHAR)
    org = parts[0].strip()
    mid = ''
    if cnt in[2,1]:
        title = parts[1].strip()
    elif cnt in [3,4]:
        mid = parts[1].strip()
        title = parts[2].strip()    
    else: 
        title = line
    return org, mid, title         
    
    
    
if __name__ == "__main__":
    from reader import emit_raw_dicts
    from itertools import islice    
    lines = [x['name'] for x in islice(emit_raw_dicts(2015),2000,2000+1000)]
    for x in lines:
        if x.count(QUOTE_CHAR) > 2:
            print(split_name(x))