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