lines = """Закрытое акционерное общество "ВН-Строй"
Закрытое акционерное общество "Водная компания "Старый источник"
Закрытое акционерное общество "Волгоград-GSM"
Закрытое акционерное общество "Волгоградский металлургический завод "Красный Октябрь" (открыто конкурсное производство)
Закрытое акционерное общество "Волгоградский металлургический комбинат "Красный Октябрь"
""".split("\n")

QUOTE_CHAR = '"'

def dequote(line):
     parts = line.split(QUOTE_CHAR)
     org_type = parts[0].strip()
     new_line = QUOTE_CHAR.join(parts[1:-1])
     if new_line.count(QUOTE_CHAR)==1:
         new_line = new_line + QUOTE_CHAR
     return org_type, new_line    
     

for line in lines:
    print(dequote(line))
         