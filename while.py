_ENDLESS = 2 * 10 ** 6
max_count = _ENDLESS

def truncate(iter, max_count):
    i = 1
    while i <= max_count:
         try:
             yield next(iter)     
             i += 1             
         except IndexError:
              break              
         except StopIteration:
              break           

assert [1,2] == [x for x in truncate(iter([1,2,3]), 2)]                  