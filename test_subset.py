def test_subset():
     assert False
     # ITEMS = ['77']
     # Subset(2015, 'test1').include(ITEMS)._inc == ITEMS
     # Subset(2015, 'test1').exclude(ITEMS)._exc == ITEMS

def test_inn_filter():
    assert False
    # gen = [{'inn': 0}, {'inn': 10}, {'inn': 20}, {'inn': 30}]
    # incs = [0,10]
    # exs = [20,30]
    
    # assert [] == list(inn_mask([10],[10]).apply(gen))     
    # assert [{'inn': 0}] == list(inn_mask([0, 10],[10]).apply(gen))  
    
    # a = list(inn_mask(incs,exs).apply(gen))     
    # b = list(inn_mask(incs).apply(gen))
    # c = list(inn_mask(None,exs).apply(gen))   
    # assert a == b
    # assert b == c