from folders import TEST_SUBSET 
from subset import inn_filter, Subset

def test_subset():
    ITEMS = ['77']
    Subset(2015, 'test1').include(ITEMS).inc == ITEMS
    Subset(2015, 'test1').exclude(ITEMS).exc == ITEMS

def test_inn_filter():
    def tf(g1,a1,b1):
        return list(inn_filter(iter(g1), a1, b1))     

    gen = [{'inn': 0}, {'inn': 10}, {'inn': 20}, {'inn': 30}]
    assert [] == tf(gen, [10], [10])     
    assert [{'inn': 0}] == tf(gen, [0, 10], [10])
    
    incs = [0,10]
    exs = [20,30]    
    
    a = tf(gen, incs, exs)
    b = tf(gen, incs, None)
    c = tf(gen, None, exs)   
    assert a == b
    assert b == c