from feinstaub import *

def test_leap_year():
    assert is_leap_year(2000) == True
    assert is_leap_year(2004) == True
    assert is_leap_year(2023) == False
    assert is_leap_year(2100) == False