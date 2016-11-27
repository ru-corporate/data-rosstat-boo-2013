#
#   config.py
#

from config import _make_path

def test_make_path():
    assert "data\\csv\\123.csv" == _make_path("123.csv", "csv")
