import pytest
from src.inventory.common_app import validate_rfid_series

@pytest.mark.parametrize("input_str, expected", [
    ("", True),
    ("C52767864-C52768000,C52768001-C52769000", True),
    ("SB1234567-SB1234570,SB1234571-SB1234600", True),
    ("C52767864-C52768000, SB1234567-SB1234570", False),  # mixed prefixes
    ("C52767864-C52768000,C52767900-C52769000", False),  # overlapping ranges
    ("SB1234567-SB1234550", False),                      # start > end
    ("C52767864-C52768000,C52768000-C52769000", False),  # not strictly increasing (start == prev end)
    ("C5276786-C52768000", False),                        # wrong digits count
    ("SB123456-SB1234570", False),
    ("SB1234567-SB1234570", True),# wrong digits count
])
def test_validate_rfid_series(input_str, expected):
    assert validate_rfid_series(input_str) == expected
