import pytest
from extract import Extractor

def test_extractor():
    with pytest.raises(ValueError):    
        ext = Extractor(None, [])