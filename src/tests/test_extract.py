import pytest
from extract import Extractor

def test_empty_fn():
    with pytest.raises(ValueError):    
        Extractor(None, [])

def test_invalid_deps():
    with pytest.raises(ValueError):
        Extractor(lambda x: print(x), {})
    
def test_http_request():
    mock_result = [1, 2, 3, 4, 5]
    def http_request_handler(context):
        return mock_result 
    ext = Extractor(http_request_handler)
    result = ext.execute()
    assert len(result) == mock_result 

def test_db_extraction():
    mock_result = [1, 2, 3, 4, 5]
    
    def db_handler(context):
        return mock_result 
    ext = Extractor(db_handler, [])
    result = ext.execute()
    assert len(result) == mock_result