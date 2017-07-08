
import os
import pytest

@pytest.fixture('session')
def bigrams_path():
    return os.path.join(os.path.dirname(__file__), 
                        '../../../data', 
                        'bigrams-cyber1.lower.json.gz')
