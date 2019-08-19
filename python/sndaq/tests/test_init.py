import sndaq
  
def test_version_exists():
    assert hasattr(sndaq, '__version__')
