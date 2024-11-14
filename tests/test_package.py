from oer_scraper import __version__, __doc__

def test_version():
    assert __version__ == '0.1.0'

def test_documentation():
    assert __doc__ is not None
