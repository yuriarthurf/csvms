"""Library Unittests"""
import re
import pytest
from csvms import __version__

@pytest.fixture
def toml() -> dict():
    """ Parse toml file to dictionary
    :return: Dictionary with toml structure
    """
    _props = dict()
    _prop = str()
    with open("pyproject.toml", encoding="utf-8") as _file:
        _pyproject = _file.readlines()
    for conf in _pyproject:
        try:
            _prop = next(re.finditer(r"^\[(.+)\]$", conf, re.IGNORECASE)).group(1)
            _props[_prop]=dict()
        except StopIteration:
            try:
                matches = next(re.finditer(r"""(.+) = (\"|\[)(.+)(\"|\])""", conf, re.IGNORECASE))
                _props[_prop].update({matches.group(1).replace('\"',''):matches.group(3).replace('\"','')})
            except StopIteration:
                continue
        except Exception as err:
            raise err
    yield _props    

def test_version(toml):
    """ Check Version """
    assert __version__ == toml['tool.poetry'].get('version')

def test_config(toml):
    """ Check Poetry configuration file """
    assert toml['tool.poetry'].get('name') is not None
    assert toml['tool.poetry'].get('version') is not None
    assert toml['tool.poetry'].get('description') is not None
    assert toml['tool.poetry'].get('authors') is not None
    assert toml['tool.poetry'].get('license') is not None
    assert toml['tool.poetry'].get('readme') is not None

def test_config_license(toml):
    """ Check License """
    assert toml['tool.poetry'].get('license') == "MIT"
