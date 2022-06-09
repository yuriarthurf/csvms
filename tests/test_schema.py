"""Catalog test cases"""
from os.path import exists
import pytest
from csvms.schema import Table, FILE_DIR

@pytest.fixture()
def tbl():
    """Table tbl definition"""
    table = Table(
    name="pytest.test",
    columns={
        "chave":int,
        "desc":str,
        "valor":float
    },
    data=[
        (1,"a",0.55),
        (2,"b",1.05),
        (3,"c",9.99),
    ])
    yield table
    if table.save():
        table.drop()

def test_database(tbl):
    """Test database properties"""
    assert str(tbl.database.location).startswith(str(FILE_DIR))
    assert tbl.database.name == "pytest"

def test_table_data(tbl):
    """Test all magic methods"""
    assert len(tbl) == 3
    assert [row for row in tbl] == [(1,"a",0.55),(2,"b",1.05),(3,"c",9.99)]
    assert tbl[0] == {"chave":int(1), "desc":str("a"), "valor": float(0.55)}
    assert tbl[1] == {"chave":int(2), "desc":str("b"), "valor": float(1.05)}
    assert tbl[2] == {"chave":int(3), "desc":str("c"), "valor": float(9.99)}
    assert tbl[3] == {'chave': None, 'desc': None, "valor": None} # Out of index
    return True

def test_imput_output(tbl):
    """ Teste table Imput and Outup """
    assert tbl.location == f"{tbl.database.location}/{tbl.name}.{Table.FORMAT}"
    assert tbl.save()
    assert test_table_data(Table(f"{tbl.database.name}.{tbl.name}", tbl.columns))
    assert tbl.drop()
    assert not exists(tbl.location)
