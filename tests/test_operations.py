"""Test Operators"""
from csvms.table import Table

A = Table(
    name="pytest.A",
    columns={
        "chave":int,
        "desc":str,
        "valor":float},
    data=[
        (1,"a",0.55),
        (3,"c",9.99)])

B = Table(
    name="pytest.B",
    columns={
        "chave":int,
        "desc":str,
        "valor":float},
    data=[
        (1,"a",0.55),
        (2,"b",1.05)])

def test_union():
    """Test union operator"""
    assert (A + B).definition == {
        "name": "mock.(A∪B)",
        "columns": {
            "chave": "integer",
            "desc": "text",
            "valor": "float"}}
    assert [r for r in (A + B)] == [
        (1,"a",0.55),
        (3,"c",9.99),
        (2,"b",1.05)
    ]

def test_inserct():
    """Test intersection operator"""
    assert (A % B).definition == {
        "name": "mock.(A∩B)",
        "columns": {
            "chave": "integer",
            "desc": "text",
            "valor": "float"}}
    assert [r for r in (A % B)] == [(1,"a",0.55)]
