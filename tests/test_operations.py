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
        (3,"c",None)])

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
        (3,"c",None),
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

def test_diff():
    """Test diff operator"""
    assert (A - B).definition == {
        "name": "mock.(A−B)",
        "columns": {
            "chave": "integer",
            "desc": "text",
            "valor": "float"}}
    assert [r for r in (A - B)] == [(3,"c",None)]

    assert (B - A).definition == {
        "name": "mock.(B−A)",
        "columns": {
            "chave": "integer",
            "desc": "text",
            "valor": "float"}}
    assert [r for r in (B - A)] == [(2,"b",1.05)]

def test_product():
    """Test product operator"""
    assert (A * B).definition == {
        "name": "mock.(A×B)",
        "columns": {
            "A.chave": "integer",
            "A.desc": "text",
            "A.valor": "float",
            "B.chave": "integer",
            "B.desc": "text",
            "B.valor": "float"}}
    assert [r for r in (A * B)] == [
        (1, 'a', 0.55, 1, 'a', 0.55),
        (1, 'a', 0.55, 2, 'b', 1.05),
        (3, 'c', None, 1, 'a', 0.55),
        (3, 'c', None, 2, 'b', 1.05)
    ]

def test_selection():
    """Test selection operator"""
    # A.σ({'and':[{"eq":['Manager','George']},{"lt":['DeptId',2]}]})
    assert A.σ({"eq":['chave',1]}).definition == {
        'name': 'mock.(Aσ)',
        'columns': {
            'chave': 'integer',
            'desc': 'text',
            'valor': 'float'}}
    assert [r for r in A.σ({'and':[
            {"eq":['chave',1]},
            {"exists":'valor'}]})] == [(1,"a",0.55)]
    assert [r for r in A.σ({'or':[
            {"eq":['chave',1]},
            {"missing":'valor'}]})] == [(1,"a",0.55),(3,"c",None)]
    assert [r for r in A.σ({"lt":['chave',3]})] == [(1,"a",0.55)]
    assert [r for r in A.σ({"gt":['chave',1]})] == [(3,"c",None)]
    assert [r for r in A.σ({"eq":['chave',1]})] == [(1,"a",0.55)]
    assert [r for r in A.σ({"lte":['chave',3]})] == [(1,"a",0.55),(3,"c",None)]
    assert [r for r in A.σ({"gte":['chave',1]})] == [(1,"a",0.55),(3,"c",None)]
    assert [r for r in A.σ({"neq":['chave',1]})] == [(3,"c",None)]
    assert [r for r in A.σ({"is":['valor',None]})] == [(3,"c",None)]
    assert [r for r in A.σ({"missing":'valor'})] == [(3,"c",None)]
    assert [r for r in A.σ({"exists":'valor'})] == [(1,"a",0.55)]
    assert [r for r in A.σ({"in":['desc','ab']})] == [(1,"a",0.55)]
    assert [r for r in A.σ({"nin":['desc','ab']})] == [(3,"c",None)]

