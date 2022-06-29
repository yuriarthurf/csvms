"""Test Operators"""
from csvms.table import Table
from tomlkit import value

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

def test_projection():
    """Test projection operator"""
    assert A.π([{'value':'chave'}]).definition == {
        'name': 'mock.(Aπ)',
        'columns': {'chave': 'integer'}}
    assert [r for r in A.π([{'value':'chave'}])] == [(1,), (3,)]
    assert A.π([{'value':'chave','name':'key'}]).definition == {
        'name': 'mock.(Aπ)',
        'columns': {'key': 'integer'}}
    assert [r for r in A.π([{'value':'chave','name':'key'}])] == [(1,), (3,)]

def test_extended_projection():
    """Test extended projection operator"""
    assert A.Π(100,'%').definition == {
        'name': 'mock.(AΠ)',
        'columns': {
            'chave': 'integer',
            'desc': 'text',
            'valor': 'float',
            '%': 'integer'}}
    assert [r for r in A.Π(100,'%')] == [(1,"a",0.55,100),(3,"c",None,100)]
    assert [r for r in A.Π({'add':['valor',2]})] == [(1,'a',0.55,2.55),(3,'c',None,None)]
    assert [r for r in A.Π({'sub':['valor',2]})] == [(1,'a',0.55,-1.45),(3,'c',None,None)]
    assert [r for r in A.Π({'div':['valor',2]})] == [(1, 'a', 0.55, 0.275),(3,'c',None,None)]
    assert [r for r in A.Π({'mul':['valor',2]})] == [(1,'a',0.55,1.1),(3,'c',None,None)]

def test_rename_projection():
    """Test rename projection operator"""
    assert A.ρ("C").definition == {
        'name': 'mock.C',
        'columns': {
            'chave': 'integer',
            'desc': 'text',
            'valor': 'float'
        }
    }

def test_join_projection():
    """Test join projection operator"""
    assert A.ᐅᐊ(B, where={'eq':['A.chave','B.chave']}).definition == {
        'name': 'mock.(A⋈B)',
        'columns': {
            'A.chave': 'integer',
            'A.desc': 'text',
            'A.valor': 'float',
            'B.chave': 'integer',
            'B.desc': 'text',
            'B.valor': 'float'}}
    assert [r for r in A.ᐅᐊ(B, where={'eq':['A.chave','B.chave']})] == [(1,'a',0.55,1,'a',0.55)]
    assert B.ᐅᐊ(A, where={'eq':['B.chave','A.chave']}).definition == {
        'name': 'mock.(B⋈A)',
        'columns': {
            'B.chave': 'integer',
            'B.desc': 'text',
            'B.valor': 'float',
            'A.chave': 'integer',
            'A.desc': 'text',
            'A.valor': 'float'}}
    assert [r for r in B.ᐅᐊ(A, where={'eq':['B.chave','A.chave']})] == [(1,'a',0.55,1,'a',0.55)]
