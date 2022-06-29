"""Test join operations"""

from tests.test_operations import A, B

def test_inner_join_projection():
    """Test inner join projection operator"""
    assert A.ᐅᐊ(B, where={'eq':['A.chave','B.chave']}).definition == {
        'name': 'mock.(A⋈B)',
        'columns': {
            'A.chave': 'integer','A.desc': 'text','A.valor': 'float',
            'B.chave': 'integer','B.desc': 'text','B.valor': 'float'}}
    assert [r for r in A.ᐅᐊ(B, where={'eq':['A.chave','B.chave']})] == [(1,'a',0.55,1,'a',0.55)]
    assert B.ᐅᐊ(A, where={'eq':['B.chave','A.chave']}).definition == {
        'name': 'mock.(B⋈A)',
        'columns': {
            'B.chave': 'integer','B.desc': 'text','B.valor': 'float',
            'A.chave': 'integer','A.desc': 'text','A.valor': 'float'}}
    assert [r for r in B.ᐅᐊ(A, where={'eq':['B.chave','A.chave']})] == [(1,'a',0.55,1,'a',0.55)]

def test_cross_join():
    """Test cross join operator"""
    assert (A * B).σ({'eq':['A.chave','B.chave']}).columns == {
        'A.chave': int, 'A.desc': str, 'A.valor': float,
        'B.chave': int, 'B.desc': str, 'B.valor': float}
    assert [r for r in (A * B).σ({'eq':['A.chave','B.chave']})] == [(1,'a',0.55,1,'a',0.55)]
