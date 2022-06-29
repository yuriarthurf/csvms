"""Test join operations"""
import warnings
from tests.test_operations import A, B

def test_inner_join_projection():
    """Test inner join projection operator"""
    # A join B
    assert A.ᐅᐊ(B, where={'eq':['A.chave','B.chave']}).definition == {
        'name': 'mock.(A⋈B)',
        'columns': {
            'A.chave': 'integer','A.desc': 'text','A.valor': 'float',
            'B.chave': 'integer','B.desc': 'text','B.valor': 'float'}}
    assert [r for r in A.ᐅᐊ(B, where={'eq':['A.chave','B.chave']})] == [(1,'a',0.55,1,'a',0.55)]
    # B join A
    assert B.ᐅᐊ(A, where={'eq':['B.chave','A.chave']}).definition == {
        'name': 'mock.(B⋈A)',
        'columns': {
            'B.chave': 'integer','B.desc': 'text','B.valor': 'float',
            'A.chave': 'integer','A.desc': 'text','A.valor': 'float'}}
    assert [r for r in B.ᐅᐊ(A, where={'eq':['B.chave','A.chave']})] == [(1,'a',0.55,1,'a',0.55)]

def test_cross_join():
    """Test cross join operator"""
    assert [r for r in (A * B).σ({'eq':['A.chave','B.chave']})] == [(1,'a',0.55,1,'a',0.55)]

def test_left_outer_join():
    """Test left outer join operator"""
    try:
        assert [r for r in A.ᗌᐊ(B, where={'eq':['A.chave','B.chave']})] == [
            (1, 'a', 0.55, 1, 'a', 0.55),
            (3, 'c', None, None, None, None)]
    except AttributeError:
        warnings.warn(UserWarning('Not implemented'))

def test_right_outer_join():
    """Test right outer join operator"""
    try:
        assert [r for r in A.ᐅᗏ(B, where={'eq':['A.chave','B.chave']})] == [
            (1, 'a', 0.55, 1, 'a', 0.55),
            (None, None, None, 2, 'b', 1.05)]
    except AttributeError:
        warnings.warn(UserWarning('Not implemented'))

def test_full_outer_join():
    """Test full outer join operator"""
    try:
        assert [r for r in A.ᗌᗏ(B, where={'eq':['A.chave','B.chave']})] == [
            (1, 'a', 0.55, 1, 'a', 0.55),
            (3, 'c', None, None, None, None),
            (None, None, None, 2, 'b', 1.05)]
    except AttributeError:
        warnings.warn(UserWarning('Not implemented'))
