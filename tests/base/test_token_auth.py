import pytest
import requests
from data_processing.base import TokenAuth


@pytest.fixture()
def auth(token='foo', auth_scheme='Token'):
    return TokenAuth(token, auth_scheme)


def test_init(auth):
    assert auth
    assert auth.token
    assert auth.auth_scheme
    assert isinstance(auth, TokenAuth)


def test_call(auth, request_object):
    assert auth(request_object)
    assert isinstance(auth(request_object), requests.Request)
    assert auth(request_object).headers['Authorization']
    assert auth(request_object).headers['Authorization'] == 'Token foo'
