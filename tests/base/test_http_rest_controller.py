import pytest
from unittest.mock import patch
from data_processing.base import HTTPRESTController


@pytest.fixture()
def exporter(
        base_url='http://example.com',
        headers={'Accept': 'application/json'}):
    exporter = HTTPRESTController(base_url, headers)
    return exporter


@pytest.fixture()
def mock_dict():
    return {'just': 'a', 'some': 'b', 'data': 'c'}


@pytest.fixture()
def params():
    return {'bar': 'baz'}


@pytest.fixture()
def endpoint():
    return '/foo'


def test_init(exporter):
    assert exporter.base_url
    assert exporter.headers
    assert not exporter.auth
    assert exporter.session
    assert not exporter.session.auth
    assert exporter.base_url == 'http://example.com'
    assert exporter.headers == {'Accept': 'application/json'}
    assert exporter.session.headers == {'Accept': 'application/json'}
    assert exporter.headers == exporter.session.headers


def test_setup_session(exporter):
    old_session = exporter.session
    exporter.headers = {'Content-Type': 'application/json'}
    exporter.auth = ('username', 'password')
    exporter.setup_session()
    assert exporter.session is not old_session
    assert exporter.session.headers == exporter.headers
    assert exporter.session.auth == exporter.auth


def test_build_url_with_params(exporter, endpoint, params):
    expected_result = 'http://example.com/foo?bar=baz'
    result = exporter.build_url(endpoint, params=params)
    assert result == expected_result


def test_build_url_without_params(exporter, endpoint):
    expected_result = 'http://example.com/foo'
    result = exporter.build_url(endpoint)
    assert result == expected_result


def test_get_with_params(exporter, response_object, endpoint, params):
    with patch.object(exporter, '_get', return_value=response_object) as mock:
        exporter.get(endpoint, params=params)
        mock.assert_called_once_with('http://example.com/foo?bar=baz')


def test_get_without_params(exporter, response_object, endpoint):
    with patch.object(exporter, '_get', return_value=response_object) as mock:
        exporter.get(endpoint)
        mock.assert_called_once_with('http://example.com/foo')


def test_put_with_params(exporter, response_object, endpoint, params):
    with patch.object(exporter, '_put', return_value=response_object) as mock:
        exporter.put(endpoint, params=params)
        mock.assert_called_once_with('http://example.com/foo?bar=baz', data=None)


def test_put_without_params(exporter, response_object, endpoint):
    with patch.object(exporter, '_put', return_value=response_object) as mock:
        exporter.put(endpoint)
        mock.assert_called_once_with('http://example.com/foo', data=None)


def test_put_with_data(exporter, response_object, endpoint, mock_dict):
    with patch.object(exporter, '_put', return_value=response_object) as mock:
        exporter.put(endpoint, data=mock_dict)
        mock.assert_called_once_with('http://example.com/foo', data=mock_dict)


def test_put_with_data_and_params(exporter, response_object, endpoint, params, mock_dict):
    with patch.object(exporter, '_put', return_value=response_object) as mock:
        exporter.put(endpoint, params=params, data=mock_dict)
        mock.assert_called_once_with('http://example.com/foo?bar=baz', data=mock_dict)


def test_post_with_params(exporter, response_object, endpoint, params):
    with patch.object(exporter, '_post', return_value=response_object) as mock:
        exporter.post(endpoint, params=params)
        mock.assert_called_once_with('http://example.com/foo?bar=baz', data=None)


def test_post_without_params(exporter, response_object, endpoint):
    with patch.object(exporter, '_post', return_value=response_object) as mock:
        exporter.post(endpoint)
        mock.assert_called_once_with('http://example.com/foo', data=None)


def test_post_with_data(exporter, response_object, endpoint, mock_dict):
    with patch.object(exporter, '_post', return_value=response_object) as mock:
        exporter.post(endpoint, data=mock_dict)
        mock.assert_called_once_with('http://example.com/foo', data=mock_dict)


def test_post_with_data_and_params(exporter, response_object, endpoint, params, mock_dict):
    with patch.object(exporter, '_post', return_value=response_object) as mock:
        exporter.post(endpoint, params=params, data=mock_dict)
        mock.assert_called_once_with('http://example.com/foo?bar=baz', data=mock_dict)
