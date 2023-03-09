import pytest
import requests


@pytest.fixture()
def response_object(
        status_code=200,
        content=b'{"just": 1, "some": 2, "json": 3}',
        encoding='utf-8'):
    response = requests.Response()
    response.encoding = encoding
    response.status_code = status_code
    response._content = content
    return response


@pytest.fixture()
def request_object():
    return requests.Request()
