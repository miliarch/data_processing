import pytest
from unittest.mock import patch
from data_processing.exporters import InfluxDBAPIv2Exporter
from data_processing.exporters.influxdb import (
    BucketAuthenticationError,
    BucketDoesNotExistError
)


@pytest.fixture()
def scraper(response_object,
            influxdb_url='http://localhost',
            org='foo_org',
            bucket='bar_bucket',
            token='baz_token',
            verify=True):
    scraper = InfluxDBAPIv2Exporter(influxdb_url, org, bucket, token, verify)
    return scraper


@pytest.fixture()
def scraper_mock(response_object,
                 influxdb_url='http://localhost',
                 org='foo_org',
                 bucket='bar_bucket',
                 token='baz_token',
                 verify=True):
    class ScraperMock(InfluxDBAPIv2Exporter):
        def __init__(self, influxdb_url, org, bucket, token, verify=True):
            super().__init__(influxdb_url, org, bucket, token, verify)

        @property
        def is_authenticated(self):
            return True

        @property
        def bucket_exists(self):
            return True

    scraper_mock = ScraperMock(influxdb_url, org, bucket, token, verify)
    return scraper_mock


@pytest.fixture()
def line_protocol_data():
    line_protocol_data = 'measurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 1678374565.0\n'
    line_protocol_data += 'measurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 1678374565.0'
    return line_protocol_data


def test_init(scraper, request_object):
    assert scraper
    assert isinstance(scraper, InfluxDBAPIv2Exporter)
    assert scraper.API_ROOT
    assert scraper.HEADERS
    assert scraper.influxdb_url
    assert scraper.org
    assert scraper.bucket
    assert scraper.auth
    assert scraper.auth(request_object).headers
    assert scraper.auth(request_object).headers['Authorization']
    assert scraper.API_ROOT == '/api/v2'
    assert scraper.HEADERS == {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }
    assert scraper.influxdb_url == 'http://localhost'
    assert scraper.org == 'foo_org'
    assert scraper.bucket == 'bar_bucket'
    assert scraper.auth(request_object).headers['Authorization'] == 'Token baz_token'


def test_write_to_bucket_default(scraper_mock, line_protocol_data, response_object):
    assert scraper_mock.write_to_bucket
    assert scraper_mock.bucket
    assert scraper_mock.org
    expected_endpoint = '/write'
    expected_params = {
        'bucket': 'bar_bucket',
        'org': 'foo_org',
        'precision': 'ms',
    }
    with patch.object(scraper_mock, 'post', return_value=response_object) as mock:
        scraper_mock.write_to_bucket(line_protocol_data)
        mock.assert_called_once_with(
            expected_endpoint,
            params=expected_params,
            data=line_protocol_data
        )


def test_write_to_bucket_with_precision(scraper_mock, line_protocol_data, response_object):
    assert scraper_mock.write_to_bucket
    assert scraper_mock.bucket
    assert scraper_mock.org
    expected_endpoint = '/write'
    expected_params = {
        'bucket': 'bar_bucket',
        'org': 'foo_org',
        'precision': 's',
    }
    with patch.object(scraper_mock, 'post', return_value=response_object) as mock:
        scraper_mock.write_to_bucket(line_protocol_data, precision='s')
        mock.assert_called_once_with(
            expected_endpoint,
            params=expected_params,
            data=line_protocol_data
        )


def test_write_to_bucket_with_compression(scraper_mock, line_protocol_data):
    assert scraper_mock.write_to_bucket
    assert scraper_mock.bucket
    assert scraper_mock.org
    with pytest.raises(NotImplementedError):
        scraper_mock.write_to_bucket(line_protocol_data, compression=True)


def test_write_to_bucket_with_precision_and_compression(scraper_mock, line_protocol_data):
    assert scraper_mock.write_to_bucket
    assert scraper_mock.bucket
    assert scraper_mock.org
    with pytest.raises(NotImplementedError):
        scraper_mock.write_to_bucket(line_protocol_data, compression=True)


def test_is_authenticated(scraper, response_object):
    response_object.status_code = 200
    with patch.object(scraper, 'get', return_value=response_object):
        assert scraper.is_authenticated


def test_bucket_authentication_error(scraper, response_object):
    response_object.status_code = 401
    with patch.object(scraper, 'get', return_value=response_object):
        with pytest.raises(BucketAuthenticationError):
            scraper.is_authenticated is True


def test_bucket_exists(scraper, response_object):
    response_object.status_code = 200
    response_object._content = b'{\n\t"links": {\n\t\t"self": "/api/v2/buckets?bucket=bar_bucket\\u0026descending=false\\u0026limit=20\\u0026offset=0"\n\t},\n\t"buckets": [\n\t\t{\n\t\t\t"id": "bcd994cde770d58d",\n\t\t\t"orgID": "a9eda493a3e5e370",\n\t\t\t"type": "user",\n\t\t\t"name": "bar_bucket",\n\t\t\t"retentionRules": [\n\t\t\t\t{\n\t\t\t\t\t"type": "expire",\n\t\t\t\t\t"everySeconds": 0,\n\t\t\t\t\t"shardGroupDurationSeconds": 604800\n\t\t\t\t}\n\t\t\t],\n\t\t\t"createdAt": "2023-03-07T05:21:49.927283025Z",\n\t\t\t"updatedAt": "2023-03-07T05:22:11.858931544Z",\n\t\t\t"links": {\n\t\t\t\t"labels": "/api/v2/buckets/bcd994cde770d58d/labels",\n\t\t\t\t"members": "/api/v2/buckets/bcd994cde770d58d/members",\n\t\t\t\t"org": "/api/v2/orgs/a9eda493a3e5e370",\n\t\t\t\t"owners": "/api/v2/buckets/bcd994cde770d58d/owners",\n\t\t\t\t"self": "/api/v2/buckets/bcd994cde770d58d",\n\t\t\t\t"write": "/api/v2/write?org=a9eda493a3e5e370\\u0026bucket=bcd994cde770d58d"\n\t\t\t},\n\t\t\t"labels": []\n\t\t}\n\t]\n}'
    with patch.object(scraper, 'get', return_value=response_object):
        assert scraper.bucket_exists is True


def test_bucket_does_not_exist_error(scraper, response_object):
    response_object.status_code = 200
    response_object._content = b'{\n\t"links": {\n\t\t"self": "/api/v2/buckets?bucket=bar_bucket\\u0026descending=false\\u0026limit=20\\u0026offset=0"\n\t},\n\t"buckets": []\n}'
    with patch.object(scraper, 'get', return_value=response_object):
        with pytest.raises(BucketDoesNotExistError):
            scraper.bucket_exists
