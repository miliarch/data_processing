import pytest
from unittest.mock import patch
from data_processing.exporters import InfluxDBAPIv2Exporter


@pytest.fixture()
def scraper(
        influxdb_url='http://localhost',
        org='foo_org',
        bucket='bar_bucket',
        token='baz_token'):
    scraper = InfluxDBAPIv2Exporter(influxdb_url, org, bucket, token)
    return scraper


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
        'Accept: application/json'
        'Content-Type': 'application/json'
    }
    assert scraper.influxdb_url == 'http://localhost'
    assert scraper.org == 'foo_org'
    assert scraper.bucket == 'bar_bucket'
    assert scraper.auth(request_object).headers['Authorization'] == 'Token baz_token'


def test_write_to_bucket_default(scraper, line_protocol_data, response_object):
    assert scraper.write_to_bucket
    assert scraper.bucket
    assert scraper.org
    expected_endpoint = '/write'
    expected_params = {
        'bucket': 'bar_bucket',
        'org': 'foo_org',
        'precision': 'ms',
    }
    with patch.object(scraper, 'post', return_value=response_object) as mock:
        scraper.write_to_bucket(line_protocol_data)
        mock.assert_called_once_with(
            expected_endpoint,
            params=expected_params,
            data=line_protocol_data
        )


def test_write_to_bucket_with_precision(scraper, line_protocol_data, response_object):
    assert scraper.write_to_bucket
    assert scraper.bucket
    assert scraper.org
    expected_endpoint = '/write'
    expected_params = {
        'bucket': 'bar_bucket',
        'org': 'foo_org',
        'precision': 's',
    }
    with patch.object(scraper, 'post', return_value=response_object) as mock:
        scraper.write_to_bucket(line_protocol_data, precision='s')
        mock.assert_called_once_with(
            expected_endpoint,
            params=expected_params,
            data=line_protocol_data
        )


def test_write_to_bucket_with_compression(scraper, line_protocol_data):
    assert scraper.write_to_bucket
    assert scraper.bucket
    assert scraper.org
    with pytest.raises(NotImplementedError):
        scraper.write_to_bucket(line_protocol_data, compression=True)


def test_write_to_bucket_with_precision_and_compression(scraper, line_protocol_data):
    assert scraper.write_to_bucket
    assert scraper.bucket
    assert scraper.org
    with pytest.raises(NotImplementedError):
        scraper.write_to_bucket(line_protocol_data, compression=True)
