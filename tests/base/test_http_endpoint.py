import pytest
from unittest.mock import MagicMock
from data_processing.base import HTTPEndpointScraper


@pytest.fixture()
def scraper(response_object, url='http://example.com/endpoint'):
    scraper = HTTPEndpointScraper(url, scrape_on_init=False)
    scraper._get_url = MagicMock(return_value=response_object)
    return scraper


def test_init(scraper):
    assert not scraper.response
    assert not scraper.data
    assert scraper.url
    assert scraper.url == 'http://example.com/endpoint'


def test_scrape(scraper):
    scraper.scrape()
    assert scraper.response
    assert scraper.data
    assert scraper.data == {'just': 1, 'some': 2, 'json': 3}


def test_reset(scraper):
    scraper.scrape()
    assert scraper.response
    assert scraper.data
    scraper.reset()
    assert not scraper.response
    assert not scraper.data
