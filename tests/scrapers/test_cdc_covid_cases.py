import pytest
from data_processing.scrapers import CDCCovidCasesScraper


@pytest.fixture()
def scraper(measurement='measurement_name'):
    scraper = CDCCovidCasesScraper(measurement, update_on_init=False)
    return scraper


@pytest.fixture()
def mock_data():
    """Mock data returned from CDC Covid Data API"""
    data = {}
    data['CSVInfo'] = {
        'filename': 'US_MAP_DATA',
        'update': 'Mar  7 2023  3:08PM',
        'disclaimer': 'Case and Death data updated as of Mar  7 2023  3:08PM.  Testing data updated as of Mar  7 2023  3:11PM',
        'fieldpropertymap': [
            {'abbr': 'abbr'},
            {'fips': 'fips'},
            {'name': 'jurisdiction'},
            {'tot_cases': 'Total Cases'},
            {'tot_death': 'Total Death'},
            {'death_100k': 'Death_100k'},
            {'new_cases07': 'CasesInLast7Days'},
            {'new_deaths07': 'DeathsInLast7Days'},
            {'Seven_day_avg_new_cases_per_100k': 'Seven_day_avg_new_cases_per_100k'},
            {'Seven_day_avg_new_deaths_per_100k': 'Seven_day_avg_new_deaths_per_100k'},
            {'Seven_day_cum_new_cases_per_100k': 'Seven_day_cum_new_cases_per_100k'},
            {'Seven_day_cum_new_deaths_per_100k': 'Seven_day_cum_new_deaths_per_100k'},
            {'incidence': 'RatePer100000'},
            {'us_trend_new_case': 'us_trend_new_case'},
            {'us_trend_new_death': 'us_trend_new_death'}
        ]
    }
    data['US_MAP_DATA'] = [
        {
            'abbr': 'AK',
            'tot_cases': 293766,
            'new_cases07': 451,
            'new_deaths07': 0,
            'Seven_day_cum_new_cases_per_100k': 61.7,
            'Seven_day_cum_new_deaths_per_100k': 0.0,
            'tot_death': 1449,
            'death_100k': 198,
            'incidence': 40157,
            'id': 2,
            'fips': '02',
            'name': 'Alaska',
            'us_trend_maxdate': '2023-03-01',
        },
        {
            'abbr': 'AL',
            'tot_cases': 1642062,
            'new_cases07': 3714,
            'new_deaths07': 69,
            'Seven_day_cum_new_cases_per_100k': 75.7,
            'Seven_day_cum_new_deaths_per_100k': 1.4,
            'tot_death': 21001,
            'death_100k': 428,
            'incidence': 33490,
            'id': 1,
            'fips': '01',
            'name': 'Alabama',
            'us_trend_maxdate': '2023-03-01',
        },
        {
            'abbr': 'AR',
            'tot_cases': 1004753,
            'new_cases07': 1252,
            'new_deaths07': 23,
            'Seven_day_cum_new_cases_per_100k': 41.5,
            'Seven_day_cum_new_deaths_per_100k': 0.8,
            'tot_death': 12980,
            'death_100k': 430,
            'incidence': 33294,
            'id': 5,
            'fips': '05',
            'name': 'Arkansas',
            'us_trend_maxdate': '2023-03-01',
        },
        {
            'abbr': 'USA',
            'tot_cases': 103499382,
            'new_cases07': 226620,
            'new_deaths07': 2290,
            'Seven_day_cum_new_cases_per_100k': 68.3,
            'Seven_day_cum_new_deaths_per_100k': 0.7,
            'tot_death': 1117856,
            'death_100k': 336,
            'incidence': 31175,
            'id': 0,
            'fips': '00',
            'name': 'United States of America',
            'us_trend_maxdate': '2023-03-01',
        }
    ]
    return data


@pytest.fixture
def line_protocol_lines():
    """Mock line protocol data to match mock_data"""
    line_protocol_lines = [
        'measurement_name,abbr=AK,fips=02,jurisdiction=Alaska total_cases=293766,cases_7_days=451,Seven_day_cum_new_cases_per_100k=61.7,total_deaths=1449,death_per_100k=198,rate_per_100k=40157,id=2 1678230480',
        'measurement_name,abbr=AL,fips=01,jurisdiction=Alabama total_cases=1642062,cases_7_days=3714,deaths_7_days=69,Seven_day_cum_new_cases_per_100k=75.7,Seven_day_cum_new_deaths_per_100k=1.4,total_deaths=21001,death_per_100k=428,rate_per_100k=33490,id=1 1678230480',
        'measurement_name,abbr=AR,fips=05,jurisdiction=Arkansas total_cases=1004753,cases_7_days=1252,deaths_7_days=23,Seven_day_cum_new_cases_per_100k=41.5,Seven_day_cum_new_deaths_per_100k=0.8,total_deaths=12980,death_per_100k=430,rate_per_100k=33294,id=5 1678230480',
        'measurement_name,abbr=USA,fips=00,jurisdiction=United\\ States\\ of\\ America total_cases=103499382,cases_7_days=226620,deaths_7_days=2290,Seven_day_cum_new_cases_per_100k=68.3,Seven_day_cum_new_deaths_per_100k=0.7,total_deaths=1117856,death_per_100k=336,rate_per_100k=31175 1678230480',
    ]
    return line_protocol_lines


def test_init(scraper):
    assert scraper
    assert isinstance(scraper, CDCCovidCasesScraper)
    assert not scraper.response
    assert not scraper.data
    assert not scraper.metadata
    assert not scraper.region_data
    assert not scraper.line_protocol_lines
    assert not scraper.line_protocol_data
    assert scraper.URL
    assert scraper.KEY_MAP
    assert scraper.TAG_KEYS
    assert scraper.IGNORED_KEYS
    assert scraper.URL == 'https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=US_MAP_DATA'
    assert scraper.KEY_MAP == {
        'abbr': 'abbr',
        'fips': 'fips',
        'name': 'jurisdiction',
        'tot_cases': 'total_cases',
        'tot_death': 'total_deaths',
        'death_100k': 'death_per_100k',
        'new_cases07': 'cases_7_days',
        'new_deaths07': 'deaths_7_days',
        'incidence': 'rate_per_100k',
        'prob_death': 'probable_deaths',
        'conf_death': 'confirmed_deaths',
        'prob_cases': 'probable_cases',
        'conf_cases': 'confirmed_cases',
        'id': 'id',
        'tot_cases_last_24_hours': 'total_cases_last_24h',
        'tot_death_last_24_hours': 'total_death_last_24h'
    }
    assert scraper.TAG_KEYS == [
        'abbr',
        'fips',
        'name'
    ]
    assert scraper.IGNORED_KEYS == [
        'state_level_community_transmission',
        'us_trend_new_case',
        'us_trend_new_death',
        'us_trend_maxdate'
    ]


def test_update(scraper, mock_data, line_protocol_lines):
    scraper.data = mock_data
    assert scraper.data
    scraper.update()
    assert scraper.metadata
    assert scraper.region_data
    assert scraper.line_protocol_lines == line_protocol_lines
    assert scraper.metadata == mock_data['CSVInfo']
    assert scraper.region_data == mock_data['US_MAP_DATA']
    assert scraper.line_protocol_data == '\n'.join(scraper.line_protocol_lines)


def test_reset(scraper, mock_data):
    scraper.data = mock_data
    scraper.update()
    assert scraper.metadata
    assert scraper.region_data
    assert scraper.line_protocol_lines
    assert scraper.line_protocol_data
    scraper.reset()
    assert not scraper.response
    assert not scraper.data
    assert not scraper.metadata
    assert not scraper.region_data
    assert not scraper.line_protocol_lines
    assert not scraper.line_protocol_data
