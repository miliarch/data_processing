from datetime import datetime
from data_processing.base import HTTPEndpointScraper


class CDCCovidCasesScraper(HTTPEndpointScraper):
    """Scraper for CDC covid case data"""

    URL = 'https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=US_MAP_DATA'

    KEY_MAP = {
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

    TAG_KEYS = [
        'abbr',
        'fips',
        'name'
    ]

    IGNORED_KEYS = [
        'state_level_community_transmission',
        'us_trend_new_case',
        'us_trend_new_death',
        'us_trend_maxdate',
        'mmwrweek_end',
        'change',
        'change_text',
        'burden',
        'burden_text',
        'data_as_of',
        'data_period_end',
    ]

    def __init__(self, measurement):
        self.measurement = measurement
        super().__init__(self.URL)

    def reset(self):
        """Resets all data attributes to default values"""
        super().reset()
        self.metadata = {}
        self.region_data = {}
        self.updated_at = float()
        self.line_protocol_lines = []

    def update(self):
        """Updates self.metadata, self.region_data, self.updated_at, and
        self.line_protocol_lines attributes.
        """
        if not self.data:
            self.scrape()
        self.metadata = self.data['CSVInfo']
        self.region_data = self.data['US_MAP_DATA']
        self.updated_at = datetime.strptime(
            self.metadata['update'],
            '%b %d %Y %I:%M%p'
        ).timestamp()
        self._parse_region_data_to_line_protocol_lines()

    @property
    def line_protocol_data(self):
        data = ''
        data += '\n'.join(self.line_protocol_lines)
        return data

    def _parse_region_data_to_line_protocol_lines(self):
        if not self.line_protocol_lines:
            for record in self.region_data:
                tag_str = ''
                field_str = ''

                for key in record:
                    if record[key] and key not in self.IGNORED_KEYS:
                        value = record[key]

                        if isinstance(value, str):
                            value = value.replace(' ', '\\ ')

                        if key in self.TAG_KEYS:
                            tag_str += f'{self.KEY_MAP[key]}={value},'
                        else:
                            if key in self.KEY_MAP:
                                field_str += f'{self.KEY_MAP[key]}={value},'
                            else:
                                field_str += f'{key}={value},'

                line_protocol_str = f'{self.measurement},'
                line_protocol_str += f'{tag_str[:-1]} '
                line_protocol_str += f'{field_str[:-1]} '
                line_protocol_str += f'{int(self.updated_at)}'

                self.line_protocol_lines.append(line_protocol_str)
