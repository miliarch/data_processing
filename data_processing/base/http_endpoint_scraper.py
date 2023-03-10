from requests import get


class HTTPEndpointScraper:
    """Base class for HTTP endpoint scrapers"""

    def __init__(self, url, data_format='json'):
        self.url = url
        self.headers = {'Accept': f'application/{data_format}'}
        self.reset()

    def scrape(self):
        """Generic scrape method. Calls reset method if self.response or
        self.data eval as True, makes HTTP get to self.url and stores as
        self.response, and sets value of self.data based on accept header
        data type (only supports json currently)
        """
        if self.response or self.data:
            self.reset()
        self.response = self._get_url()
        if 'json' in self.headers['Accept']:
            self.data = self.response.json()

    def update(self):
        """Method child classes should implement to handle their specific
        data manipulation needs
        """
        raise NotImplementedError

    def reset(self):
        """Resets all data attributes to default None/null values"""
        self.response = None
        self.data = {}

    def _get_url(self):
        return get(self.url, headers=self.headers)
