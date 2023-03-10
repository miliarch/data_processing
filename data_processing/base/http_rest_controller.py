from requests import Session
from urllib.parse import urlencode


class HTTPRESTController:
    """Base class for HTTP REST controllers"""

    def __init__(self, base_url, headers, auth=None, verify=True):
        self.base_url = base_url
        self.headers = headers
        self.auth = auth
        self.verify = verify
        self.setup_session()

    def setup_session(self):
        self.session = Session()
        self.session.headers = self.headers
        if self.auth:
            self.session.auth = self.auth
        self.session.verify = self.verify

    def get(self, endpoint, params=None):
        """Make GET request to given endpoint with params on self.url"""
        url = self.build_url(endpoint, params)
        return self._get(url)

    def put(self, endpoint, data=None, params=None):
        """Make PUT request to endpoint on self.url with given data and
        params
        """
        url = self.build_url(endpoint, params)
        return self._put(url, data=data)

    def post(self, endpoint, data=None, params=None):
        """Make POST request to endpoint on self.url with given data
        and params
        """
        url = self.build_url(endpoint, params)
        return self._post(url, data=data)

    def build_url(self, endpoint, params=None):
        url = f'{self.base_url}{endpoint}'
        url += f'?{self.encode_params(params)}' if params else ''
        return url

    @staticmethod
    def encode_params(params):
        return urlencode(params)

    def _get(self, url):
        return self.session.get(url)

    def _put(self, url, data=None):
        if not data:
            return self.session.put(url)
        return self.session.put(url, data=data)

    def _post(self, url, data=None):
        if not data:
            return self.session.post(url)
        return self.session.post(url, data=data)
