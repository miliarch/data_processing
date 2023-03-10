from data_processing.base import HTTPRESTController, TokenAuth


class BucketDoesNotExistError(Exception):
    def __init__(self, bucket):
        super().__init__(f'The specified bucket does not exist: {bucket}')


class BucketAuthenticationError(Exception):
    def __init__(self, bucket):
        super().__init__(f'Error authenticating to bucket: {bucket}')


class InfluxDBAPIv2Exporter(HTTPRESTController):
    API_ROOT = '/api/v2'
    HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }

    def __init__(self, influxdb_url, org, bucket, token, verify=True):
        self.influxdb_url = influxdb_url
        self.org = org
        self.bucket = bucket
        base_url = f'{self.influxdb_url}{self.API_ROOT}'
        super().__init__(
            base_url,
            self.HEADERS,
            auth=TokenAuth(token),
            verify=verify)

    def write_to_bucket(self, line_protocol_data, precision='ms', compression=None):
        """Write line protocol format data to influx bucket at provided
        precision. Future: Specify compression (scaffolded, not implemented).
        https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
        """
        endpoint = '/write'
        params = {
            'bucket': self.bucket,
            'org': self.org,
            'precision': precision,
        }
        if compression:
            # Doesn't exist yet - modify Content-Encoding header
            raise NotImplementedError
        if self.bucket_exists:
            return self.post(endpoint, params=params, data=line_protocol_data)

    @property
    def bucket_exists(self):
        """Check if InfluxDB bucket exists. Returns True if self.bucket
        exists. Raises BucketDoesNotExistError if it does not.
        """
        response = self.get('/buckets', {'name': self.bucket})
        buckets = response.json().get('buckets', None)
        if not buckets:
            raise BucketDoesNotExistError(self.bucket)
        return True

    @property
    def is_authenticated(self):
        """Check if InfluxDB authentication is successful."""
        success = self.get('/buckets').status_code == 200
        if not success:
            raise BucketAuthenticationError(self.bucket)
        return True
