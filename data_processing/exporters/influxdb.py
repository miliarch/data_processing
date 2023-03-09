from data_processing.base import HTTPRESTController, TokenAuth


class InfluxDBAPIv2Exporter(HTTPRESTController):
    API_ROOT = '/api/v2'
    HEADERS = {
        'Accept: application/json'
        'Content-Type': 'application/json'
    }

    def __init__(self, influxdb_url, org, bucket, token):
        self.influxdb_url = influxdb_url
        self.org = org
        self.bucket = bucket
        self.auth = TokenAuth(token)
        base_url = f'{self.influxdb_url}{self.API_ROOT}'
        super().__init__(base_url, self.HEADERS, auth=self.auth)

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
        return self.post(endpoint, params=params, data=line_protocol_data)
