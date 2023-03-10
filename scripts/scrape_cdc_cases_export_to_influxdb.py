#!/usr/bin/env python3
import sys
import yaml
from data_processing.exporters import InfluxDBAPIv2Exporter
from data_processing.scrapers import CDCCovidCasesScraper

REQUIRED_KEYS = [
    'measurement',
    'influx_url',
    'influx_org',
    'influx_bucket',
    'influx_token'
]

KEY_DESCRIPTIONS = {
    'measurement': 'Measurement name to use in line protocol data generation',
    'influx_url': 'Base URL of the InfluxDB instance (e.g.: "https://localhost:8086")',
    'influx_org': 'Name of InfluxDB organization the target bucket belongs to',
    'influx_bucket': 'Name of bucket to write line protocol data points to',
    'influx_token': 'API token to use for authorization of GET and POST calls',
    'https_verify': 'Boolean value that controls whether TLS certs will be verified (default True)',
}


def verify_required_keys(config):
    for key in REQUIRED_KEYS:
        try:
            config[key]
        except KeyError:
            out_str = f'Config is missing required key: {key} ({KEY_DESCRIPTIONS[key]})'
            print(out_str)
            raise


def main(config_file_path):
    # Handle configuration
    with open(config_file_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
    verify_required_keys(config)
    measurement = config['measurement']
    base_url = config['influx_url']
    org = config['influx_org']
    bucket = config['influx_bucket']
    token = config['influx_token']
    verify = config.get('https_verify', True)

    # Init objects
    scraper = CDCCovidCasesScraper(measurement)
    exporter = InfluxDBAPIv2Exporter(base_url, org, bucket, token, verify=verify)

    # Scrape data from CDC API
    scraper.update()

    # Export data to InfluxDB
    assert exporter.is_authenticated
    exporter.write_to_bucket(scraper.line_protocol_data, precision='s')


if __name__ == '__main__':
    try:
        config_file_path = sys.argv[1]
    except IndexError:
        print('Provide path to config file as first positional argument')
    main(config_file_path)
