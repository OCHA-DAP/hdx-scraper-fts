#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions
that register datasets in HDX.

'''
import sys
import logging
from os.path import join
from tempfile import gettempdir

from hdx.configuration import Configuration
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.downloader import Download

from fts import generate_dataset

logger = logging.getLogger(__name__)


def main():
    '''Generate dataset and create it in HDX'''

    base_url = Configuration.read()['base_url']
    url = '%sglobal-cluster' % base_url
    downloader = Download(auth=('ocha_hdx', 'bfeR432ujm'))
    response = downloader.download(url)
    json = response.json()
    clusters = json['data']
    url = '%slocation' % base_url
    response = downloader.download(url)
    json = response.json()
    folder = gettempdir()
    for country in json['data']:
        countryname = country['name']
        logger.info('Adding FTS data for %s' % countryname)
        dataset = generate_dataset(folder, downloader, clusters, country['iso3'], countryname)
        if dataset is not None:
            dataset.update_from_yaml()
            dataset.create_in_hdx()
            sys.exit(0)

if __name__ == '__main__':
    facade(main, hdx_site='feature', project_config_yaml=join('config', 'project_configuration.yml'))
