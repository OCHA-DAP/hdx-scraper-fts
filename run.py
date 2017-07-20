#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions
that register datasets in HDX.

'''
import logging
from datetime import datetime
from os.path import join, expanduser
from tempfile import gettempdir

from hdx.hdx_configuration import Configuration
from hdx.facades import logging_kwargs
logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.downloader import Download

from fts import generate_dataset_and_showcase, get_clusters, get_countries

logger = logging.getLogger(__name__)


def main():
    '''Generate dataset and create it in HDX'''

    base_url = Configuration.read()['base_url']
    downloader = Download(basicauthfile=join(expanduser("~"), '.ftskey'))
    clusters = get_clusters(base_url, downloader)
    countries = get_countries(base_url, downloader)
    folder = gettempdir()
    today = datetime.now()
    for country in countries:
        locationid = country['id']
        countryname = country['name']
        if countryname == 'World':
            logger.info('Ignoring  %s' % countryname)
            continue
        logger.info('Adding FTS data for %s' % countryname)
        dataset, showcase = generate_dataset_and_showcase(base_url, folder, downloader, clusters, country['iso3'], countryname, locationid, today)
        if dataset is None:
            logger.info('No data for %s' % countryname)
        else:
            dataset.update_from_yaml()
            dataset.create_in_hdx()
            showcase.create_in_hdx()
            showcase.add_dataset(dataset)

if __name__ == '__main__':
    facade(main, hdx_site='feature', project_config_yaml=join('config', 'project_configuration.yml'))

