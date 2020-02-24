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

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir, progress_storing_tempdir

from fts import generate_dataset_and_showcase, get_countries

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-fts'


def main():
    '''Generate dataset and create it in HDX'''

    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup=lookup, rate_limit={'calls': 1, 'period': 1}) as downloader:
        base_url = Configuration.read()['base_url']
        countries = get_countries(base_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(countries))
        today = datetime.now()
        for folder, country in progress_storing_tempdir('FTS', countries, 'iso3'):
            dataset, showcase, hxl_resource = generate_dataset_and_showcase(base_url, downloader, folder, country, today)
            if dataset is not None:
                dataset.update_from_yaml()
                if hxl_resource is None:
                    dataset.preview_off()
                else:
                    dataset.set_quickchart_resource(hxl_resource)
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: FTS')
                resources = dataset.get_resources()
                resource_ids = [x['id'] for x in sorted(resources, key=lambda x: len(x['name']), reverse=True)]
                dataset.reorder_resources(resource_ids, hxl_update=False)
                if hxl_resource:
                    dataset.generate_resource_view()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))

