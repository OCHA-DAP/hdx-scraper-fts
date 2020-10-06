#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for fts.

'''
import copy
from datetime import datetime
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import DownloadError
from hdx.utilities.path import temp_dir


class TestFTS:
    countries = [{
        'id': 1,
        'iso3': 'AFG',
        'name': 'Afghanistan'
    }, {
        'id': 3,
        'iso3': 'ALB',
        'name': 'Albania'
    }, {
        'id': 41,
        'iso3': 'CPV',
        'name': 'Cape Verde'}
    ]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'alb', 'title': 'Albania'}, {'name': 'cpv', 'title': 'Cape Verde'}, {'name': 'world', 'title': 'World'}])
        Country.countriesdata(False)
        Vocabulary._tags_dict = {'ep-2020-000012-chn': {'Action to Take': 'merge', 'New Tag(s)': 'epidemics and outbreaks;covid-19'}}
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'hxl'}, {'name': 'financial tracking service - fts'}, {'name': 'aid funding'}, {'name': 'epidemics and outbreaks'}, {'name': 'covid-19'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}
        return Configuration.read()

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                return response
        return Download()

    def test_get_countries(self, downloader):
        countries = get_countries('http://afgsite/', downloader)
        assert countries == TestFTS.countries

    def test_get_plans(self, downloader):
        today = datetime.strptime('14042020', '%d%m%Y').date()
        all_plans, plans_by_emergency, plans_by_country = get_plans('http://lala/', downloader, TestFTS.countries, today, start_year=2019)
        assert all_plans == TestFTS.all_plans
        assert plans_by_emergency == TestFTS.plans_by_emergency
        assert plans_by_country == TestFTS.plans_by_country

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        with temp_dir('FTS-TEST') as folder:
