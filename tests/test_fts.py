#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for fts.

'''
from datetime import datetime
from os.path import join
from tempfile import gettempdir

import pytest
from hdx.hdx_configuration import Configuration
from fts import generate_dataset_and_showcase, get_clusters, get_countries


class TestFTS:
    clusters = [{
        'id': 1,
        'name': 'Camp Coordination / Management',
        'code': 'CCM',
        'type': 'global',
        'parentId': None
    }, {
        'id': 12,
        'name': 'Child Protection',
        'code': 'PRO-CPN',
        'type': 'aor',
        'parentId': None
    }]
    
    countries = [{
        'id': 1,
        'iso3': 'AFG',
        'name': 'Afghanistan'
    }, {
        'id': 3,
        'iso3': 'ALB',
        'name': 'Albania'
    }]

    flows = [{'firstReportedDate': '2016-11-09T00:00:00Z', 'contributionType': 'financial',
              'createdAt': '2017-05-15T15:20:52.142Z', 'budgetYear': '2016', 'date': '2017-04-17T00:00:00Z',
              'childFlowIds': None, 'amountUSD': 10250000, 'method': 'Traditional aid', 'exchangeRate': None,
              'updatedAt': '2017-05-15T15:20:52.142Z', 'keywords': ['USA/BPRM'],
              'sourceObjects': [{'type': 'Organization', 'organizationTypes': ['Government', 'National government'],
                                 'id': '2933', 'name': 'United States of America, Government of'},
                                {'type': 'Location', 'id': '237', 'name': 'United States'},
                                {'type': 'UsageYear', 'id': '38', 'name': '2017'}], 'refCode': 'FS # 2 FY 2017',
              'destinationObjects': [{'type': 'Organization', 'organizationTypes': ['Red Cross/Red Crescent'],
                                      'id': '2967', 'name': 'International Committee of the Red Cross'},
                                     {'type': 'Location', 'id': '1', 'name': 'Afghanistan'},
                                     {'type': 'UsageYear', 'id': '38', 'name': '2017'}], 'flowType': 'Standard',
              'reportDetails': [{'date': '2017-04-18T00:00:00.000Z',
                                 'organization': 'United States of America, Government of', 'sourceType': 'Primary',
                                 'reportChannel': 'Email'}], 'parentFlowId': None, 'originalCurrency': 'USD',
              'status': 'commitment', 'id': '149198', 'boundary': 'incoming',
              'description': 'Humanitarian Assistance (STATE/PRM)', 'decisionDate': '2016-06-24T00:00:00Z',
              'versionId': 2}]

    requirements = [{'code': 'CAFG01', 'years': [{'id': 22, 'year': '2001'}], 'revisedRequirements': None,
                     'startDate': '2001-01-01T00:00:00.000Z', 'name': 'Afghanistan 2001',
                     'endDate': '2001-09-30T00:00:00.000Z', 'categories': [{'group': 'planType', 'id': 110,
                                                                            'code': None, 'name': 'CAP'}],
                     'emergencies': [], 'id': 70, 'origRequirements': None,
                     'locations': [{'id': 1, 'adminLevel': 0, 'iso3': 'AFG', 'name': 'Afghanistan'}]},
                    {'code': 'CAFG0102', 'years': [{'id': 23, 'year': '2002'}], 'revisedRequirements': 1780509639,
                     'startDate': '2001-10-01T00:00:00.000Z', 'name': 'Afghanistan 2002 (ITAP for the Afghan People)',
                     'endDate': '2002-12-31T00:00:00.000Z', 'categories': [{'group': 'planType', 'id': 110,
                                                                            'code': None, 'name': 'CAP'}],
                     'emergencies': [], 'id': 92, 'origRequirements': 1763894630,
                     'locations': [{'id': 1, 'adminLevel': 0, 'iso3': 'AFG', 'name': 'Afghanistan'}]}]

    funding_objects = [{'type': 'Plan', 'totalFunding': -81522932, 'id': 70, 'direction': 'destination',
                        'name': 'Afghanistan 2001'},
                       {'type': 'Plan', 'totalFunding': 1196693956, 'id': 92, 'direction': 'destination',
                        'name': 'Afghanistan 2002 (ITAP for the Afghan People)'}]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True,
                             project_config_yaml=join('tests', 'config', 'project_configuration.yml'))

    @pytest.fixture(scope='function')
    def downloader(self):
        class Request:
            def json(self):
                pass

        class Download:
            @staticmethod
            def download(url):
                request = Request()
                if url == 'http://lala/global-cluster':
                    def fn():
                        return {'data': TestFTS.clusters}
                    request.json = fn
                elif url == 'http://lala/location':
                    def fn():
                        return {'data': TestFTS.countries}
                    request.json = fn
                elif url == 'http://lala/fts/flow?countryISO3=AFG&year=2017':
                    def fn():
                        return {'data': {'flows':  TestFTS.flows}}
                    request.json = fn
                elif url == 'http://lala/plan/country/AFG':
                    def fn():
                        return {'data': TestFTS.requirements}
                    request.json = fn
                elif url == 'http://lala/fts/flow?groupby=plan&countryISO3=AFG':
                    def fn():
                        return {'data': {'report3': {'fundingTotals': {'objects': [{'singleFundingObjects': TestFTS.funding_objects}]}}}}
                    request.json = fn
                elif url == 'http://lala/fts/flow?groupby=plan&countryISO3=AFG&filterBy=destinationGlobalClusterId:1':
                    def fn():
                        return {'data': {'report3': {'fundingTotals': {'objects': list()}}}}
                    request.json = fn
                elif url == 'http://lala/fts/flow?groupby=plan&countryISO3=AFG&filterBy=destinationGlobalClusterId:12':
                    def fn():
                        return {'data': {'report3': {'fundingTotals': {
                            'objects': [{'type': 'Plan', 'singleFundingTotal': 1521486,
                                         'singleFundingObjects': [{'type': 'Plan', 'totalFunding': 816189, 'id': 544,
                                                                   'direction': 'destination', 'name':
                                                                       'Afghanistan 2017'},
                                                                  {'type': 'Plan', 'totalFunding': 705297,
                                                                   'direction': 'destination',
                                                                   'name': 'Not specified'}],
                                         'direction': 'destination'}]}}}}
                    request.json = fn
                return request
        return Download()

    def test_get_clusters(self, downloader):
        clusters = get_clusters('http://lala/', downloader)
        assert clusters == TestFTS.clusters

    def test_get_countries(self, downloader):
        countries = get_countries('http://lala/', downloader)
        assert countries == TestFTS.countries

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        folder = gettempdir()
        today = datetime.strptime('01062017', '%d%m%Y').date()
        dataset, showcase = generate_dataset_and_showcase('http://lala/', downloader, folder, TestFTS.clusters, 'AFG', 'Afghanistan', 1, today)
        assert dataset == {'groups': [{'name': 'afg'}], 'name': 'fts-requirements-and-funding-data-for-afghanistan',
                           'title': 'FTS Requirements and Funding Data for Afghanistan',
                           'tags': [{'name': 'cash'}, {'name': 'FTS'}], 'dataset_date': '06/01/2017',
                           'data_update_frequency': '1'}
        resources = dataset.get_resources()
        assert resources == [{'name': 'fts_funding_afg.csv', 'description': 'FTS Funding Data for Afghanistan for 2017', 'format': 'csv'},
                             {'name': 'fts_funding_requirements_afg.csv', 'description': 'FTS Requirements and Funding Data for Afghanistan', 'format': 'csv'},
                             {'name': 'fts_funding_cluster_afg.csv', 'description': 'FTS Funding Data by Cluster for Afghanistan', 'format': 'csv'}]
        assert showcase == {'image_url': 'https://fts.unocha.org/sites/default/files/styles/fts_feature_image/public/navigation_101.jpg',
                            'name': 'fts-requirements-and-funding-data-for-afghanistan-showcase',
                            'notes': 'Click the image on the right to go to the FTS funding summary page for Afghanistan',
                            'url': 'https://fts.unocha.org/countries/1/flows/2017', 'title': 'FTS Afghanistan Summary Page',
                            'tags': [{'name': 'cash'}, {'name': 'FTS'}]}

