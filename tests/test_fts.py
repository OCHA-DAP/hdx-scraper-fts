#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for fts.

'''
import filecmp
from datetime import datetime
from os.path import join
from tempfile import gettempdir

import pytest
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations

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

    requirements = [{'locations': [{'iso3': 'AFG', 'id': 1, 'name': 'Afghanistan', 'adminLevel': 0}], 'id': 645,
                     'emergencies': [], 'code': 'HAFG18', 'years': [{'id': 39, 'year': '2018'}],
                     'startDate': '2018-01-01T00:00:00.000Z', 'endDate': '2018-12-31T00:00:00.000Z',
                     'name': 'Afghanistan 2018',
                     'categories': [{'id': 4, 'name': 'Humanitarian response plan', 'group': 'planType', 'code': None}],
                     'revisedRequirements': 437000000},
                    {'locations': [{'iso3': 'AFG', 'id': 1, 'name': 'Afghanistan', 'adminLevel': 0}], 'id': 544,
                     'emergencies': [], 'code': 'HAFG17', 'years': [{'id': 38, 'year': '2017'}],
                     'startDate': '2017-01-01T00:00:00.000Z', 'endDate': '2017-12-31T00:00:00.000Z',
                     'name': 'Afghanistan 2017',
                     'categories': [{'id': 4, 'name': 'Humanitarian response plan', 'group': 'planType', 'code': None}],
                     'revisedRequirements': 409413812}]

    objectsBreakdown = [{'sharedFunding': 0, 'onBoundaryFunding': 0, 'direction': 'destination',
                         'totalFunding': 2500000, 'name': 'Afghanistan 2018', 'overlapFunding': 0, 'id': 645,
                         'singleFunding': 2500000, 'type': 'Plan'},
                        {'sharedFunding': 0, 'onBoundaryFunding': 1489197, 'direction': 'destination',
                         'totalFunding': 318052952, 'name': 'Afghanistan 2017', 'overlapFunding': 0, 'id': 544,
                         'singleFunding': 318052952, 'type': 'Plan'}]

    cluster_objectsBreakdown = [{'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Protection', 'id': 10, 'totalFunding': 12152422, 'singleFunding': 12152422, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Emergency Shelter and NFI', 'id': 4, 'totalFunding': 33904754, 'singleFunding': 33904754, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Food Security', 'id': 6, 'totalFunding': 498462845, 'singleFunding': 498462845, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Education', 'id': 3, 'totalFunding': 65668317, 'singleFunding': 65668317, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Multi-sector', 'id': 26479, 'totalFunding': 317252284, 'singleFunding': 317252284, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Coordination and support services', 'id': 26480, 'totalFunding': 58076793, 'singleFunding': 58076793, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Health', 'id': 7, 'totalFunding': 76050805, 'singleFunding': 76050805, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Agriculture', 'id': 26512, 'totalFunding': 61347440, 'singleFunding': 61347440, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Mine Action', 'id': 15, 'totalFunding': 27771389, 'singleFunding': 27771389, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Water Sanitation Hygiene', 'id': 11, 'totalFunding': 11933249, 'singleFunding': 11933249, 'type': 'GlobalCluster', 'direction': 'destination'}, {'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0, 'name': 'Early Recovery', 'id': 2, 'totalFunding': 34073658, 'singleFunding': 34073658, 'type': 'GlobalCluster', 'direction': 'destination'}]

    cluster_objects = [{'revisedRequirements': 85670595, 'origRequirements': 85670595, 'name': 'Agriculture', 'id': 26512, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 64933280, 'origRequirements': 56933280, 'name': 'Coordination and support services', 'id': 26480, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 171088509, 'origRequirements': 171088509, 'name': 'Early Recovery', 'id': 2, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 96995379, 'origRequirements': 96995379, 'name': 'Education', 'id': 3, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 82751013, 'origRequirements': 82751013, 'name': 'Emergency Shelter and NFI', 'id': 4, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 591933076, 'origRequirements': 591933076, 'name': 'Food Security', 'id': 6, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 188971264, 'origRequirements': 188971264, 'name': 'Health', 'id': 7, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 49455000, 'origRequirements': 49455000, 'name': 'Mine Action', 'id': 15, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 381778683, 'origRequirements': 373163674, 'name': 'Multi-sector', 'id': 26479, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 25810653, 'origRequirements': 25810653, 'name': 'Protection', 'id': 10, 'objectType': 'GlobalCluster'}, {'revisedRequirements': 41122187, 'origRequirements': 41122187, 'name': 'Water Sanitation Hygiene', 'id': 11, 'objectType': 'GlobalCluster'}]


    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}])

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
                if url == 'http://lala/global-cluster':
                    def fn():
                        return {'data': TestFTS.clusters}
                    response.json = fn
                elif url == 'http://lala/location':
                    def fn():
                        return {'data': TestFTS.countries}
                    response.json = fn
                elif url == 'http://lala/fts/flow?countryISO3=AFG&year=2017':
                    def fn():
                        return {'data': {'flows':  TestFTS.flows}}
                    response.json = fn
                elif url == 'http://lala/plan/country/AFG':
                    def fn():
                        return {'data': TestFTS.requirements}
                    response.json = fn
                elif url == 'http://lala/fts/flow?groupby=plan&countryISO3=AFG':
                    def fn():
                        return {'data': {'report3': {'fundingTotals': {'objects': [{'objectsBreakdown': TestFTS.objectsBreakdown}]}}}}
                    response.json = fn
                elif 'http://lala/fts/flow?planid=' in url:
                    def fn():
                        return {'data': {'report3': {'fundingTotals': {'objects': [{'totalBreakdown': {'sharedFunding': 0},
                                                                                    'objectsBreakdown': TestFTS.cluster_objectsBreakdown}]}},
                                         'requirements': {'objects': TestFTS.cluster_objects}}}
                    response.json = fn
                return response
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
                           'title': 'Afghanistan - Requirements and Funding Data',
                           'tags': [{'name': 'HXL'}, {'name': 'cash'}, {'name': 'FTS'}], 'dataset_date': '06/01/2017',
                           'data_update_frequency': '1', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                           'owner_org': 'fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e'}
        resources = dataset.get_resources()
        assert resources == [{'name': 'fts_funding_afg.csv', 'description': 'FTS Funding Data for Afghanistan for 2017', 'format': 'csv'},
                             {'name': 'fts_requirements_funding_afg.csv', 'description': 'FTS Requirements and Funding Data for Afghanistan', 'format': 'csv'},
                             {'name': 'fts_requirements_funding_cluster_afg.csv', 'description': 'FTS Requirements and Funding Data by Cluster for Afghanistan', 'format': 'csv'}]
        for resource in resources:
            resource_name = resource['name']
            expected_file = join('tests', 'fixtures', resource_name)
            actual_file = join(folder, resource_name)
            assert filecmp.cmp(expected_file, actual_file)

        assert showcase == {'image_url': 'https://fts.unocha.org/sites/default/files/styles/fts_feature_image/public/navigation_101.jpg',
                            'name': 'fts-requirements-and-funding-data-for-afghanistan-showcase',
                            'notes': 'Click the image on the right to go to the FTS funding summary page for Afghanistan',
                            'url': 'https://fts.unocha.org/countries/1/flows/2017', 'title': 'FTS Afghanistan Summary Page',
                            'tags': [{'name': 'HXL'}, {'name': 'cash'}, {'name': 'FTS'}]}

