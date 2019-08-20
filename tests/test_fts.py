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

from fts import generate_dataset_and_showcase, get_countries


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

    afgflows = [{'firstReportedDate': '2016-11-09T00:00:00Z', 'contributionType': 'financial',
              'createdAt': '2017-05-15T15:20:52.142Z', 'budgetYear': '2016', 'date': '2017-04-17T00:00:00Z',
              'childFlowIds': None, 'amountUSD': 10250000, 'method': 'Traditional aid', 'exchangeRate': None,
              'updatedAt': '2017-05-15T15:20:52.142Z', 'keywords': ['USA/BPRM'],
              'sourceObjects': [{'type': 'Organization',  'id': '2933', 'name': 'United States of America, Government of',
                                 'organizationTypes': ['Government', 'National government']},
                                {'type': 'Location', 'id': '237', 'name': 'United States'}, {'type': 'Location', 'id': '237', 'name': 'Bermuda'},
                                {'type': 'UsageYear', 'id': '38', 'name': '2017'}], 'refCode': 'FS # 2 FY 2017',
                 'destinationObjects': [{'type': 'Organization', 'id': '2967', 'name': 'International Committee of the Red Cross',
                                     'organizationTypes': ['Red Cross/Red Crescent']},
                                     {'type': 'Plan', 'id': '544', 'name': 'Afghanistan 2017', 'behavior': 'single'},
                                     {'type': 'GlobalCluster', 'id': '4', 'name': 'Emergency Shelter and NFI', 'behavior': 'single'},
                                     {'type': 'GlobalCluster', 'id': '11', 'name': 'Water Sanitation Hygiene', 'behavior': 'single'},
                                     {'type': 'Location', 'id': '1', 'name': 'Afghanistan'},
                                     {'type': 'Project', 'id': '54621', 'name': 'Cholera and other water borne diseases Shield WASH Project', 'behavior': 'single', 'code': 'HTI-18/WS/125105/124'},
                                     {'type': 'UsageYear', 'id': '38', 'name': '2018'}, {'type': 'UsageYear', 'id': '38', 'name': '2017'}], 'flowType': 'Standard',
                 'reportDetails': [{'date': '2017-04-18T00:00:00.000Z',
                                 'organization': 'United States of America, Government of', 'sourceType': 'Primary',
                                 'reportChannel': 'Email'}], 'parentFlowId': None, 'originalCurrency': 'USD',
                 'status': 'commitment', 'id': '149198', 'boundary': 'incoming',
                 'description': 'Humanitarian Assistance (STATE/PRM)', 'decisionDate': '2016-06-24T00:00:00Z',
                 'versionId': 2}]

    cpvflows = [{'id': '170870', 'versionId': 1, 'description': 'The project will help restore, protect livelihoods and increase the resilience of households affected by drought in Cabo Verde.',
                 'status': 'paid', 'date': '2018-02-16T00:00:00Z', 'amountUSD': 218918, 'exchangeRate': None, 'firstReportedDate': '2018-02-16T00:00:00Z', 'budgetYear': None,
                 'decisionDate': '2018-02-16T00:00:00Z', 'flowType': 'Standard', 'contributionType': 'financial', 'keywords': None, 'method': 'Traditional aid', 'parentFlowId': None, 'childFlowIds': None,
                 'newMoney': True, 'createdAt': '2018-02-20T13:09:45.158Z', 'updatedAt': '2018-02-20T13:09:45.158Z',
                 'sourceObjects': [{'type': 'Organization', 'id': '2927', 'name': 'Belgium, Government of', 'behavior': 'single', 'organizationTypes': ['Government'], 'organizationSubTypes': ['National government']},
                                   {'type': 'Location', 'id': '22', 'name': 'Belgium', 'behavior': 'single'},
                                   {'type': 'UsageYear', 'id': '39', 'name': '2018', 'behavior': 'single'}],
                 'destinationObjects': [{'type': 'Organization', 'id': '4399', 'name': 'Food & Agriculture Organization of the United Nations',
                                         'behavior': 'single', 'organizationTypes': ['UN agency']}, {'type': 'GlobalCluster', 'id': '26512', 'name': 'Agriculture', 'behavior': 'single'},
                                        {'type': 'Location', 'id': '41', 'name': 'Cape Verde', 'behavior': 'single'}, {'type': 'UsageYear', 'id': '39', 'name': '2018', 'behavior': 'single'}],
                 'boundary': 'incoming', 'onBoundary': 'single', 'reportDetails': [{'sourceType': 'Primary', 'organization': 'Food & Agriculture Organization of the United Nations',
                                                                                    'reportChannel': 'FTS Web', 'date': '2018-02-16T00:00:00.000Z'}], 'refCode': 'OSRO/CVI/802/BEL'}]

    albflows = [{'id': '174423', 'versionId': 1, 'description': 'Provision of 1,000 food parcels to the most needed people in Albania via Muslim Community of Albania',
                 'status': 'paid', 'date': '2018-02-15T00:00:00Z', 'amountUSD': 86754, 'originalAmount': 325328, 'originalCurrency': 'SAR', 'exchangeRate': 3.75, 'firstReportedDate': '2018-05-02T00:00:00Z',
                 'budgetYear': None, 'decisionDate': None, 'flowType': 'Standard', 'contributionType': 'financial', 'keywords': None, 'method': 'Traditional aid', 'parentFlowId': None, 'childFlowIds': None,
                 'newMoney': True, 'createdAt': '2018-05-14T09:55:20.374Z', 'updatedAt': '2018-05-14T09:55:20.374Z',
                 'sourceObjects': [{'type': 'Organization', 'id': '2998', 'name': 'Saudi Arabia (Kingdom of), Government of', 'behavior': 'single', 'organizationTypes': ['Government'], 'organizationSubTypes': ['National government']},
                                   {'type': 'Location', 'id': '196', 'name': 'Saudi Arabia', 'behavior': 'single'},
                                   {'type': 'UsageYear', 'id': '39', 'name': '2018', 'behavior': 'single'}],
                 'destinationObjects': [{'type': 'Cluster', 'id': '4206', 'name': 'Food security', 'behavior': 'single'},
                                        {'type': 'GlobalCluster', 'id': '6', 'name': 'Food Security', 'behavior': 'single'},
                                        {'type': 'Location', 'id': '3', 'name': 'Albania', 'behavior': 'single'},
                                        {'type': 'UsageYear', 'id': '39', 'name': '2018', 'behavior': 'single'}],
                 'boundary': 'incoming', 'onBoundary': 'single', 'reportDetails': [{'sourceType': 'Primary', 'organization': 'Saudi Arabia (Kingdom of), Government of',
                                                                                    'reportChannel': 'Email', 'date': '2018-05-02T00:00:00.000Z'}], 'refCode': '291'}]

    afgrequirements = [{'locations': [{'iso3': 'AFG', 'id': 1, 'name': 'Afghanistan', 'adminLevel': 0}], 'id': 645,
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

    cpvrequirements = [{'id': 222, 'name': 'West Africa 2007', 'code': 'CXWAF07', 'startDate': '2007-01-01T00:00:00.000Z',
                         'endDate': '2007-12-31T00:00:00.000Z', 'currentReportingPeriodId': None, 'isForHPCProjects': False,
                         'locations': [{'id': 41, 'iso3': 'CPV', 'name': 'Cape Verde', 'adminLevel': 0}], 'emergencies': [], 'years': [{'id': 28, 'year': '2007'}],
                         'categories': [{'id': 110, 'name': 'CAP', 'group': 'planType', 'code': None}], 'origRequirements': 309081675, 'revisedRequirements': 361026890}]

    afgobjectsBreakdownByYear = [{'type': 'UsageYear', 'direction': 'destination', 'id': '38', 'name': '2017',
                                  'totalFunding': 378906464, 'singleFunding': 378906464, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 1489197},
                                 {'type': 'UsageYear', 'direction': 'destination', 'id': '39', 'name': '2018',
                                  'totalFunding': 536757775, 'singleFunding': 536757775, 'overlapFunding': 0, 'sharedFunding': 40, 'onBoundaryFunding': 0}]

    cpvobjectsBreakdownByYear = [{'type': 'UsageYear', 'direction': 'destination', 'id': '28', 'name': '2007',
                                  'totalFunding': 1270424, 'singleFunding': 1270424, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0},
                                 {'type': 'UsageYear', 'direction': 'destination', 'id': '39', 'name': '2018',
                                  'totalFunding': 568918, 'singleFunding': 568918, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}]

    albobjectsBreakdownByYear = [{'type': 'UsageYear', 'direction': 'destination', 'id': '23', 'name': '2002',
                                  'totalFunding': 3384231, 'singleFunding': 3384231, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0},
                                 {'type': 'UsageYear', 'direction': 'destination', 'id': '39', 'name': '2018',
                                  'totalFunding': 86754, 'singleFunding': 86754, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}]

    afgobjectsBreakdownByPlan = [{'sharedFunding': 0, 'onBoundaryFunding': 0, 'direction': 'destination',
                         'totalFunding': 2500000, 'name': 'Afghanistan 2018', 'overlapFunding': 0, 'id': '645',
                         'singleFunding': 2500000, 'type': 'Plan'},
                                 {'sharedFunding': 0, 'onBoundaryFunding': 1489197, 'direction': 'destination',
                         'totalFunding': 318052952, 'name': 'Afghanistan 2017', 'overlapFunding': 0, 'id': '544',
                         'singleFunding': 318052952, 'type': 'Plan'},
                                 {'sharedFunding': '', 'onBoundaryFunding': 3391, 'direction': '',
                        'totalFunding': 3121940000, 'name': 'Not specified', 'overlapFunding': '', 'id': '',
                        'singleFunding': '', 'type': 'Plan'}]

    cpvobjectsBreakdownByPlan = [{'type': 'Plan', 'direction': 'destination', 'name': 'Not specified', 'totalFunding': 16922064,
                            'singleFunding': 16922064, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}]

    albobjectsBreakdownByPlan = [{'type': 'Plan', 'direction': 'destination', 'name': 'Not specified', 'totalFunding': 20903498,
                            'singleFunding': 20903498, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0},
                                 {'type': 'Plan', 'direction': 'destination', 'id': '90', 'name': 'Southeastern Europe 2002',
                            'totalFunding': 470201, 'singleFunding': 470201, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}]

    objectsBreakdownByPlannoid = [{'sharedFunding': '', 'onBoundaryFunding': 3391, 'direction': '',
                             'totalFunding': 3121940000, 'name': 'Not specified', 'overlapFunding': '',
                             'singleFunding': '', 'type': 'Plan'}]

    afg645cluster_objectsBreakdown = [{'type': 'Cluster', 'direction': 'destination', 'id': '4090', 'name': 'Emergency Shelter and NFI', 'totalFunding': 27033198, 'singleFunding': 27033198, 'overlapFunding': 0, 'sharedFunding': 6973218, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '4095', 'name': 'Protection', 'totalFunding': 29198746, 'singleFunding': 29198746, 'overlapFunding': 0, 'sharedFunding': 10190280, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '4092', 'name': 'Health', 'totalFunding': 17898154, 'singleFunding': 17898154, 'overlapFunding': 0, 'sharedFunding': 3565144, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '4094', 'name': 'Nutrition', 'totalFunding': 34404449, 'singleFunding': 34404449, 'overlapFunding': 0, 'sharedFunding': 18712976, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '4096', 'name': 'Water, Sanitation and Hygiene', 'totalFunding': 24569034, 'singleFunding': 24569034, 'overlapFunding': 0, 'sharedFunding': 25268810, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'name': 'Not specified', 'totalFunding': 65949904, 'singleFunding': 65949904, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '4091', 'name': 'Food Security and Agriculture', 'totalFunding': 186752362, 'singleFunding': 186752362, 'overlapFunding': 0, 'sharedFunding': 22809134, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '4089', 'name': 'Coordination', 'totalFunding': 14915683, 'singleFunding': 14915683, 'overlapFunding': 0, 'sharedFunding': 3623188, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '4097', 'name': 'Education in Emergencies WG', 'totalFunding': 5013588, 'singleFunding': 5013588, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '4093', 'name': 'Multi Purpose Cash', 'totalFunding': 4080769, 'singleFunding': 4080769, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '4088', 'name': 'Aviation', 'totalFunding': 12225926, 'singleFunding': 12225926, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}]

    afg544cluster_objectsBreakdown = [{'type': 'Cluster', 'direction': 'destination', 'id': '212', 'name': 'Protection', 'totalFunding': 29481076, 'singleFunding': 29481076, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '213', 'name': 'Water, Sanitation and Hygiene', 'totalFunding': 9776526, 'singleFunding': 9776526, 'overlapFunding': 0, 'sharedFunding': 8850159, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '206', 'name': 'Coordination', 'totalFunding': 13785104, 'singleFunding': 13785104, 'overlapFunding': 0, 'sharedFunding': 5000159, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '211', 'name': 'Nutrition', 'totalFunding': 27474573, 'singleFunding': 27474573, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '209', 'name': 'Health', 'totalFunding': 24425281, 'singleFunding': 24425281, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '207', 'name': 'Emergency Shelter and NFI', 'totalFunding': 18661212, 'singleFunding': 18661212, 'overlapFunding': 0, 'sharedFunding': 8332854, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '208', 'name': 'Food Security and Agriculture', 'totalFunding': 83368135, 'singleFunding': 83368135, 'overlapFunding': 0, 'sharedFunding': 4482854, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '3964', 'name': 'Education in Emergencies WG', 'totalFunding': 1321353, 'singleFunding': 1321353, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'name': 'Not specified', 'totalFunding': 69032991, 'singleFunding': 69032991, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '210', 'name': 'Multi Purpose Cash', 'totalFunding': 25938725, 'singleFunding': 25938725, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '205', 'name': 'Aviation', 'totalFunding': 12742023, 'singleFunding': 12742023, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Cluster', 'direction': 'destination', 'id': '3965', 'name': 'Refugee Chapter', 'totalFunding': 3388177, 'singleFunding': 3388177, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}]

    afg645cluster_objects = [{'id': 4088, 'name': 'Aviation', 'objectType': 'Cluster', 'revisedRequirements': 16390000}, {'id': 4097, 'name': 'Education in Emergencies WG', 'objectType': 'Cluster', 'revisedRequirements': 29685000}, {'id': 4090, 'name': 'Emergency Shelter and NFI', 'objectType': 'Cluster', 'revisedRequirements': 52975538}, {'id': 4096, 'name': 'Water, Sanitation and Hygiene', 'objectType': 'Cluster', 'revisedRequirements': 39000782}, {'id': 4095, 'name': 'Protection', 'objectType': 'Cluster', 'revisedRequirements': 66505803}, {'id': 4094, 'name': 'Nutrition', 'objectType': 'Cluster', 'revisedRequirements': 62370635}, {'id': 4089, 'name': 'Coordination', 'objectType': 'Cluster', 'revisedRequirements': 15900000}, {'id': 4092, 'name': 'Health', 'objectType': 'Cluster', 'revisedRequirements': 48696240}, {'id': 4091, 'name': 'Food Security and Agriculture', 'objectType': 'Cluster', 'revisedRequirements': 231000000}, {'id': 4093, 'name': 'Multi Purpose Cash', 'objectType': 'Cluster', 'revisedRequirements': 36400000}]

    afg544cluster_objects = [{'id': 3964, 'name': 'Education in Emergencies WG', 'objectType': 'Cluster', 'revisedRequirements': 40000000}, {'id': 212, 'name': 'Protection', 'objectType': 'Cluster', 'revisedRequirements': 54413760}, {'id': 213, 'name': 'Water, Sanitation and Hygiene', 'objectType': 'Cluster', 'revisedRequirements': 25000000}, {'id': 206, 'name': 'Coordination', 'objectType': 'Cluster', 'revisedRequirements': 9500000}, {'id': 208, 'name': 'Food Security and Agriculture', 'objectType': 'Cluster', 'revisedRequirements': 65590000}, {'id': 211, 'name': 'Nutrition', 'objectType': 'Cluster', 'revisedRequirements': 48000000}, {'id': 209, 'name': 'Health', 'objectType': 'Cluster', 'revisedRequirements': 30000000}, {'id': 205, 'name': 'Aviation', 'objectType': 'Cluster', 'revisedRequirements': 17000000}, {'id': 3965, 'name': 'Refugee Chapter', 'objectType': 'Cluster', 'revisedRequirements': 19535052}, {'id': 210, 'name': 'Multi Purpose Cash', 'objectType': 'Cluster', 'revisedRequirements': 64775000}, {'id': 207, 'name': 'Emergency Shelter and NFI', 'objectType': 'Cluster', 'revisedRequirements': 35600000}]

    afg645location_objectsBreakdown = [{'type': 'Location', 'direction': 'destination', 'id': '1', 'name': 'Afghanistan', 'totalFunding': 468106102, 'singleFunding': 468106102, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}]

    afg544location_objectsBreakdown = [{'type': 'Location', 'direction': 'destination', 'id': '1', 'name': 'Afghanistan', 'totalFunding': 331238992, 'singleFunding': 331238992, 'overlapFunding': 0, 'sharedFunding': 1489197, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '25799581', 'onBoundaryFunding': 0, 'name': 'Western', 'totalFunding': 0, 'singleFunding': 0, 'overlapFunding': 0, 'sharedFunding': 592979}, {'type': 'Location', 'direction': 'destination', 'id': '25799580', 'onBoundaryFunding': 0, 'name': 'Southern', 'totalFunding': 0, 'singleFunding': 0, 'overlapFunding': 0, 'sharedFunding': 592979}, {'type': 'Location', 'direction': 'destination', 'id': '25799579', 'onBoundaryFunding': 0, 'name': 'South Eastern', 'totalFunding': 0, 'singleFunding': 0, 'overlapFunding': 0, 'sharedFunding': 592979}, {'type': 'Location', 'direction': 'destination', 'id': '25799578', 'onBoundaryFunding': 0, 'name': 'Northern', 'totalFunding': 0, 'singleFunding': 0, 'overlapFunding': 0, 'sharedFunding': 592979}, {'type': 'Location', 'direction': 'destination', 'id': '25799574', 'onBoundaryFunding': 0, 'name': 'Central Highland', 'totalFunding': 0, 'singleFunding': 0, 'overlapFunding': 0, 'sharedFunding': 592979}, {'type': 'Location', 'direction': 'destination', 'id': '25799575', 'onBoundaryFunding': 0, 'name': 'Capital', 'totalFunding': 0, 'singleFunding': 0, 'overlapFunding': 0, 'sharedFunding': 592979}, {'type': 'Location', 'direction': 'destination', 'id': '25799576', 'onBoundaryFunding': 0, 'name': 'Eastern', 'totalFunding': 0, 'singleFunding': 0, 'overlapFunding': 0, 'sharedFunding': 592979}, {'type': 'Location', 'direction': 'destination', 'id': '25799577', 'onBoundaryFunding': 0, 'name': 'North Eastern', 'totalFunding': 0, 'singleFunding': 0, 'overlapFunding': 0, 'sharedFunding': 592979}, {'type': 'Location', 'direction': 'destination', 'id': '4060', 'onBoundaryFunding': 0, 'name': 'Capital', 'totalFunding': 0, 'singleFunding': 0, 'overlapFunding': 0, 'sharedFunding': 896218}]

    cpvlocation_objectsBreakdown = [{'type': 'Location', 'direction': 'destination', 'id': '93', 'name': 'Guinea', 'totalFunding': 19613744, 'singleFunding': 19613744, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'name': 'Not specified', 'totalFunding': 69727905, 'singleFunding': 69727905, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '94', 'name': 'Guinea-Bissau', 'totalFunding': 7934188, 'singleFunding': 7934188, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '197', 'name': 'Senegal', 'totalFunding': 5385626, 'singleFunding': 5385626, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '81', 'name': 'Gambia', 'totalFunding': 85231, 'singleFunding': 85231, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '137', 'name': 'Mali', 'totalFunding': 9155718, 'singleFunding': 9155718, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '36', 'name': 'Burkina Faso', 'totalFunding': 10753570, 'singleFunding': 10753570, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '162', 'name': 'Niger', 'totalFunding': 20855001, 'singleFunding': 20855001, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '126', 'name': 'Liberia', 'totalFunding': 23638958, 'singleFunding': 23638958, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '141', 'name': 'Mauritania', 'totalFunding': 17446352, 'singleFunding': 17446352, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '224', 'name': 'Togo', 'totalFunding': 7893281, 'singleFunding': 7893281, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '55', 'name': "Côte d'Ivoire", 'totalFunding': 3818321, 'singleFunding': 3818321, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '200', 'name': 'Sierra Leone', 'totalFunding': 6614790, 'singleFunding': 6614790, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '24', 'name': 'Benin', 'totalFunding': 1606150, 'singleFunding': 1606150, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '84', 'name': 'Ghana', 'totalFunding': 952767, 'singleFunding': 952767, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}]

    alblocation_objectsBreakdown = [{'type': 'Location', 'direction': 'destination', 'name': 'Not specified', 'totalFunding': 107894770, 'singleFunding': 107894770, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '132', 'name': 'Macedonia, The Former Yugoslav Republic of', 'totalFunding': 19506354, 'singleFunding': 19506354, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '25724802', 'name': 'Serbia and Montenegro (until 2006-2009)', 'totalFunding': 3664205, 'singleFunding': 3664205, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '3', 'name': 'Albania', 'totalFunding': 470201, 'singleFunding': 470201, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}, {'type': 'Location', 'direction': 'destination', 'id': '28', 'name': 'Bosnia and Herzegovina', 'totalFunding': 3130464, 'singleFunding': 3130464, 'overlapFunding': 0, 'sharedFunding': 0, 'onBoundaryFunding': 0}]

    afg645location_objects = [{'id': 1, 'name': 'Afghanistan', 'objectType': 'Location', 'revisedRequirements': 598923998}]

    afg544location_objects = [{'id': 25799579, 'name': 'South Eastern', 'objectType': 'Location', 'revisedRequirements': 409413812}, {'id': 25799577, 'name': 'North Eastern', 'objectType': 'Location', 'revisedRequirements': 409413812}, {'id': 25799578, 'name': 'Northern', 'objectType': 'Location', 'revisedRequirements': 409413812}, {'id': 25799581, 'name': 'Western', 'objectType': 'Location', 'revisedRequirements': 409413812}, {'id': 25799574, 'name': 'Central Highland', 'objectType': 'Location', 'revisedRequirements': 409413812}, {'id': 1, 'name': 'Afghanistan', 'objectType': 'Location', 'revisedRequirements': 409413812}, {'id': 25799580, 'name': 'Southern', 'objectType': 'Location', 'revisedRequirements': 409413812}, {'id': 25799576, 'name': 'Eastern', 'objectType': 'Location', 'revisedRequirements': 409413812}, {'id': 25799575, 'name': 'Capital', 'objectType': 'Location', 'revisedRequirements': 409413812}]

    cpvlocation_objects = [{'id': 24, 'name': 'Benin', 'objectType': 'Location', 'revisedRequirements': 1606150, 'origRequirements': 687585}, {'id': 36, 'name': 'Burkina Faso', 'objectType': 'Location', 'revisedRequirements': 21627280, 'origRequirements': 11094513}, {'id': 41, 'name': 'Cape Verde', 'objectType': 'Location', 'revisedRequirements': 565000, 'origRequirements': 0}, {'id': 55, 'name': "Côte d'Ivoire", 'objectType': 'Location', 'revisedRequirements': 1017000, 'origRequirements': 0}, {'id': 84, 'name': 'Ghana', 'objectType': 'Location', 'revisedRequirements': 874182, 'origRequirements': 0}, {'id': 93, 'name': 'Guinea', 'objectType': 'Location', 'revisedRequirements': 21356802, 'origRequirements': 2508846}, {'id': 94, 'name': 'Guinea-Bissau', 'objectType': 'Location', 'revisedRequirements': 9625090, 'origRequirements': 6210154}, {'id': 126, 'name': 'Liberia', 'objectType': 'Location', 'revisedRequirements': 15223613, 'origRequirements': 700000}, {'id': 137, 'name': 'Mali', 'objectType': 'Location', 'revisedRequirements': 19755994, 'origRequirements': 16906128}, {'id': 141, 'name': 'Mauritania', 'objectType': 'Location', 'revisedRequirements': 23049215, 'origRequirements': 11672826}, {'id': 162, 'name': 'Niger', 'objectType': 'Location', 'revisedRequirements': 51782808, 'origRequirements': 44497516}, {'id': 197, 'name': 'Senegal', 'objectType': 'Location', 'revisedRequirements': 13346146, 'origRequirements': 10988935}, {'id': 200, 'name': 'Sierra Leone', 'objectType': 'Location', 'revisedRequirements': 7579317, 'origRequirements': 0}, {'id': 224, 'name': 'Togo', 'objectType': 'Location', 'revisedRequirements': 3923960, 'origRequirements': 0}, {'name': 'Not specified', 'objectType': 'Location', 'revisedRequirements': 169694333, 'origRequirements': 203815172}]

    alblocation_objects = [{'name': 'Not specified', 'objectType': 'Location', 'revisedRequirements': 212686531, 'origRequirements': 236654801}]

    plan645 = {'id': 645, 'planVersion': {'planId': 645, 'name': 'Afghanistan 2018', 'code': 'HAFG18', 'startDate': '2018-01-01T00:00:00.000Z', 'endDate': '2018-12-31T00:00:00.000Z', 'isForHPCProjects': False}, 'emergencies': [], 'years': [{'id': 39, 'year': '2018'}], 'locations': [{'id': 1, 'name': 'Afghanistan', 'iso3': 'AFG', 'adminlevel': 0}], 'categories': [{'id': 4, 'name': 'Humanitarian response plan', 'group': 'planType', 'code': None}], 'revisedRequirements': 598923998, 'meta': {'language': 'en'}}

    plan544 = {'id': 544, 'planVersion': {'planId': 544, 'name': 'Afghanistan 2017', 'code': 'HAFG17', 'startDate': '2017-01-01T00:00:00.000Z', 'endDate': '2017-12-31T00:00:00.000Z', 'isForHPCProjects': False}, 'emergencies': [], 'years': [{'id': 38, 'year': '2017'}], 'locations': [{'id': 1, 'name': 'Afghanistan', 'iso3': 'AFG', 'adminlevel': 0}, {'id': 25799575, 'name': 'Capital', 'iso3': None, 'adminlevel': 1}, {'id': 25799574, 'name': 'Central Highland', 'iso3': None, 'adminlevel': 1}], 'categories': [{'id': 4, 'name': 'Humanitarian response plan', 'group': 'planType', 'code': None}], 'revisedRequirements': 409413812, 'meta': {'language': 'en'}}

    plan90 = {'id': 90, 'planVersion': {'planId': 90, 'name': 'Southeastern Europe 2002', 'code': 'CXSEUR02', 'startDate': '2002-01-01T00:00:00.000Z', 'endDate': '2002-12-31T00:00:00.000Z', 'isForHPCProjects': False}, 'emergencies': [{'id': 10, 'name': 'Southeastern Europe 2000 - 2002'}], 'years': [{'id': 23, 'year': '2002'}], 'locations': [], 'categories': [{'id': 110, 'name': 'CAP', 'group': 'planType', 'code': None}], 'origRequirements': 236654801, 'revisedRequirements': 212686531, 'meta': {'language': 'en'}}

    plan222 = {'id': 222, 'planVersion': {'planId': 222, 'name': 'West Africa 2007', 'code': 'CXWAF07', 'startDate': '2007-01-01T00:00:00.000Z', 'endDate': '2007-12-31T00:00:00.000Z', 'isForHPCProjects': False}, 'emergencies': [], 'years': [{'id': 28, 'year': '2007'}], 'locations': [{'id': 55, 'iso3': 'CIV', 'name': "Côte d'Ivoire", 'adminLevel': 0}, {'id': 126, 'iso3': 'LBR', 'name': 'Liberia', 'adminLevel': 0}, {'id': 137, 'iso3': 'MLI', 'name': 'Mali', 'adminLevel': 0}, {'id': 93, 'iso3': 'GIN', 'name': 'Guinea', 'adminLevel': 0}, {'id': 84, 'iso3': 'GHA', 'name': 'Ghana', 'adminLevel': 0}, {'id': 162, 'iso3': 'NER', 'name': 'Niger', 'adminLevel': 0}, {'id': 197, 'iso3': 'SEN', 'name': 'Senegal', 'adminLevel': 0}, {'id': 224, 'iso3': 'TGO', 'name': 'Togo', 'adminLevel': 0}, {'id': 36, 'iso3': 'BFA', 'name': 'Burkina Faso', 'adminLevel': 0}, {'id': 141, 'iso3': 'MRT', 'name': 'Mauritania', 'adminLevel': 0}, {'id': 200, 'iso3': 'SLE', 'name': 'Sierra Leone', 'adminLevel': 0}, {'id': 24, 'iso3': 'BEN', 'name': 'Benin', 'adminLevel': 0}, {'id': 41, 'iso3': 'CPV', 'name': 'Cape Verde', 'adminLevel': 0}, {'id': 94, 'iso3': 'GNB', 'name': 'Guinea-Bissau', 'adminLevel': 0}], 'categories': [{'id': 110, 'name': 'CAP', 'group': 'planType', 'code': None}], 'origRequirements': 309081675, 'revisedRequirements': 361026890, 'meta': {'language': 'en'}}

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'alb', 'title': 'Albania'}, {'name': 'cpv', 'title': 'Cape Verde'}])
        Country.countriesdata(False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'hxl'}, {'name': 'financial tracking service - fts'}, {'name': 'aid funding'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

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
                if 'groupby' not in url and 'location' in url:
                    def fn():
                        return {'data': TestFTS.countries}
                    response.json = fn
                elif 'groupby' not in url and 'fts/flow?countryISO3=AFG&year=2017' in url:
                    def fn():
                        meta = dict()
                        if 'nofund' in url:
                            data = []
                            incoming = 0
                        else:
                            data = copy.deepcopy(TestFTS.afgflows)
                            incoming = 391639926
                            if 'page' in url:
                                data[0]['id'] = '149198X'
                            else:
                                meta['nextLink'] = '%s&page=2' % url
                        return {'data': {'incoming': {'fundingTotal': incoming}, 'flows': data}, 'meta': meta}
                    response.json = fn
                elif 'groupby' not in url and 'fts/flow?countryISO3=AFG&year=2018' in url:
                    def fn():
                        if 'nofund' in url:
                            data = []
                            incoming = 0
                        else:
                            data = [1]  # value is not used in test, just needs to be non empty list
                            incoming = 537301773
                        return {'data': {'incoming': {'fundingTotal': incoming}, 'flows': data}, 'meta': dict()}

                    response.json = fn
                elif 'groupby' not in url and 'fts/flow?countryISO3=CPV&year=2007' in url:
                    def fn():
                        return {'data': {'incoming': {'fundingTotal': 1270424}, 'flows': [1]}, 'meta': dict()}
                    response.json = fn
                elif 'groupby' not in url and 'fts/flow?countryISO3=CPV&year=2018' in url:
                    def fn():
                        return {'data': {'incoming': {'fundingTotal': 568918}, 'flows': TestFTS.cpvflows}, 'meta': dict()}
                    response.json = fn
                elif 'groupby' not in url and 'fts/flow?countryISO3=ALB&year=2002' in url:
                    def fn():
                        return {'data': {'incoming': {'fundingTotal': 3384231}, 'flows': [1]}, 'meta': dict()}
                    response.json = fn
                elif 'groupby' not in url and 'fts/flow?countryISO3=ALB&year=2018' in url:
                    def fn():
                        return {'data': {'incoming': {'fundingTotal': 86754}, 'flows': TestFTS.albflows}, 'meta': dict()}
                    response.json = fn
                elif 'plan/country/AFG' in url:
                    def fn():
                        if 'noreq' in url:
                            data = []
                        else:
                            data = TestFTS.afgrequirements
                        return {'data': data}
                    response.json = fn
                elif 'plan/country/CPV' in url:
                    def fn():
                        return {'data': TestFTS.cpvrequirements}
                    response.json = fn
                elif 'plan/country/ALB' in url:
                    def fn():
                        return {'data': []}
                    response.json = fn
                elif 'fts/flow?countryISO3=AFG&groupby=year' in url:
                    def fn():
                        return {'data': {'report3': {
                                'fundingTotals': {'objects': [{'objectsBreakdown': TestFTS.afgobjectsBreakdownByYear}]}}}}
                    response.json = fn
                elif 'fts/flow?countryISO3=CPV&groupby=year' in url:
                    def fn():
                        return {'data': {'report3': {
                                'fundingTotals': {'objects': [{'objectsBreakdown': TestFTS.cpvobjectsBreakdownByYear}]}}}}
                    response.json = fn
                elif 'fts/flow?countryISO3=ALB&groupby=year' in url:
                    def fn():
                        return {'data': {'report3': {
                                'fundingTotals': {'objects': [{'objectsBreakdown': TestFTS.albobjectsBreakdownByYear}]}}}}
                    response.json = fn
                elif 'fts/flow?groupby=plan&countryISO3=AFG' in url:
                    def fn():
                        if 'fundnoid' in url:
                            data = TestFTS.objectsBreakdownByPlannoid
                        elif 'nofund' in url:
                            data = None
                        else:
                            data = TestFTS.afgobjectsBreakdownByPlan
                        return {'data': {'report3': {'fundingTotals': {'objects': [{'objectsBreakdown': data}]}}}}
                    response.json = fn
                elif 'fts/flow?groupby=plan&countryISO3=CPV' in url:
                    def fn():
                        return {'data': {'report3': {
                                'fundingTotals': {'objects': [{'objectsBreakdown': TestFTS.cpvobjectsBreakdownByPlan}]}}}}
                    response.json = fn
                elif 'fts/flow?groupby=plan&countryISO3=ALB' in url:
                    def fn():
                        return {'data': {'report3': {
                            'fundingTotals': {'objects': [{'objectsBreakdown': TestFTS.albobjectsBreakdownByPlan}]}}}}
                    response.json = fn
                elif 'fts/flow?planid=' in url:
                    def fn():
                        if 'groupby=cluster' in url:
                            fundtotaldata = {'sharedFunding': 0}
                            if 'dlerr' in url:
                                raise DownloadError()
                            if 'nofund' in url:
                                funddata = []
                            else:
                                if '645' in url:
                                    funddata = TestFTS.afg645cluster_objectsBreakdown
                                else:
                                    funddata = TestFTS.afg544cluster_objectsBreakdown
                            if 'noreq' in url:
                                reqdata = {}
                            else:
                                if '645' in url:
                                    reqdata = TestFTS.afg645cluster_objects
                                else:
                                    reqdata = TestFTS.afg544cluster_objects
                        else:  # groupby=location
                            fundtotaldata = None
                            if 'cpvsite' in url:
                                funddata = TestFTS.cpvlocation_objectsBreakdown
                                reqdata = TestFTS.cpvlocation_objects
                            elif 'albsite' in url:
                                funddata = TestFTS.alblocation_objectsBreakdown
                                reqdata = TestFTS.alblocation_objects
                            else:  # afgsite
                                if 'nofund' in url:
                                    funddata = []
                                else:
                                    if '645' in url:
                                        funddata = TestFTS.afg645location_objectsBreakdown
                                    else:  # 544
                                        funddata = TestFTS.afg544location_objectsBreakdown
                                if 'noreq' in url:
                                    reqdata = {}
                                else:
                                    if '645' in url:
                                        reqdata = TestFTS.afg645location_objects
                                    else:  # 544
                                        reqdata = TestFTS.afg544location_objects

                        return {'data': {'report3': {'fundingTotals': {'objects': [{'totalBreakdown': fundtotaldata,
                                                                                    'objectsBreakdown': funddata}]}},
                                         'requirements': {'objects': reqdata}}}

                    response.json = fn
                elif 'plan/id/' in url:
                    if '645' in url:
                        plandata = TestFTS.plan645
                    elif '544' in url:
                        plandata = TestFTS.plan544
                    elif '90' in url:
                        plandata = TestFTS.plan90
                    elif '222' in url:
                        plandata = TestFTS.plan222
                    else:
                        plandata = None

                    def fn():
                        return {'data': plandata}
                    response.json = fn
                return response
        return Download()

    def test_get_countries(self, downloader):
        countries = get_countries('http://afgsite/', downloader)
        assert countries == TestFTS.countries

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        with temp_dir('fts') as folder:
            today = datetime.strptime('01062018', '%d%m%Y').date()
            dataset, showcase, hxl_resource = generate_dataset_and_showcase('http://abcsite/', downloader, folder, None, 'None', 1, today)
            assert dataset is None
            assert showcase is None
            assert hxl_resource is None

            dataset, showcase, hxl_resource = generate_dataset_and_showcase('http://abcsite/', downloader, folder, 'ABC', 'ABC', 1, today)
            assert dataset is None
            assert showcase is None
            assert hxl_resource is None

    def test_generate_afg_dataset_and_showcase(self, configuration, downloader):
        afgdataset = {'groups': [{'name': 'afg'}], 'name': 'fts-requirements-and-funding-data-for-afghanistan',
                      'title': 'Afghanistan - Requirements and Funding Data',
                      'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                               {'name': 'financial tracking service - fts', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                               {'name': 'aid funding', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                      'dataset_date': '06/01/2017',
                      'data_update_frequency': '1', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                      'owner_org': 'fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e', 'subnational': '0'}
        afgresources = [
            {'name': 'fts_incoming_funding_afg.csv', 'description': 'FTS Incoming Funding Data for Afghanistan for 2017',
             'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
            {'name': 'fts_requirements_funding_afg.csv',
             'description': 'FTS Annual Requirements and Funding Data for Afghanistan', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
            {'name': 'fts_requirements_funding_cluster_afg.csv',
             'description': 'FTS Annual Requirements and Funding Data by Cluster for Afghanistan', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]
        afgshowcase = {
            'image_url': 'https://fts.unocha.org/sites/default/files/styles/fts_feature_image/public/navigation_101.jpg',
            'name': 'fts-requirements-and-funding-data-for-afghanistan-showcase',
            'notes': 'Click the image on the right to go to the FTS funding summary page for Afghanistan',
            'url': 'https://fts.unocha.org/countries/1/flows/2017', 'title': 'FTS Afghanistan Summary Page',
            'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                     {'name': 'financial tracking service - fts', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                     {'name': 'aid funding', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}

        def compare_afg(dataset, showcase, hxl_resource, expected_resources=afgresources, expected_hxl_resource='fts_requirements_funding_cluster_afg.csv', prefix=''):
            assert dataset == afgdataset
            resources = dataset.get_resources()
            assert resources == expected_resources
            if prefix:
                prefix = '%s_' % prefix
            for resource in resources:
                expected_file = join('tests', 'fixtures', '%s%s' % (prefix, resource['name']))
                actual_file = join(folder, resource['name'])
                assert_files_same(expected_file, actual_file)
            assert showcase == afgshowcase
            assert hxl_resource == expected_hxl_resource

        with temp_dir('fts') as folder:
            today = datetime.strptime('01062017', '%d%m%Y').date()
            test = 'nofundnoreq'
            dataset, showcase, hxl_resource = generate_dataset_and_showcase('http://%s/' % test, downloader, folder, 'AFG', 'Afghanistan', 1, today)
            assert dataset is None
            assert showcase is None
            assert hxl_resource is None
            test = 'nofund'
            dataset, showcase, hxl_resource = generate_dataset_and_showcase('http://%s/' % test, downloader, folder, 'AFG', 'Afghanistan', 1, today)
            compare_afg(dataset, showcase, hxl_resource, expected_resources=afgresources[1:], expected_hxl_resource=None, prefix=test)
            test = 'noreq'
            dataset, showcase, hxl_resource = generate_dataset_and_showcase('http://%s/' % test, downloader, folder, 'AFG', 'Afghanistan', 1, today)
            compare_afg(dataset, showcase, hxl_resource, expected_hxl_resource=None, prefix=test)

            dataset, showcase, hxl_resource = generate_dataset_and_showcase('http://afgsite/', downloader, folder, 'AFG', 'Afghanistan', 1, today)
            compare_afg(dataset, showcase, hxl_resource)

    def test_generate_cpv_dataset_and_showcase(self, configuration, downloader):
        with temp_dir('fts') as folder:
            today = datetime.strptime('01062018', '%d%m%Y').date()
            dataset, showcase, hxl_resource = generate_dataset_and_showcase('http://cpvsite/', downloader, folder, 'CPV', 'Cape Verde', 1, today)
            assert dataset == {'groups': [{'name': 'cpv'}], 'name': 'fts-requirements-and-funding-data-for-cape-verde',
                               'title': 'Cape Verde - Requirements and Funding Data',
                               'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                        {'name': 'financial tracking service - fts', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                        {'name': 'aid funding', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                               'dataset_date': '06/01/2018',
                               'data_update_frequency': '1', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                               'owner_org': 'fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e', 'subnational': '0'}

            resources = dataset.get_resources()
            assert resources == [{'name': 'fts_incoming_funding_cpv.csv', 'description': 'FTS Incoming Funding Data for Cape Verde for 2018', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                                 {'name': 'fts_requirements_funding_cpv.csv', 'description': 'FTS Annual Requirements and Funding Data for Cape Verde', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]
            for resource in resources:
                resource_name = resource['name']
                expected_file = join('tests', 'fixtures', resource_name)
                actual_file = join(folder, resource_name)
                assert_files_same(expected_file, actual_file)

            assert showcase == {'image_url': 'https://fts.unocha.org/sites/default/files/styles/fts_feature_image/public/navigation_101.jpg',
                                'name': 'fts-requirements-and-funding-data-for-cape-verde-showcase',
                                'notes': 'Click the image on the right to go to the FTS funding summary page for Cape Verde',
                                'url': 'https://fts.unocha.org/countries/1/flows/2018', 'title': 'FTS Cape Verde Summary Page',
                                'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'financial tracking service - fts', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'aid funding', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}
            assert hxl_resource is None

    def test_generate_alb_dataset_and_showcase(self, configuration, downloader):
        with temp_dir('fts') as folder:
            today = datetime.strptime('01062018', '%d%m%Y').date()
            dataset, showcase, hxl_resource = generate_dataset_and_showcase('http://albsite/', downloader, folder, 'ALB', 'Albania', 1, today)
            assert dataset == {'groups': [{'name': 'alb'}], 'name': 'fts-requirements-and-funding-data-for-albania',
                               'title': 'Albania - Requirements and Funding Data',
                               'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                        {'name': 'financial tracking service - fts', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                        {'name': 'aid funding', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                               'dataset_date': '06/01/2018',
                               'data_update_frequency': '1', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                               'owner_org': 'fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e', 'subnational': '0'}

            resources = dataset.get_resources()
            assert resources == [{'name': 'fts_incoming_funding_alb.csv', 'description': 'FTS Incoming Funding Data for Albania for 2018', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                                 {'name': 'fts_requirements_funding_alb.csv', 'description': 'FTS Annual Requirements and Funding Data for Albania', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]
            for resource in resources:
                resource_name = resource['name']
                expected_file = join('tests', 'fixtures', resource_name)
                actual_file = join(folder, resource_name)
                assert_files_same(expected_file, actual_file)

            assert showcase == {'image_url': 'https://fts.unocha.org/sites/default/files/styles/fts_feature_image/public/navigation_101.jpg',
                                'name': 'fts-requirements-and-funding-data-for-albania-showcase',
                                'notes': 'Click the image on the right to go to the FTS funding summary page for Albania',
                                'url': 'https://fts.unocha.org/countries/1/flows/2018', 'title': 'FTS Albania Summary Page',
                                'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'financial tracking service - fts', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'aid funding', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}
            assert hxl_resource is None
