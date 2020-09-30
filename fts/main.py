#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
FTS:
---

Generates FTS datasets.

'''
import logging

from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import dict_of_lists_add
from slugify import slugify

from fts.flows import Flows
from fts.helpers import get_dataset_and_showcase
from fts.requirements_funding import RequirementsFunding
from fts.requirements_funding_cluster import generate_requirements_funding_cluster_resource

logger = logging.getLogger(__name__)


class FTS:
    def __init__(self, downloader, locations, today, notes):
        self.downloader = downloader
        self.locations = locations
        self.today = today
        self.notes = notes
        self.all_plans = dict()
        self.plans_by_year_by_country = dict()
        self.planidcodemapping = dict()
        self.flows = Flows(downloader, locations, self.planidcodemapping)
        self.reqfund = RequirementsFunding(downloader, locations, today)
        self.get_plans()

    def get_plans(self, start_year=1998):
        for year in range(self.today.year, start_year, -1):
            data = self.downloader.download_data(f'fts/flow/plan/overview/progress/{year}', use_v2=True)
            for plan in data['plans']:
                planid = plan['id']
                self.all_plans[str(planid)] = plan
                self.planidcodemapping[planid] = plan['code']
                countries = plan['countries']
                if countries:
                    self.reqfund.add_country_requirements_funding(planid, plan, countries)
                    for country in countries:
                        countryiso = country['iso3']
                        if not countryiso:
                            continue
                        plans_by_year = self.plans_by_year_by_country.get(countryiso, {})
                        dict_of_lists_add(plans_by_year, year, plan)
                        self.plans_by_year_by_country[countryiso] = plans_by_year

    def generate_dataset_and_showcase(self, folder, country):
        '''
        api.hpc.tools/v1/public/fts/flow?countryISO3=CMR&Year=2016&groupby=cluster
        '''
        countryname = country['name']
        if countryname == 'World':
            logger.info('Ignoring  %s' % countryname)
            return None, None, None
        title = '%s - Requirements and Funding Data' % countryname
        countryiso = country['iso3']
        if countryiso is None:
            logger.error('%s has a problem! Iso3 is None!' % title)
            return None, None, None
        logger.info('Adding FTS data for %s' % countryname)
        latestyear = str(self.today.year)
        slugified_name = slugify('FTS Requirements and Funding Data for %s' % countryname).lower()
        showcase_url = 'https://fts.unocha.org/countries/%d/flows/%s' % (country['id'], latestyear)
        dataset, showcase = get_dataset_and_showcase(slugified_name, title, self.notes, self.today, countryname,
                                                     showcase_url)

        try:
            dataset.add_country_location(countryiso)
        except HDXError as e:
            logger.error('%s has a problem! %s' % (title, e))
            return None, None, None
        no_resources = self.flows.generate_flows_resources(folder, dataset, latestyear, country)
        if no_resources == 0:
            logger.warning('No requirements or funding data available')
            return None, None, None

        hxl_resource = None
        plans_by_year = self.plans_by_year_by_country.get(countryiso)
        if plans_by_year is None:
            logger.error('We have latest year funding data but no overall funding data for %s' % title)
        else:
            hxl_resource = \
                self.reqfund.generate_requirements_funding_resource(folder, dataset, plans_by_year, country)
            if hxl_resource is None:
                pass
                # hxl_resource_c = generate_requirements_funding_cluster_resource(v1_url, downloader, folder, countryname,
                #                                                                 countryiso, planids, dffundreq, all_plans,
                #                                                                 dataset)
                # if hxl_resource_c:
                #     hxl_resource = hxl_resource_c
            else:
                logger.error('We have latest year funding data but no overall funding data for %s' % title)

        return dataset, showcase, hxl_resource
