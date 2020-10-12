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
from fts.requirements_funding_covid import RequirementsFundingCovid
from requirements_funding_cluster import RequirementsFundingCluster

logger = logging.getLogger(__name__)


class FTS:
    def __init__(self, downloader, locations, today, notes, start_year=1998):
        self.downloader = downloader
        self.locations = locations
        self.today = today
        self.notes = notes
        self.plans_by_year_by_country = dict()
        self.planidcodemapping = dict()
        self.planidswithonelocation = set()
        self.reqfund = RequirementsFunding(downloader, locations, today)
        self.get_plans(start_year=start_year)
        self.flows = Flows(downloader, locations, self.planidcodemapping)
        self.others = self.setup_others(downloader, locations)

    def setup_others(self, downloader, locations):
        covid = RequirementsFundingCovid(downloader, self.plans_by_year_by_country)
        cluster = RequirementsFundingCluster(downloader, locations, self.planidswithonelocation)
        globalcluster = RequirementsFundingCluster(downloader, locations, self.planidswithonelocation, clusterlevel='global')
        return {'covid': covid, 'cluster': cluster, 'globalcluster': globalcluster}

    def get_plans(self, start_year=1998):
        for year in range(self.today.year, start_year, -1):
            data = self.downloader.download(f'fts/flow/plan/overview/progress/{year}', use_v2=True)
            plans = data['plans']
            for plan in plans:
                planid = plan['id']
                self.planidcodemapping[planid] = plan['code']
                countries = plan['countries']
                if countries:
                    self.reqfund.add_country_requirements_funding(planid, plan, countries)
                    if len(countries) == 1:
                        self.planidswithonelocation.add(planid)
                    for country in countries:
                        countryiso = country['iso3']
                        if not countryiso:
                            continue
                        plans_by_year = self.plans_by_year_by_country.get(countryiso, {})
                        dict_of_lists_add(plans_by_year, year, plan)
                        self.plans_by_year_by_country[countryiso] = plans_by_year

    def call_others(self, row):
        requirements_clusters, funding_clusters, notspecified, shared = self.others['cluster'].get_requirements_funding_plan(row)
        self.others['cluster'].generate_rows_requirements_funding(row, requirements_clusters, funding_clusters, notspecified, shared)
        self.others['covid'].generate_plan_requirements_funding(row, requirements_clusters)
        self.others['globalcluster'].generate_plan_requirements_funding(row)

    def generate_other_resources(self, resources, folder, dataset, country):
        resource = self.others['globalcluster'].generate_resource(folder, dataset, country)
        if resource:
            resources.insert(1, resource)
        hxlresource = self.others['cluster'].generate_resource(folder, dataset, country)
        if hxlresource:
            resources.insert(1, hxlresource)
        resource = self.others['covid'].generate_resource(folder, dataset, country)
        if resource:
            resources.insert(1, resource)
        return hxlresource

    def generate_dataset_and_showcase(self, folder, country):
        '''
        api.hpc.tools/v1/public/fts/flow?countryISO3=CMR&Year=2016&groupby=cluster
        '''
        countryname = country['name']
        if countryname == 'World':
            logger.info(f'Ignoring {countryname}')
            return None, None, None, None
        title = f'{countryname} - Requirements and Funding Data'
        countryiso = country['iso3']
        if countryiso is None:
            logger.error(f'{title} has a problem! Iso3 is None!')
            return None, None, None, None
        logger.info(f'Adding FTS data for {countryname}')
        latestyear = str(self.today.year)
        slugified_name = slugify(f'FTS Requirements and Funding Data for {countryname}').lower()
        showcase_url = f'https://fts.unocha.org/countries/{country["id"]}/flows/{latestyear}'
        dataset, showcase = get_dataset_and_showcase(slugified_name, title, self.notes, self.today, countryname,
                                                     showcase_url, additional_tags=['covid-19'])
        try:
            dataset.add_country_location(countryiso)
        except HDXError as e:
            logger.error(f'{title} has a problem! {e}')
            return None, None, None, None
        resources = self.flows.generate_resources(folder, dataset, latestyear, country)
        if len(resources) == 0:
            logger.warning('No requirements or funding data available')
            return None, None, None, None

        hxl_resource = None
        plans_by_year = self.plans_by_year_by_country.get(countryiso)
        if plans_by_year is None:
            logger.error(f'We have latest year funding data but no overall funding data for {title}')
        else:
            hxl_resource = self.reqfund.generate_resource(folder, dataset, plans_by_year, country, self.call_others)
            resources.insert(0, hxl_resource)
            other_hxl_resource = self.generate_other_resources(resources, folder, dataset, country)
            if other_hxl_resource:
                hxl_resource = other_hxl_resource
        ordered_resource_names = [x['name'] for x in resources]
        return dataset, showcase, hxl_resource, ordered_resource_names
