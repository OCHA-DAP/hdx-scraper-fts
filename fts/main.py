#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
FTS:
---

Generates FTS datasets.

'''
import logging

from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import dict_of_lists_add, dict_of_sets_add
from slugify import slugify

from fts.flows import generate_flows_resources, generate_flows_files
from fts.helpers import download_data, get_dataset_and_showcase
from fts.requirements_funding import generate_requirements_funding_resource
from fts.requirements_funding_cluster import generate_requirements_funding_cluster_resource

logger = logging.getLogger(__name__)


def get_countries(base_url, downloader):
    return download_data('%slocation' % base_url, downloader)


def get_plans(base_url, downloader, countries, today, start_year=1998):
    all_plans = dict()
    planids_by_country = dict()
    plans_by_country = dict()
    for country in countries:
        countryiso = country['iso3']
        plans_by_country[countryiso] = list()
    for year in range(today.year, start_year, -1):
        data = download_data('%splan/year/%d' % (base_url, year), downloader)
        for plan in data:
            plan_id = plan['id']
            all_plans[str(plan_id)] = plan
            for location in plan['locations']:
                countryiso = location['iso3']
                if not countryiso:
                    continue
                if countryiso not in planids_by_country or plan_id not in planids_by_country[countryiso]:
                    dict_of_lists_add(planids_by_country, countryiso, plan_id)
                    dict_of_lists_add(plans_by_country, countryiso, plan)

    return all_plans, plans_by_country


def generate_dataset_and_showcase(base_url, downloader, folder, country, all_plans, plans_by_country, today, notes):
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
    countryid = str(country['id'])
    latestyear = str(today.year)
    slugified_name = slugify('FTS Requirements and Funding Data for %s' % countryname).lower()
    showcase_url = 'https://fts.unocha.org/countries/%s/flows/%s' % (countryid, latestyear)
    dataset, showcase = get_dataset_and_showcase(slugified_name, title, notes, today, countryname, showcase_url)

    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.error('%s has a problem! %s' % (title, e))
        return None, None, None
    fund_boundaries_info = generate_flows_resources(base_url, downloader, folder, dataset,
                                                    countryid, countryname, latestyear, countryiso)
    plans = plans_by_country[countryiso]
    dffundreq, planids, planidcodemapping, incompleteplans, hxl_resource = \
        generate_requirements_funding_resource('location', base_url, all_plans, plans, downloader, folder, countryname,
                                               countryid, dataset, countryiso, countryiso.lower())
    if dffundreq is None:
        if len(fund_boundaries_info) == 0:
            logger.warning('No requirements or funding data available')
            return None, None, None
        else:
            logger.error('We have latest year funding data but no overall funding data for %s' % title)
    else:
        hxl_resource_c = generate_requirements_funding_cluster_resource(base_url, downloader, folder, countryname,
                                                                        countryiso, planids, dffundreq, all_plans,
                                                                        dataset)
        if hxl_resource_c:
            hxl_resource = hxl_resource_c

    generate_flows_files(fund_boundaries_info, planidcodemapping)
    return dataset, showcase, hxl_resource
