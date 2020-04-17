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
    planids_by_emergency = dict()
    plans_by_country = dict()
    plans_by_emergency = dict()
    for country in countries:
        countryiso = country['iso3']
        plans_by_country[countryiso] = list()
    for year in range(today.year, start_year, -1):
        data = download_data('%splan/year/%d' % (base_url, year), downloader)
        for plan in data:
            plan_id = plan['id']
            all_plans[str(plan_id)] = plan
            for emergency in plan['emergencies']:
                emergency_id = emergency['id']
                if emergency_id not in planids_by_emergency or plan_id not in planids_by_emergency[emergency_id]:
                    dict_of_sets_add(planids_by_emergency, emergency_id, plan_id)
                    dict_of_lists_add(plans_by_emergency, emergency_id, plan)
            for location in plan['locations']:
                countryiso = location['iso3']
                if not countryiso:
                    continue
                if countryiso not in planids_by_country or plan_id not in planids_by_country[countryiso]:
                    dict_of_lists_add(planids_by_country, countryiso, plan_id)
                    dict_of_lists_add(plans_by_country, countryiso, plan)

    return all_plans, plans_by_emergency, plans_by_country


def generate_emergency_dataset_and_showcase(base_url, downloader, folder, emergency, all_plans, plans_by_emergency, today, notes):
    # https://api.hpc.tools/v1/public/emergency/id/911
    emergencyid = emergency['emergency_id']
    latestyear = str(today.year)
    emergency_url = '%semergency/id/%d' % (base_url, emergencyid)
    data = download_data(emergency_url, downloader)
    name = data['name']
    glideid = data.get('glideId')
    date = data['date']
    slugified_name = slugify('FTS Funding Data for %s' % name).lower()
    title = '%s Funding Data' % name
    description = '%s  \n  \nGlide Id=%s, Date=%s' % (notes, glideid, date)
    showcase_url = 'https://fts.unocha.org/emergencies/%d/flows/%s' % (emergencyid, latestyear)
    dataset, showcase = get_dataset_and_showcase(slugified_name, title, description, today, name, showcase_url)
    dataset.add_other_location('world')
    objecttype = 'emergency'
    fund_boundaries_info = generate_flows_resources(objecttype, base_url, downloader, folder, dataset, str(emergencyid),
                                                    name, latestyear)
    generate_flows_files(fund_boundaries_info, dict())
    return dataset, showcase


def generate_dataset_and_showcase(base_url, downloader, folder, country, all_plans, plans_by_country, today, notes):
    '''
    api.hpc.tools/v1/public/fts/flow?countryISO3=CMR&Year=2016&groupby=cluster
    '''
    countryname = country['name']
    if countryname == 'World':
        logger.info('Ignoring  %s' % countryname)
        return None, None, None
    logger.info('Adding FTS data for %s' % countryname)
    latestyear = str(today.year)
    slugified_name = slugify('FTS Requirements and Funding Data for %s' % countryname).lower()
    title = '%s - Requirements and Funding Data' % countryname
    showcase_url = 'https://fts.unocha.org/countries/%s/flows/%s' % (country['id'], latestyear)
    dataset, showcase = get_dataset_and_showcase(slugified_name, title, notes, today, countryname, showcase_url)

    countryiso = country['iso3']
    if countryiso is None:
        logger.error('%s has a problem! Iso3 is None!' % title)
        return None, None, None
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.error('%s has a problem! %s' % (title, e))
        return None, None, None

    objecttype = 'location'
    fund_boundaries_info = generate_flows_resources(objecttype, base_url, downloader, folder, dataset,
                                                    countryiso, countryname, latestyear)
    plans = plans_by_country[countryiso]
    dffundreq, planids, planidcodemapping, incompleteplans = \
        generate_requirements_funding_resource(objecttype, base_url, all_plans, plans, downloader, folder, countryname,
                                               countryiso, dataset)
    if dffundreq is None:
        hxl_resource = None
        if len(fund_boundaries_info) == 0:
            logger.warning('No requirements or funding data available')
            return None, None, None
        else:
            logger.error('We have latest year funding data but no overall funding data for %s' % title)
    else:
        hxl_resource = generate_requirements_funding_cluster_resource(base_url, downloader, folder, countryname,
                                                                      countryiso, planids, dffundreq, all_plans,
                                                                      dataset)

    generate_flows_files(fund_boundaries_info, planidcodemapping)
    return dataset, showcase, hxl_resource
