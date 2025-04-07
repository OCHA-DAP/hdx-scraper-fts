#!/usr/bin/python
"""
FTS:
---

Generates FTS datasets.

"""

import logging

from hdx.scraper.fts.flows import Flows
from hdx.scraper.fts.requirements_funding import RequirementsFunding
from hdx.scraper.fts.requirements_funding_cluster import RequirementsFundingCluster
from hdx.scraper.fts.requirements_funding_covid import RequirementsFundingCovid
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, downloader, locations, today, start_year=1998):
        self.downloader = downloader
        self.locations = locations
        self.today = today
        self.plans_by_year_by_country = dict()
        self.planidcodemapping = dict()
        self.planidswithonelocation = set()
        self.globalplanids = set()
        self.reqfund = RequirementsFunding(
            downloader, locations, self.globalplanids, today
        )
        self.get_plans(start_year=start_year)
        self.flows = Flows(downloader, locations, self.planidcodemapping)
        self.others = self.setup_others(downloader, locations)

    def setup_others(self, downloader, locations):
        covid = RequirementsFundingCovid(
            downloader, locations, self.plans_by_year_by_country
        )
        cluster = RequirementsFundingCluster(downloader, self.planidswithonelocation)
        globalcluster = RequirementsFundingCluster(
            downloader, self.planidswithonelocation, clusterlevel="global"
        )
        return {"covid": covid, "cluster": cluster, "globalcluster": globalcluster}

    def get_plans(self, start_year=1998):
        for year in range(self.today.year, start_year, -1):
            data = self.downloader.download(f"2/fts/flow/plan/overview/progress/{year}")
            plans = data["plans"]
            for plan in plans:
                planid = plan["id"]
                self.planidcodemapping[planid] = plan["code"]
                countries = plan["countries"]
                if countries:
                    is_global = self.reqfund.add_country_requirements_funding(
                        planid, plan, countries
                    )
                    if is_global:
                        self.globalplanids.add(planid)
                    if len(countries) == 1:
                        self.planidswithonelocation.add(planid)
                    for country in countries:
                        countryiso3 = country["iso3"]
                        if not countryiso3:
                            continue
                        plans_by_year = self.plans_by_year_by_country.get(
                            countryiso3, {}
                        )
                        dict_of_lists_add(plans_by_year, year, plan)
                        self.plans_by_year_by_country[countryiso3] = plans_by_year

    def call_others(self, row):
        requirements_clusters, funding_clusters, notspecified, shared = self.others[
            "cluster"
        ].get_requirements_funding_plan(row)
        self.others["cluster"].generate_rows_requirements_funding(
            row, requirements_clusters, funding_clusters, notspecified, shared
        )
        self.others["covid"].generate_plan_funding(row)
        self.others["globalcluster"].generate_plan_requirements_funding(row)

    def generate_other_resources(self, resources, folder, dataset, country):
        hxlresource = None
        resource = self.others["globalcluster"].generate_resource(
            folder, dataset, country
        )
        if resource:
            resources.insert(1, resource)
        resource = self.others["cluster"].generate_resource(folder, dataset, country)
        if resource:
            resources.insert(1, resource)
            if self.others["cluster"].can_make_quickchart(country["iso3"]):
                hxlresource = resource
        resource = self.others["covid"].generate_resource(folder, dataset, country)
        if resource:
            resources.insert(1, resource)
        return hxlresource

    def generate_dataset_and_showcase(self, folder, country, dataset):
        """
        api.hpc.tools/v1/public/fts/flow?countryISO3=CMR&Year=2016&groupby=cluster
        """

        resources, start_date = self.flows.generate_resources(
            folder, dataset, self.today.year, country
        )
        if len(resources) == 0:
            logger.warning("No requirements or funding data available")
            return None, None, None

        hxl_resource = None
        countryiso3 = country["iso3"]
        plans_by_year = self.plans_by_year_by_country.get(countryiso3)
        if plans_by_year is None:
            logger.error(
                f"We have latest year funding data but no overall funding data for {countryiso3}"
            )
        else:
            hxl_resource, reqfund_start_year = self.reqfund.generate_resource(
                folder, dataset, plans_by_year, country, self.call_others
            )
            reqfund_start_date = parse_date(f"{reqfund_start_year}-01-01")
            if reqfund_start_date < start_date:
                start_date = reqfund_start_date
            resources.insert(0, hxl_resource)
            other_hxl_resource = self.generate_other_resources(
                resources, folder, dataset, country
            )
            if other_hxl_resource:
                hxl_resource = other_hxl_resource
        dataset.resources = resources
        dataset.set_time_period(start_date, self.today)
        return hxl_resource
