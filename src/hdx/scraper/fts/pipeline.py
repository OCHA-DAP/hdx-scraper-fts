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
from hdx.utilities.dateparse import default_enddate, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, downloader, folder, locations, today, start_year=1998):
        self._downloader = downloader
        self._today = today
        self._plans_by_year_by_country = {}
        self._planidcodemapping = {}
        self._planidswithonelocation = set()
        self._globalplanids = set()
        self._reqfund = RequirementsFunding(
            downloader, folder, locations, self._globalplanids, today
        )
        self.get_plans(start_year=start_year)
        self._flows = Flows(
            downloader, folder, locations, self._planidcodemapping, today
        )
        self._others = self.setup_others(folder, locations)
        self._start_date = default_enddate

    def setup_others(self, folder, locations):
        covid = RequirementsFundingCovid(
            self._downloader, folder, locations, self._plans_by_year_by_country
        )
        cluster = RequirementsFundingCluster(
            self._downloader, folder, self._planidswithonelocation
        )
        globalcluster = RequirementsFundingCluster(
            self._downloader,
            folder,
            self._planidswithonelocation,
            clusterlevel="global",
        )
        return {"covid": covid, "cluster": cluster, "globalcluster": globalcluster}

    def get_plans(self, start_year=1998):
        for year in range(self._today.year, start_year, -1):
            data = self._downloader.download(
                f"2/fts/flow/plan/overview/progress/{year}"
            )
            plans = data["plans"]
            for plan in plans:
                planid = plan["id"]
                self._planidcodemapping[planid] = plan["code"]
                countries = plan["countries"]
                if countries:
                    is_global = self._reqfund.add_country_requirements_funding(
                        planid, plan, countries
                    )
                    if is_global:
                        self._globalplanids.add(planid)
                    if len(countries) == 1:
                        self._planidswithonelocation.add(planid)
                    for country in countries:
                        countryiso3 = country["iso3"]
                        if not countryiso3:
                            continue
                        plans_by_year = self._plans_by_year_by_country.get(
                            countryiso3, {}
                        )
                        dict_of_lists_add(plans_by_year, year, plan)
                        self._plans_by_year_by_country[countryiso3] = plans_by_year

    def call_others(self, row):
        requirements_clusters, funding_clusters, notspecified, shared = self._others[
            "cluster"
        ].get_requirements_funding_plan(row)
        self._others["cluster"].generate_rows_requirements_funding(
            row, requirements_clusters, funding_clusters, notspecified, shared
        )
        self._others["covid"].generate_plan_funding(row)
        self._others["globalcluster"].generate_plan_requirements_funding(row)

    def generate_other_resources(self, resources, dataset, country):
        hxlresource = None
        resource = self._others["globalcluster"].generate_country_resource(
            dataset, country
        )
        if resource:
            resources.insert(1, resource)
        resource = self._others["cluster"].generate_country_resource(dataset, country)
        if resource:
            resources.insert(1, resource)
            if self._others["cluster"].can_make_quickchart(country["iso3"]):
                hxlresource = resource
        resource = self._others["covid"].generate_country_resource(dataset, country)
        if resource:
            resources.insert(1, resource)
        return hxlresource

    def generate_country_dataset_and_showcase(self, country, dataset):
        """
        api.hpc.tools/v1/public/fts/flow?countryISO3=CMR&Year=2016&groupby=cluster
        """

        resources, start_date = self._flows.generate_country_resources(dataset, country)
        if len(resources) == 0:
            logger.warning("No requirements or funding data available")
            return False, None

        hxl_resource = None
        countryiso3 = country["iso3"]
        plans_by_year = self._plans_by_year_by_country.get(countryiso3)
        if plans_by_year is None:
            logger.error(
                f"We have latest year funding data but no overall funding data for {countryiso3}"
            )
        else:
            hxl_resource, reqfund_start_year = self._reqfund.generate_country_resource(
                dataset, plans_by_year, country, self.call_others
            )
            reqfund_start_date = parse_date(f"{reqfund_start_year}-01-01")
            if reqfund_start_date < start_date:
                start_date = reqfund_start_date
            resources.insert(0, hxl_resource)
            other_hxl_resource = self.generate_other_resources(
                resources, dataset, country
            )
            if other_hxl_resource:
                hxl_resource = other_hxl_resource
        dataset.resources = resources
        dataset.set_time_period(start_date, self._today)
        if start_date < self._start_date:
            self._start_date = start_date
        return True, hxl_resource

    def generate_global_dataset(self, dataset):
        success, results = self._reqfund.generate_global_resource(dataset)
        self._others["covid"].generate_global_resource(dataset)
        self._others["cluster"].generate_global_resource(dataset)
        self._others["globalcluster"].generate_global_resource(dataset)
        self._flows.generate_global_resources(dataset)
        dataset.set_time_period(self._start_date, self._today)
        results["dataset"] = dataset
        return results
