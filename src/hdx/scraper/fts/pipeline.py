#!/usr/bin/python
"""
FTS:
---

Generates FTS datasets.

"""

import logging

from hdx.utilities.dateparse import default_enddate, parse_date
from hdx.utilities.dictandlist import dict_of_dicts_add, dict_of_lists_add

from hdx.scraper.fts.flows import Flows
from hdx.scraper.fts.noncountry_funding import NonCountryFunding
from hdx.scraper.fts.requirements_funding import RequirementsFunding
from hdx.scraper.fts.requirements_funding_cluster import RequirementsFundingCluster
from hdx.scraper.fts.requirements_funding_covid import RequirementsFundingCovid

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self, configuration, downloader, folder, locations, today, start_year=1999
    ):
        self._configuration = configuration
        self._downloader = downloader
        self._locations = locations
        self._today = today
        self._plans_by_year = {}
        self._plans_by_id = {}
        self._plans_by_year_by_country = {}
        self._planidcodemapping = {}
        self._planidswithonelocation = set()
        self._globalplanids = set()
        self._reqfund = RequirementsFunding(
            downloader, folder, locations, self._globalplanids, today, start_year
        )
        self.get_plans(start_year)
        self._flows = Flows(
            configuration, downloader, folder, locations, self._planidcodemapping, today
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

    def get_plans(self, start_year):
        for year in range(self._today.year, start_year - 1, -1):
            data = self._downloader.download(
                f"2/fts/flow/plan/overview/progress/{year}"
            )
            plans = data["plans"]
            for plan in plans:
                planid = plan["id"]
                self._planidcodemapping[planid] = plan["code"]
                dict_of_dicts_add(self._plans_by_year, year, planid, plan)
                self._plans_by_id[planid] = plan
                countries = plan["countries"]
                # 22 plans in 2000-2006 and 2017 have null countries, e.g. 180
                # Indian Ocean Earthquake-Tsunami 2005. Their countries are
                # filled from their requirements and funding by location.
                by_location = countries is None
                if by_location:
                    countries = []
                    plan["countries"] = countries
                    plan["countriesByLocation"] = True
                if countries or by_location:
                    is_global = self._reqfund.add_country_requirements_funding(
                        planid, plan, countries
                    )
                    if is_global:
                        self._globalplanids.add(planid)
                    if len(countries) == 1 and not by_location:
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
        resource = self._others["globalcluster"].generate_country_resource(
            dataset, country
        )
        if resource:
            resources.insert(1, resource)
        resource = self._others["cluster"].generate_country_resource(dataset, country)
        if resource:
            resources.insert(1, resource)
        resource = self._others["covid"].generate_country_resource(dataset, country)
        if resource:
            resources.insert(1, resource)

    def generate_country_dataset_and_showcase(self, country, dataset):
        """
        api.hpc.tools/v1/public/fts/flow?countryISO3=CMR&Year=2016&groupby=cluster
        """

        resources, start_date = self._flows.generate_country_resources(dataset, country)
        if len(resources) == 0:
            logger.warning("No requirements or funding data available")
            return False

        countryiso3 = country["iso3"]
        plans_by_year = self._plans_by_year_by_country.get(countryiso3, {})
        resource, reqfund_start_year = self._reqfund.generate_country_resource(
            dataset, plans_by_year, country, self.call_others
        )
        if resource is None:
            logger.error(f"No requirements or funding data for {countryiso3}")
        else:
            reqfund_start_date = parse_date(f"{reqfund_start_year}-01-01")
            start_date = min(start_date, reqfund_start_date)
            resources.insert(0, resource)
            self.generate_other_resources(resources, dataset, country)
        dataset._resources = resources
        dataset.set_time_period(start_date, self._today)
        self._start_date = min(self._start_date, start_date)
        return True

    def add_country_to_global(self, country):
        """Add requirements/funding rows to the global file for a country that
        has no country dataset (e.g. no flows)"""
        plans_by_year = self._plans_by_year_by_country.get(country["iso3"], {})
        self._reqfund.add_country_global_rows(plans_by_year, country)

    def generate_global_dataset(self, dataset):
        noncountry_funding = NonCountryFunding(
            self._downloader,
            self._locations,
            self._plans_by_year,
            self._plans_by_id,
        )
        country_rows = self._reqfund.get_global_rows()
        noncountry_rows = noncountry_funding.generate_rows(country_rows)
        _, results = self._reqfund.generate_global_resource(
            dataset,
            self._configuration["requirements_global_headers"],
            country_rows,
            noncountry_rows,
        )
        self._others["covid"].generate_global_resource(dataset)
        self._others["cluster"].generate_global_resource(dataset)
        self._others["globalcluster"].generate_global_resource(dataset)
        self._flows.generate_global_resources(dataset)
        dataset.set_time_period(self._start_date, self._today)
        results["dataset"] = dataset
        return results
