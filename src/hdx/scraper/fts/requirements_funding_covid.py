import copy
import logging

from hdx.scraper.fts.helpers import hxl_names
from hdx.scraper.fts.resource_generator import ResourceGenerator

logger = logging.getLogger(__name__)


class RequirementsFundingCovid(ResourceGenerator):
    def __init__(self, downloader, folder, locations, plans_by_year_by_country):
        super().__init__(downloader, folder, hxl_names)
        self._covidfundingbyplanandlocation = {}
        self._rows = []
        self._get_covid_funding(locations.get_id_to_iso3(), plans_by_year_by_country)
        self._filename = "fts_requirements_funding_covid"
        self._description = "FTS Annual Requirements, Funding and Covid Funding Data"

    def _get_covid_funding(
        self, locationid_to_iso3, plans_by_year_by_country, covidstartyear=2020
    ):
        multiplecountry_planids = {}
        planid_to_country = {}
        for plans_by_year in plans_by_year_by_country.values():
            for year in plans_by_year:
                if year < covidstartyear:
                    continue
                for plan in plans_by_year[year]:
                    planid = str(plan["id"])
                    countryiso3s = set()
                    for country in plan["countries"]:
                        adminlevel = country.get(
                            "adminlevel", country.get("adminLevel")
                        )
                        if adminlevel == 0:
                            countryiso3s.add(country["iso3"])
                    if len(countryiso3s) == 1:
                        planid_to_country[planid] = countryiso3s.pop()
                    else:
                        multiplecountry_planids[planid] = countryiso3s

        onecountry_planids = ",".join(sorted(planid_to_country.keys()))
        data = self._downloader.download(
            f"1/fts/flow/custom-search?emergencyid=911&planid={onecountry_planids}&groupby=plan"
        )
        for fundingobject in data["report3"]["fundingTotals"]["objects"][0][
            "objectsBreakdown"
        ]:
            planid = fundingobject.get("id")
            countryiso3 = planid_to_country[planid]
            self._covidfundingbyplanandlocation[f"{planid}-{countryiso3}"] = (
                fundingobject["totalFunding"]
            )

        for planid in multiplecountry_planids:
            data = self._downloader.download(
                f"1/fts/flow/custom-search?emergencyid=911&planid={planid}&groupby=location"
            )
            fundingobjects = data["report3"]["fundingTotals"]["objects"]
            if len(fundingobjects) == 0:
                continue
            for fundingobject in fundingobjects[0]["objectsBreakdown"]:
                locationid = int(fundingobject["id"])
                countryiso3 = locationid_to_iso3.get(locationid)
                if countryiso3:
                    self._covidfundingbyplanandlocation[f"{planid}-{countryiso3}"] = (
                        fundingobject["totalFunding"]
                    )

    def generate_plan_funding(self, inrow):
        planid = inrow["id"]
        countryiso3 = inrow["countryCode"]
        covidfunding = self._covidfundingbyplanandlocation.get(
            f"{planid}-{countryiso3}"
        )
        if covidfunding is None:
            logger.info(
                f"Location {countryiso3} of plan {planid} has no COVID component!"
            )
            return
        row = copy.deepcopy(inrow)
        del row["percentFunded"]
        row["covidFunding"] = covidfunding
        row["covidPercentageOfFunding"] = int(
            covidfunding / inrow["funding"] * 100 + 0.5
        )
        self._rows.append(row)

    def generate_country_resource(self, dataset, country):
        if not self._rows:
            return None
        countryiso3 = country["iso3"]
        success, results = self.generate_resource(
            dataset, self._rows, countryiso3, countryname=country["name"]
        )
        self._global_rows[countryiso3] = self._rows
        self._rows = []
        if success:
            return results["resource"]
        else:
            return None
