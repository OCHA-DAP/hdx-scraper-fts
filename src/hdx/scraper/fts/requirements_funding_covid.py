import copy
import logging

from hdx.scraper.fts.helpers import hxl_names

logger = logging.getLogger(__name__)


class RequirementsFundingCovid:
    def __init__(self, downloader, locations, plans_by_year_by_country):
        self.downloader = downloader
        self.covidfundingbyplanandlocation = {}
        self.rows = []
        self.get_covid_funding(locations.get_id_to_iso3(), plans_by_year_by_country)

    def clear_rows(self):
        self.rows = []

    def get_covid_funding(
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
        data = self.downloader.download(
            f"1/fts/flow/custom-search?emergencyid=911&planid={onecountry_planids}&groupby=plan"
        )
        for fundingobject in data["report3"]["fundingTotals"]["objects"][0][
            "objectsBreakdown"
        ]:
            planid = fundingobject.get("id")
            countryiso3 = planid_to_country[planid]
            self.covidfundingbyplanandlocation[f"{planid}-{countryiso3}"] = (
                fundingobject["totalFunding"]
            )

        for planid in multiplecountry_planids:
            data = self.downloader.download(
                f"1/fts/flow/custom-search?emergencyid=911&planid={planid}&groupby=location"
            )
            fundingobjects = data["report3"]["fundingTotals"]["objects"]
            if len(fundingobjects) == 0:
                continue
            for fundingobject in fundingobjects[0]["objectsBreakdown"]:
                locationid = int(fundingobject["id"])
                countryiso3 = locationid_to_iso3.get(locationid)
                if countryiso3:
                    self.covidfundingbyplanandlocation[f"{planid}-{countryiso3}"] = (
                        fundingobject["totalFunding"]
                    )

    def generate_plan_funding(self, inrow):
        planid = inrow["id"]
        countryiso3 = inrow["countryCode"]
        covidfunding = self.covidfundingbyplanandlocation.get(f"{planid}-{countryiso3}")
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
        self.rows.append(row)

    def generate_resource(self, folder, dataset, country):
        if not self.rows:
            return None
        headers = list(self.rows[0].keys())
        filename = f"fts_requirements_funding_covid_{country['iso3'].lower()}.csv"
        resourcedata = {
            "name": filename,
            "description": f"FTS Annual Requirements, Funding and Covid Funding Data for {country['name']}",
            "format": "csv",
        }
        success, results = dataset.generate_resource_from_iterator(
            headers, self.rows, hxl_names, folder, filename, resourcedata
        )
        self.rows = []
        if success:
            return results["resource"]
        else:
            return None
