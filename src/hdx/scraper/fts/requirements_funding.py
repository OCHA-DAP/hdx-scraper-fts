import logging

from hdx.scraper.fts.resource_generator import ResourceGenerator

logger = logging.getLogger(__name__)


def create_row(countryiso3, year, plan=None, name="Not specified"):
    row = {
        "countryCode": countryiso3,
        "id": "",
        "name": name,
        "code": "",
        "typeId": "",
        "typeName": "",
        "isPartOfGHO": "N",
        "startDate": "",
        "endDate": "",
        "year": year,
        "requirements": "",
        "funding": "",
        "percentFunded": "",
    }
    if plan:
        row["id"] = plan["id"]
        row["name"] = plan["name"]
        row["code"] = plan["code"]
        row["typeId"] = plan["planType"]["id"]
        row["typeName"] = plan["planType"]["name"]
        row["isPartOfGHO"] = "Y" if plan.get("isPartOfGHO") else "N"
        row["startDate"] = plan["startDate"]
        row["endDate"] = plan["endDate"]
    return row


class RequirementsFunding(ResourceGenerator):
    def __init__(self, downloader, folder, locations, globalplanids, today):
        super().__init__(downloader, folder)
        self._locations = locations
        self._globalplanids = globalplanids
        self._today = today
        self._filename = "fts_requirements_funding"
        self._description = "FTS Annual Requirements and Funding Data"

    def add_country_requirements_funding(self, planid, plan, countries):
        if len(countries) == 1:
            requirements = plan.get("requirements")
            if requirements is not None:
                requirements = requirements.get("revisedRequirements")
            funding = plan.get("funding")
            if funding is None:
                progress = None
            else:
                progress = funding.get("progress")
                funding = funding.get("totalFunding")
            countries[0]["requirements"] = requirements
            countries[0]["funding"] = funding
            if progress:
                progress = int(progress + 0.5)
            countries[0]["percentFunded"] = progress
        else:
            if plan.get("customLocationCode") == "COVD":
                return True
            funding_url = f"1/fts/flow/custom-search?planid={planid}&groupby=location"
            data = self._downloader.download(funding_url)
            requirements = data.get("requirements")
            country_requirements = {}
            if requirements is not None:
                totalreq = requirements["totalRevisedReqs"]
                countryreq_is_totalreq = True
                for req_object in requirements.get("objects", []):
                    country_id = self._locations.get_countryid_from_object(req_object)
                    country_req = req_object.get("revisedRequirements")
                    if country_id is not None and country_req is not None:
                        country_requirements[country_id] = country_req
                        if country_req != totalreq:
                            countryreq_is_totalreq = False
                if countryreq_is_totalreq:
                    logger.info(
                        f"Ignoring {planid} country requirements as same as total requirements!"
                    )
                    country_requirements = {}
            fund_objects = data["report3"]["fundingTotals"]["objects"]
            country_funding = {}
            if len(fund_objects) == 1:
                for fund_object in fund_objects[0].get("objectsBreakdown", []):
                    country_id = self._locations.get_countryid_from_object(fund_object)
                    country_fund = fund_object.get("totalFunding")
                    if country_id is not None and country_fund is not None:
                        country_funding[int(country_id)] = country_fund
            for country in countries:
                countryid = country["id"]
                requirements = country_requirements.get(countryid)
                country["requirements"] = requirements
                funding = country_funding.get(countryid)
                country["funding"] = funding
                if requirements is not None and funding is not None:
                    country["percentFunded"] = int(funding / requirements * 100 + 0.5)
        return False

    def get_country_funding(self, countryid, plans_by_year, start_year=2010):
        funding_by_year = {}
        if plans_by_year:
            start_year = sorted(plans_by_year.keys())[0]
        for year in range(self._today.year + 5, start_year - 5, -11):
            data = self._downloader.download(
                f"2/country/{countryid}/summary/trends/{year}"
            )
            for object in data:
                year = object["year"]
                funding = object["totalFunding"]
                if funding:
                    funding_by_year[year] = funding
        return funding_by_year

    def generate_country_rows(self, plans_by_year, country, call_others=lambda x: None):
        countryiso3 = country["iso3"]
        funding_by_year = self.get_country_funding(country["id"], plans_by_year)
        rows = []

        all_years = sorted(
            set(plans_by_year.keys()) | set(funding_by_year.keys()), reverse=True
        )
        for year in all_years:
            not_specified_funding = funding_by_year.get(year, "")
            subrows = []
            for plan in plans_by_year.get(year, []):
                planid = plan["id"]
                if planid in self._globalplanids:
                    continue
                found_other_countries = False
                for country in plan["countries"]:
                    adminlevel = country.get("adminlevel", country.get("adminLevel"))
                    if adminlevel == 0 and country["iso3"] != countryiso3:
                        found_other_countries = True
                        continue
                    requirements = country.get("requirements")
                    funding = country.get("funding")
                    if requirements is None:
                        if funding is None:
                            continue
                        requirements = ""
                    if funding is None:
                        funding = ""
                    percentFunded = country.get("percentFunded", "")
                    if not_specified_funding and funding:
                        not_specified_funding -= funding
                    row = create_row(countryiso3, year, plan)
                    row["requirements"] = requirements
                    row["funding"] = funding
                    row["percentFunded"] = percentFunded
                    subrows.append(row)

                if found_other_countries:
                    logger.warning(
                        f"Plan {planid} spans multiple locations - ignoring in cluster breakdown!"
                    )
                    continue
            for row in sorted(subrows, key=lambda k: (k["typeId"], k["id"])):
                rows.append(row)
                call_others(row)

            row = create_row(countryiso3, year)
            row["funding"] = not_specified_funding
            rows.append(row)
        return rows, all_years

    def generate_country_resource(
        self, dataset, plans_by_year, country, call_others=lambda x: None
    ):
        rows, all_years = self.generate_country_rows(
            plans_by_year, country, call_others
        )
        if not rows:
            return None, None
        countryname = country["name"]
        self._global_rows[countryname] = rows
        _, results = self.generate_resource(
            dataset, rows, country["iso3"], countryname=countryname
        )
        return results["resource"], all_years[-1]

    def add_country_global_rows(self, plans_by_year, country):
        rows, _ = self.generate_country_rows(plans_by_year, country)
        if rows:
            self._global_rows[country["name"]] = rows

    def get_global_rows(self):
        return [
            {"location": countryname, "locationType": "country", **row}
            for countryname in sorted(self._global_rows)
            for row in self._global_rows[countryname]
        ]

    def generate_global_resource(self, dataset, headers, country_rows, noncountry_rows):
        return self.generate_resource(
            dataset, country_rows + noncountry_rows, "global", headers=headers
        )
