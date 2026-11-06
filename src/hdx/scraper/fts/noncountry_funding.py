import logging

logger = logging.getLogger(__name__)


class NonCountryFunding:
    """Rows for the global requirements/funding file that are not attributed to a
    single country: funding to non-country locations (Global, regions, FTS "Not
    specified" location, locations HDX does not recognise), funding shared across
    several locations and plan requirements not attributed to any country. See
    docs/decisions/0002-noncountry-rows-global-file-only.md."""

    location_type_order = ("global", "region", "other", "unknown", "multiple")

    def __init__(self, downloader, locations, plans_by_id, multicountry_plans):
        self._downloader = downloader
        self._countryids = {int(country["id"]) for country in locations.countries}
        self._plans_by_id = plans_by_id
        self._multicountry_plans = multicountry_plans

    @staticmethod
    def get_location_type(location_id, name):
        if location_id is None:
            return "unknown"
        if name == "Global":
            return "global"
        if name.startswith("Region - "):
            return "region"
        return "other"

    @staticmethod
    def create_row(location, location_type, year, plan=None, name="Not specified"):
        row = {
            "countryCode": "",
            "location": location,
            "locationType": location_type,
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

    def get_plan(self, planid, year):
        return self._plans_by_id.get((planid, year), self._plans_by_id.get(planid))

    def generate_location_rows(self, location_id, location, location_type, year, total):
        """Plan rows for one non-country location plus a "Not specified" row
        holding the location's single-location funding not in those plan rows,
        as is done for country rows"""
        rows = []
        not_specified_funding = total
        if location_id is not None:
            data = self._downloader.download(
                f"1/fts/flow/custom-search?locationid={location_id}&year={year}&groupby=plan"
            )
            fund_objects = data["report3"]["fundingTotals"]["objects"]
            if fund_objects:
                subrows = []
                for fund_object in fund_objects[0].get("objectsBreakdown", []):
                    planid = fund_object.get("id")
                    funding = fund_object.get("totalFunding")
                    if planid is None or planid == "undefined" or not funding:
                        continue
                    planid = int(planid)
                    plan = self.get_plan(planid, year)
                    if plan is None:
                        logger.warning(
                            f"Plan {planid} for {location} {year} not in plan overview!"
                        )
                        row = self.create_row(
                            location, location_type, year, name=fund_object["name"]
                        )
                        row["id"] = planid
                        row["isPartOfGHO"] = ""
                    else:
                        row = self.create_row(location, location_type, year, plan)
                    row["funding"] = funding
                    not_specified_funding -= funding
                    subrows.append(row)
                rows.extend(sorted(subrows, key=lambda k: k["id"]))
        row = self.create_row(location, location_type, year)
        row["funding"] = not_specified_funding
        rows.append(row)
        return rows

    def generate_year_rows(self, year, country_rows):
        data = self._downloader.download(
            f"1/fts/flow/custom-search?year={year}&groupby=location"
        )
        funding_totals = data["report3"]["fundingTotals"]
        fund_objects = funding_totals["objects"]
        if not fund_objects:
            return []
        rows = []
        for fund_object in fund_objects[0].get("objectsBreakdown", []):
            location_id = fund_object.get("id")
            if location_id is not None:
                location_id = int(location_id)
                if location_id in self._countryids:
                    continue
            funding = fund_object.get("totalFunding")
            if not funding:
                continue
            location = fund_object["name"]
            location_type = self.get_location_type(location_id, location)
            rows.extend(
                self.generate_location_rows(
                    location_id, location, location_type, year, funding
                )
            )
        unattributed_rows, unattributed_funding = self.generate_unattributed_rows(
            year, country_rows + rows
        )
        # Shared funding not already on a plan's "Multiple locations" row
        shared = (fund_objects[0].get("sharedFundingTotal") or 0) - unattributed_funding
        if shared < 0:
            logger.warning(
                f"{year}: unattributed plan funding exceeds shared funding by {-shared}"
            )
        if shared:
            row = self.create_row("Multiple locations", "multiple", year)
            row["funding"] = shared
            rows.append(row)
        rows.extend(unattributed_rows)
        rows = sorted(
            rows,
            key=lambda k: (
                self.location_type_order.index(k["locationType"]),
                k["location"],
            ),
        )
        self.check_total(year, country_rows + rows, funding_totals["total"])
        return rows

    def generate_unattributed_rows(self, year, location_rows):
        """For plans with more or less than 1 country, requirements and funding
        not in location rows go on a "Multiple locations" row for that plan.
        Returns the rows and the total funding put on them."""
        requirements_in_rows = {}
        funding_in_rows = {}
        for row in location_rows:
            planid = row["id"]
            if not planid:
                continue
            if row["requirements"]:
                requirements_in_rows[planid] = (
                    requirements_in_rows.get(planid, 0) + row["requirements"]
                )
            if row["funding"]:
                funding_in_rows[planid] = (
                    funding_in_rows.get(planid, 0) + row["funding"]
                )
        rows = []
        total_funding = 0
        for plan in self._multicountry_plans.get(year, []):
            planid = plan["id"]
            requirements = (plan.get("requirements") or {}).get("revisedRequirements")
            funding = (plan.get("funding") or {}).get("totalFunding")
            requirements = (requirements or 0) - requirements_in_rows.get(planid, 0)
            funding = (funding or 0) - funding_in_rows.get(planid, 0)
            if requirements <= 0 and funding <= 0:
                continue
            row = self.create_row("Multiple locations", "multiple", year, plan)
            if requirements > 0:
                row["requirements"] = requirements
            if funding > 0:
                row["funding"] = funding
                total_funding += funding
                if requirements > 0:
                    row["percentFunded"] = int(funding / requirements * 100 + 0.5)
            rows.append(row)
        return sorted(rows, key=lambda k: k["id"]), total_funding

    @staticmethod
    def check_total(year, rows, fts_total):
        total = sum(row["funding"] for row in rows if row["funding"])
        if fts_total and abs(total - fts_total) > fts_total * 0.01:
            logger.warning(
                f"{year}: total funding in global file {total} differs from FTS total {fts_total}"
            )

    def generate_rows(self, country_rows):
        """Non-country rows for every year in the country rows, latest year
        first"""
        country_rows_by_year = {}
        for row in country_rows:
            country_rows_by_year.setdefault(row["year"], []).append(row)
        rows = []
        for year in sorted(country_rows_by_year, reverse=True):
            rows.extend(self.generate_year_rows(year, country_rows_by_year[year]))
        return rows
