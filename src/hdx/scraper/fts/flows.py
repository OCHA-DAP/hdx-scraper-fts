import logging
import sys
from collections import Counter

from hdx.utilities.dateparse import default_enddate, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.matching import multiple_replace

from hdx.scraper.fts.resource_generator import ResourceGenerator

logger = logging.getLogger(__name__)

srcdestmap = {"sourceObjects": "src", "destinationObjects": "dest"}


def sort_rows(rows):
    rows.sort(key=lambda k: (k["date"], int(k["id"])), reverse=True)


def earliest_date(rows, start_year):
    # Dates are YYYY-MM-DD so compare as strings and parse only the earliest.
    # Dates before start_year are ignored: flows 148776 and 147653 (2016) have
    # date 1900-01-01
    start = str(start_year)
    date = min((row["date"] for row in rows if row["date"] >= start), default=None)
    return parse_date(date) if date else default_enddate


class Flows(ResourceGenerator):
    def __init__(
        self,
        configuration,
        downloader,
        folder,
        locations,
        planidcodemapping,
        today,
        start_year=1990,
    ):
        super().__init__(downloader, folder)
        self._configuration = configuration
        self._locations = locations
        self._id_to_iso3 = locations.get_id_to_iso3()
        self._planidcodemapping = planidcodemapping
        self._today = today
        self._start_year = start_year
        self._all_rows = []
        self._country_rows = {}
        self._multiple_counts = Counter()
        # One str object per distinct value, shared by all rows
        self._values = {}
        # Mostly distinct per flow so sharing would save little
        self._not_shared = ("description", "refCode")

    def flatten_objects(self, objs, shortened, newrow):
        objinfo_by_type = {}
        plan_id = None
        destPlanId = None
        for obj in objs:
            objtype = obj["type"]
            objinfo = objinfo_by_type.get(objtype, {})
            for key in obj:
                if objtype == "Plan" and key == "id":
                    plan_id = obj[key]
                    dict_of_lists_add(objinfo, key, plan_id)
                    if shortened == "dest":
                        destPlanId = plan_id
                if key not in ["type", "behavior", "id"]:
                    value = obj[key]
                    if isinstance(value, list):
                        for element in value:
                            dict_of_lists_add(objinfo, key, element)
                    else:
                        dict_of_lists_add(objinfo, key, value)
            objinfo_by_type[objtype] = objinfo
        for objtype, objinfo in objinfo_by_type.items():
            prefix = f"{shortened}{objtype}"
            for key, values in objinfo.items():
                keyname = f"{prefix}{key.capitalize()}"
                replacements = {
                    "OrganizationOrganization": "Organization",
                    "Name": "",
                    "types": "Types",
                    "code": "Code",
                }
                keyname = multiple_replace(keyname, replacements)
                multiple = False
                if "UsageYear" in keyname:
                    values = sorted(values)
                    newrow[sys.intern(f"{keyname}Start")] = values[0]
                    outputstr = values[-1]
                    keyname = f"{keyname}End"
                elif any(
                    x in keyname for x in ["Cluster", "Location", "OrganizationTypes"]
                ):
                    if keyname[-1] != "s":
                        keyname = f"{keyname}s"
                    if "Location" in keyname:
                        # Global and regions have no ISO3 so keep their names
                        locations = []
                        for location in values:
                            location = self._locations.get_iso3_or_noncountry_name(
                                location
                            )
                            if location:
                                locations.append(location)
                        values = locations
                    outputstr = ",".join(sorted(values))
                else:
                    if len(values) > 1:
                        outputstr = "Multiple"
                        multiple = True
                    else:
                        outputstr = values[0]
                if keyname in self._configuration["country_all_columns_to_keep"]:
                    # Interned so the ~500k rows share one copy of each key
                    newrow[sys.intern(keyname)] = outputstr
                    if multiple:
                        self._multiple_counts[keyname] += 1
        return destPlanId

    def get_flows(self):
        """Download flows year by year, keeping each flow once. A flow's usage
        years can span several years. Years with no flows are skipped until
        the current year, after which two empty years in a row end the loop."""
        seen_ids = set()
        year = self._start_year
        empty_years = 0
        while True:
            count = 0
            funding_url = self._downloader.get_url(
                f"1/fts/flow/custom-search?year={year}&limit=1000"
            )
            while funding_url:
                json = self._downloader.download(url=funding_url, data=False)
                for flow in json["data"]["flows"]:
                    count += 1
                    flowid = flow["id"]
                    if flowid in seen_ids:
                        continue
                    seen_ids.add(flowid)
                    self.add_flow(flow)
                funding_url = json["meta"].get("nextLink")
            logger.info(f"{count} flows for {year}")
            if count == 0 and year > self._today.year:
                empty_years += 1
                if empty_years == 2:
                    break
            else:
                empty_years = 0
            year += 1
        # Rows keep the shared strings; only the lookup dict is freed
        self._values.clear()
        if self._multiple_counts:
            counts = ", ".join(
                f"{keyname}: {count}"
                for keyname, count in sorted(self._multiple_counts.items())
            )
            logger.warning(f"Flows with Multiple instead of several values: {counts}")

    @staticmethod
    def get_location_ids(objs):
        return {int(obj["id"]) for obj in objs if obj["type"] == "Location"}

    def add_flow(self, flow):
        newrow = {}
        destPlanId = None
        for key in flow:
            # boundary and onBoundary are relative to the query (year here), so
            # they are recalculated per country below
            if key in ("reportDetails", "boundary", "onBoundary"):
                continue
            value = flow[key]
            shortened = srcdestmap.get(key)
            if shortened:
                newdestPlanId = self.flatten_objects(value, shortened, newrow)
                if newdestPlanId:
                    destPlanId = int(newdestPlanId)
                continue
            if key == "keywords":
                if value:
                    newrow[key] = ",".join(value)
                else:
                    newrow[key] = ""
                continue
            if key in [
                "date",
                "firstReportedDate",
                "decisionDate",
                "createdAt",
                "updatedAt",
            ]:
                if value:
                    newrow[key] = value[:10]
                else:
                    newrow[key] = ""
                continue
            renamed_column = self._configuration["rename_columns"].get(key)
            if renamed_column:
                newrow[renamed_column] = value
                continue
            if key in self._configuration["country_all_columns_to_keep"]:
                newrow[key] = value
        for key in ("originalAmount", "originalCurrency", "refCode"):
            newrow.setdefault(key, "")
        newrow["destPlanCode"] = self._planidcodemapping.get(destPlanId, "")
        for key, value in newrow.items():
            if isinstance(value, str) and key not in self._not_shared:
                newrow[key] = self._values.setdefault(value, value)
        self._all_rows.append(newrow)

        # Rules match FTS's custom-search?locationid= boundary and onBoundary
        src_ids = self.get_location_ids(flow["sourceObjects"])
        dest_ids = self.get_location_ids(flow["destinationObjects"])
        if len(src_ids) > 1 or len(dest_ids) > 1:
            onboundary = "shared"
        else:
            onboundary = "single"
        for locationid in src_ids | dest_ids:
            countryiso3 = self._id_to_iso3.get(locationid)
            if not countryiso3:
                continue
            if locationid not in src_ids:
                boundary = "incoming"
            elif locationid not in dest_ids:
                boundary = "outgoing"
            elif flow["newMoney"]:
                boundary = "incoming"
            else:
                boundary = "internal"
            dict_of_lists_add(
                self._country_rows, countryiso3, (boundary, onboundary, newrow)
            )

    def generate_country_resources(self, dataset, country):
        countryiso3 = country["iso3"]
        fund_boundaries_info = {}
        country_rows = self._country_rows.pop(countryiso3, [])
        start_date = earliest_date(
            (row for _, _, row in country_rows), self._start_year
        )
        for boundary, onboundary, row in country_rows:
            newrow = row | {"boundary": boundary, "onBoundary": onboundary}
            dict_of_lists_add(fund_boundaries_info, boundary, newrow)

        resources = []
        for boundary in sorted(fund_boundaries_info):
            rows = fund_boundaries_info[boundary]
            sort_rows(rows)
            filename = f"fts_{boundary}_funding_{countryiso3.lower()}.csv"
            description = f"FTS {boundary.capitalize()} Funding Data for {country['name']} for all years"
            success, results = self.generate_resource(
                dataset,
                rows,
                country["iso3"],
                headers=self._configuration["plans_headers"],
                countryname=country["name"],
                filename=filename,
                description=description,
            )
            if success:
                resources.append(results["resource"])
        return resources, start_date

    def generate_global_resource(self, dataset):
        """One file of every flow, including flows to no country (Global,
        regions, no location). It has no boundary or onBoundary columns as
        those are relative to a country."""
        # Rows of countries with no dataset or skipped on resume
        self._country_rows.clear()
        rows = self._all_rows
        sort_rows(rows)
        headers = [
            header
            for header in self._configuration["plans_headers"]
            if header not in ("boundary", "onBoundary")
        ]
        self.generate_resource(
            dataset,
            rows,
            "global",
            headers=headers,
            filename="fts_funding_global.csv",
            description="FTS Funding Data globally for all years",
        )
