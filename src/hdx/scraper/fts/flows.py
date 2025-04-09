import logging

from hdx.scraper.fts.helpers import (
    country_all_columns_to_keep,
    funding_hxl_names,
    rename_columns,
)
from hdx.scraper.fts.resource_generator import ResourceGenerator
from hdx.utilities.dateparse import default_enddate, parse_date
from hdx.utilities.dictandlist import dict_of_dicts_add, dict_of_lists_add
from hdx.utilities.matching import multiple_replace

logger = logging.getLogger(__name__)

srcdestmap = {"sourceObjects": "src", "destinationObjects": "dest"}


class Flows(ResourceGenerator):
    def __init__(self, downloader, folder, locations, planidcodemapping, today):
        super().__init__(downloader, folder, funding_hxl_names)
        self._locations = locations
        self._planidcodemapping = planidcodemapping
        self._latestyear = today.year

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
        for objtype in objinfo_by_type:
            prefix = f"{shortened}{objtype}"
            for key in objinfo_by_type[objtype]:
                keyname = f"{prefix}{key.capitalize()}"
                values = objinfo_by_type[objtype][key]
                replacements = {
                    "OrganizationOrganization": "Organization",
                    "Name": "",
                    "types": "Types",
                    "code": "Code",
                }
                keyname = multiple_replace(keyname, replacements)
                if "UsageYear" in keyname:
                    values = sorted(values)
                    newrow[f"{keyname}Start"] = values[0]
                    outputstr = values[-1]
                    keyname = f"{keyname}End"
                elif any(
                    x in keyname for x in ["Cluster", "Location", "OrganizationTypes"]
                ):
                    if keyname[-1] != "s":
                        keyname = f"{keyname}s"
                    if "Location" in keyname:
                        iso3s = []
                        for country in values:
                            iso3 = self._locations.get_countryiso_from_name(country)
                            if iso3:
                                iso3s.append(iso3)
                        values = iso3s
                    outputstr = ",".join(sorted(values))
                else:
                    if len(values) > 1:
                        outputstr = "Multiple"
                        logger.error(
                            f"Multiple used instead of {values} for {keyname} in {plan_id} ({shortened})"
                        )
                    else:
                        outputstr = values[0]
                if keyname in country_all_columns_to_keep:
                    newrow[keyname] = outputstr
        return destPlanId

    def generate_country_resources(self, dataset, country):
        fund_boundaries_info = {}
        fund_data = []
        base_funding_url = f"1/fts/flow/custom-search?locationid={country['id']}&"
        funding_url = self._downloader.get_url(
            f"{base_funding_url}year={self._latestyear}"
        )
        while funding_url:
            json = self._downloader.download(url=funding_url, data=False)
            fund_data.extend(json["data"]["flows"])
            funding_url = json["meta"].get("nextLink")

        start_date = default_enddate
        for row in fund_data:
            newrow = {}
            destPlanId = None
            for key in row:
                if key == "reportDetails":
                    continue
                value = row[key]
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
                        datestr = value[:10]
                        newrow[key] = datestr
                        date = parse_date(datestr)
                        if date < start_date:
                            start_date = date
                    else:
                        newrow[key] = ""
                    continue
                renamed_column = rename_columns.get(key)
                if renamed_column:
                    newrow[renamed_column] = value
                    continue
                if key in country_all_columns_to_keep:
                    newrow[key] = value
            if "originalAmount" not in newrow:
                newrow["originalAmount"] = ""
            if "originalCurrency" not in newrow:
                newrow["originalCurrency"] = ""
            if "refCode" not in newrow:
                newrow["refCode"] = ""
            newrow["destPlanCode"] = self._planidcodemapping.get(destPlanId, "")
            boundary = row["boundary"]
            dict_of_lists_add(fund_boundaries_info, boundary, newrow)

        countryiso3 = country["iso3"]
        resources = []
        for boundary in sorted(fund_boundaries_info):
            rows = sorted(
                fund_boundaries_info[boundary], key=lambda k: k["date"], reverse=True
            )
            filename = f"fts_{boundary}_funding_{countryiso3.lower()}.csv"
            description = f"FTS {boundary.capitalize()} Funding Data for {country['name']} for {self._latestyear}"
            success, results = self.generate_resource(
                dataset,
                rows,
                country["iso3"],
                headers=list(funding_hxl_names.keys()),
                countryname=country["name"],
                filename=filename,
                description=description,
            )
            if success:
                resources.append(results["resource"])
            dict_of_dicts_add(self._global_rows, boundary, countryiso3, rows)
        return resources, start_date

    def generate_global_resources(self, dataset):
        for boundary in sorted(self._global_rows):
            global_rows = self._global_rows[boundary]
            rows = []
            for countryiso3 in sorted(global_rows):
                rows.extend(global_rows[countryiso3])
            filename = f"fts_{boundary}_funding_global.csv"
            description = f"FTS {boundary.capitalize()} Funding Data globally for {self._latestyear}"
            self.generate_resource(
                dataset,
                rows,
                "global",
                headers=list(funding_hxl_names.keys()),
                filename=filename,
                description=description,
            )
