import logging

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import multiple_replace

from fts.helpers import country_all_columns_to_keep, funding_hxl_names, rename_columns

logger = logging.getLogger(__name__)

srcdestmap = {"sourceObjects": "src", "destinationObjects": "dest"}


class Flows:
    def __init__(self, downloader, locations, planidcodemapping):
        self.downloader = downloader
        self.locations = locations
        self.planidcodemapping = planidcodemapping

    def flatten_objects(self, objs, shortened, newrow):
        objinfo_by_type = dict()
        plan_id = None
        destPlanId = None
        for obj in objs:
            objtype = obj["type"]
            objinfo = objinfo_by_type.get(objtype, dict())
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
                        iso3s = list()
                        for country in values:
                            iso3 = self.locations.get_countryiso_from_name(country)
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

    def generate_resources(self, folder, dataset, latestyear, country):
        fund_boundaries_info = dict()
        fund_data = list()
        base_funding_url = f'1/fts/flow/custom-search?locationid={country["id"]}&'
        funding_url = self.downloader.get_url(f"{base_funding_url}year={latestyear}")
        while funding_url:
            json = self.downloader.download(url=funding_url, data=False)
            fund_data.extend(json["data"]["flows"])
            funding_url = json["meta"].get("nextLink")

        for row in fund_data:
            newrow = dict()
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
                        newrow[key] = value[:10]
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
            newrow["destPlanCode"] = self.planidcodemapping.get(destPlanId, "")
            boundary = row["boundary"]
            rows = fund_boundaries_info.get(boundary, list())
            rows.append(newrow)
            fund_boundaries_info[boundary] = rows

        resources = list()
        for boundary in sorted(fund_boundaries_info.keys()):
            rows = sorted(
                fund_boundaries_info[boundary], key=lambda k: k["date"], reverse=True
            )
            headers = list(funding_hxl_names.keys())
            filename = f'fts_{boundary}_funding_{country["iso3"].lower()}.csv'
            resourcedata = {
                "name": filename,
                "description": f'FTS {boundary.capitalize()} Funding Data for {country["name"]} for {latestyear}',
                "format": "csv",
            }
            success, results = dataset.generate_resource_from_iterator(
                headers, rows, funding_hxl_names, folder, filename, resourcedata
            )
            if success:
                resources.append(results["resource"])
        return resources
