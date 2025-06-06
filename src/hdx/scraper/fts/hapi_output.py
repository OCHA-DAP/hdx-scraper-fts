from logging import getLogger

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import (
    iso_string_from_datetime,
    parse_date,
    parse_date_range,
)

logger = getLogger(__name__)


class HAPIOutput:
    def __init__(self, configuration, error_handler, global_results, today, folder):
        self._configuration = configuration
        self._error_handler = error_handler
        self._temp_dir = folder
        self._today = today
        self._global_results = global_results

    def generate_dataset(self) -> Dataset:
        dataset = Dataset(self._configuration["hapi_dataset"])

        global_data = []
        duplicate_checks = []
        start_dates = []

        global_dataset = self._global_results["dataset"]
        dataset_id = global_dataset["id"]
        dataset_name = global_dataset["name"]
        global_resource = self._global_results["resource"]
        resource_name = global_resource["name"]
        resource_id = None
        for resource in global_dataset.get_resources():
            if resource["name"] == resource_name:
                resource_id = resource["id"]
                break
        for row in self._global_results["rows"]:
            countryiso3 = row["countryCode"]
            if countryiso3[0] == "#":
                continue
            errors = []

            row["location_code"] = countryiso3
            row["has_hrp"] = (
                "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
            )
            row["in_gho"] = (
                "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"
            )

            appeal_code = row.get("code")
            if not appeal_code:
                appeal_code = "Not specified"
            row["appeal_code"] = appeal_code

            row["appeal_name"] = row.get("name")
            row["appeal_type"] = row.get("typeName")
            row["requirements_usd"] = row.get("requirements")

            funding = row.get("funding")
            if not funding:
                funding = 0
            if funding < 0:
                self._error_handler.add_message(
                    "Funding",
                    dataset_name,
                    f"Negative funding value found for {countryiso3}",
                    resource_name=resource_name,
                    err_to_hdx=True,
                )
                errors.append("Negative funding value")
            row["funding_usd"] = funding

            funding_pct = row.get("percentFunded")
            if not funding_pct and row.get("requirements"):
                funding_pct = 0
            row["funding_pct"] = funding_pct

            if row.get("startDate"):
                start_date = parse_date(row["startDate"])
                end_date = parse_date(row["endDate"])
                if start_date > end_date:
                    self._error_handler.add_message(
                        "Funding",
                        dataset_name,
                        f"Start date occurs after end date for {countryiso3}",
                        resource_name=resource_name,
                        err_to_hdx=True,
                    )
                    errors.append("Start date occurs after end date")
            else:
                start_date, end_date = parse_date_range(str(row["year"]))
            start_dates.append(start_date)
            row["reference_period_start"] = iso_string_from_datetime(start_date)
            row["reference_period_end"] = iso_string_from_datetime(end_date)

            row["dataset_hdx_id"] = dataset_id
            row["resource_hdx_id"] = resource_id

            duplicate_check = (countryiso3, row["appeal_code"], start_date)
            if duplicate_check in duplicate_checks:
                self._error_handler.add_message(
                    "Funding",
                    dataset_name,
                    f"Duplicate row found for {countryiso3}",
                    resource_name=resource_name,
                    err_to_hdx=True,
                )
                errors.append("Duplicate row")
            else:
                duplicate_checks.append(duplicate_check)

            row["error"] = "|".join(errors)
            global_data.append(row)

        start_date = min(start_dates)
        dataset.set_time_period(start_date, self._today)

        tags = ["funding", "hxl", "humanitarian financial tracking service-fts"]
        dataset.add_tags(tags)

        dataset.add_other_location("world")

        hxl_tags = self._configuration["hapi_hxl_tags"]
        headers = list(hxl_tags.keys())
        dataset.generate_resource_from_iterable(
            headers,
            global_data,
            hxl_tags,
            self._temp_dir,
            "hdx_hapi_funding_global.csv",
            self._configuration["hapi_resource"],
            encoding="utf-8-sig",
        )
        return dataset
