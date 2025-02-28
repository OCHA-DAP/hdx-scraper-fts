from logging import getLogger

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date, parse_date_range
from slugify import slugify

logger = getLogger(__name__)


class HAPIOutput:
    def __init__(self, configuration, global_rows, today, folder):
        self._configuration = configuration
        self._temp_dir = folder
        self._today = today
        self.global_rows = global_rows

    def generate_dataset(self) -> Dataset:
        dataset = Dataset(
            self._configuration["hapi_dataset"]
        )

        global_data = []
        duplicate_checks = []
        start_dates = []
        for countryname, rows in self.global_rows.items():
            dataset_name = slugify(
                f"FTS Requirements and Funding Data for {countryname}"
            ).lower()
            country_dataset = Dataset.read_from_hdx(dataset_name)
            resources = country_dataset.get_resources()

            for row in rows:
                error = []
                countryiso3 = row["countryCode"]
                row["location_code"] = countryiso3

                row["has_hrp"] = (
                    "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
                )
                row["in_gho"] = (
                    "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"
                )

                row["appeal_code"] = row.get("code")
                row["appeal_name"] = row.get("name")
                row["appeal_type"] = row.get("typeName")
                row["requirements_usd"] = row.get("requirements")

                funding = row.get("funding")
                if funding is None:
                    funding = 0
                if funding < 0:
                    logger.error(f"Negative funding value found for {countryiso3}")
                    error = "Negative funding value"
                row["funding_usd"] = funding

                funding_pct = row.get("percentFunded")
                if funding_pct is None and row.get("requirements") is not None:
                    funding_pct = 0
                row["funding_pct"] = funding_pct

                if row.get("startDate"):
                    start_date = parse_date(row["startDate"])
                    end_date = parse_date(row["endDate"])
                    if start_date > end_date:
                        error.append("Start date occurs after end date")
                        logger.error(f"Start date occurs after end date for {countryiso3}")
                else:
                    start_date, end_date = parse_date_range(str(row["year"]))
                start_dates.append(start_date)
                row["reference_period_start"] = iso_string_from_datetime(start_date)
                row["reference_period_end"] = iso_string_from_datetime(end_date)

                row["dataset_hdx_id"] = country_dataset["id"]
                resource_name = f"fts_requirements_funding_{countryiso3.lower()}.csv"
                resource = [r for r in resources if r["name"] == resource_name]
                row["resource_hdx_id"] = resource[0]["id"]

                duplicate_check = (countryiso3, row["appeal_code"], start_date)
                if duplicate_check in duplicate_checks:
                    error.append("Duplicate row")
                    logger.error(f"Duplicate row found for {countryiso3}")
                else:
                    duplicate_checks.append(duplicate_check)

                row["error"] = "|".join(error)
                global_data.append(row)

        start_date = min(start_dates)
        dataset.set_time_period(start_date, self._today)

        tags = ["funding", "hxl"]
        dataset.add_tags(tags)

        dataset.add_other_location("world")

        hxl_tags = self._configuration["hapi_hxl_tags"]
        headers = list(hxl_tags.keys())
        dataset.generate_resource_from_iterable(
            headers,
            global_data,
            hxl_tags,
            self._temp_dir,
            f"hdx_hapi_funding_global.csv",
            self._configuration["hapi_resource"],
            encoding="utf-8-sig",
        )
        return dataset
