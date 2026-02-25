import logging

logger = logging.getLogger(__name__)


class ResourceGenerator:
    def __init__(self, downloader, folder):
        self._downloader = downloader
        self._folder = folder
        self._global_rows = {}
        self._filename = ""
        self._description = ""

    def generate_resource(
        self,
        dataset,
        rows,
        countryiso3,
        headers=None,
        countryname=None,
        filename=None,
        description=None,
    ):
        if not headers:
            headers = list(rows[0].keys())
        if not filename:
            filename = f"{self._filename}_{countryiso3.lower()}.csv"
        if countryname:
            countryname = f"for {countryname}"
        else:
            countryname = "globally"
        if not description:
            description = f"{self._description} {countryname}"
        resourcedata = {
            "name": filename.lower(),
            "description": description,
            "format": "csv",
        }
        success, results = dataset.generate_resource(
            self._folder, filename, rows, resourcedata, headers
        )
        return success, results

    def generate_global_resource(self, dataset):
        rows = []
        for countryiso3 in sorted(self._global_rows):
            rows.extend(self._global_rows[countryiso3])
        return self.generate_resource(dataset, rows, "global")
