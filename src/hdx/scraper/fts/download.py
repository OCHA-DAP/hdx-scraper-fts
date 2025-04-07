from os.path import basename, join
from urllib.parse import urlsplit

from hdx.utilities.saver import save_json
from slugify import slugify


class FTSException(Exception):
    pass


class FTSDownload:
    def __init__(
        self,
        configuration,
        downloader,
        countryiso3s=None,
        years=None,
        testfolder=None,
        testpath=False,
    ):
        self._url = configuration["base_url"]
        self._test_url = configuration["test_url"]
        self._downloader = downloader
        if countryiso3s:
            self._countryiso3s = countryiso3s.split(",")
        else:
            self._countryiso3s = None
        if years:
            self._years = years.split(",")
        else:
            self._years = None
        self._testfolder = testfolder
        self._testpath = testpath

    def get_url(self, partial_url):
        return f"{self._url}{partial_url}"

    @staticmethod
    def get_testfile_path(partial_url=None, url=None):
        if partial_url:
            filename = partial_url
        else:
            split = urlsplit(url)
            filename = f"{basename(split.path)}"
            if split.query:
                filename = f"{filename}_{split.query}"
        filename = slugify(filename)
        extension = filename[-4:]
        if extension == "json":
            dot = filename[-5]
            if dot != ".":
                filename = filename.replace(f"{dot}json", ".json")
        else:
            filename = f"{filename}.json"
        return filename

    def download(self, partial_url=None, data=True, url=None):
        if self._testpath:
            partial_url = self.get_testfile_path(partial_url, url)
        if partial_url is not None:
            url = self.get_url(partial_url)
        r = self._downloader.download(url)
        origjson = r.json()
        status = origjson["status"]
        if status != "ok":
            raise FTSException(f"{url} gives status {status}")
        save = True
        if data:
            json = origjson["data"]
            if isinstance(json, dict):
                try:
                    # We never use these
                    del json["report1"]
                    del json["report2"]
                    del json["report4"]
                except KeyError:
                    pass
                plans = json.get("plans")
                if plans is not None:
                    for i, plan in reversed(list(enumerate(plans))):
                        delete = False
                        if self._countryiso3s:
                            countries = plan["countries"]
                            if countries:
                                delete = True
                                for country in countries:
                                    if country["iso3"] in self._countryiso3s:
                                        delete = False
                                        break
                        if not delete and self._years:
                            delete = True
                            for year in plan["usageYears"]:
                                if year["year"] in self._years:
                                    delete = False
                                    break
                        if delete:
                            del plans[i]
                    if len(plans) == 0:
                        save = False
            elif self._countryiso3s or self._years:
                for i, object in reversed(list(enumerate(json))):
                    if "iso3" in object:
                        countryiso3 = object["iso3"]
                        if countryiso3 is None or countryiso3 not in self._countryiso3s:
                            del json[i]
                            continue
                    if "year" in object:
                        year = str(object["year"])
                        if year is None or year not in self._years:
                            del json[i]
        else:
            json = origjson
        if save and self._testfolder and json:
            filename = self.get_testfile_path(partial_url, url)
            filepath = join(self._testfolder, filename)
            meta = origjson.get("meta")
            if meta:
                nextlink = meta.get("nextLink")
            else:
                nextlink = None
            if nextlink:
                nextname = self.get_testfile_path(None, nextlink)
                meta["nextLink"] = f"{self._test_url}{nextname}"
                save_json(origjson, filepath)
                meta["nextLink"] = nextlink
            else:
                save_json(origjson, filepath)
        return json
