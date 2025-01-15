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
        self.url = configuration["base_url"]
        self.test_url = configuration["test_url"]
        self.downloader = downloader
        if countryiso3s:
            self.countryiso3s = countryiso3s.split(",")
        else:
            self.countryiso3s = None
        if years:
            self.years = years.split(",")
        else:
            self.years = None
        self.testfolder = testfolder
        self.testpath = testpath

    def get_url(self, partial_url):
        return f"{self.url}{partial_url}"

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
        if self.testpath:
            partial_url = self.get_testfile_path(partial_url, url)
        if partial_url is not None:
            url = self.get_url(partial_url)
        r = self.downloader.download(url)
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
                        if self.countryiso3s:
                            countries = plan["countries"]
                            if countries:
                                delete = True
                                for country in countries:
                                    if country["iso3"] in self.countryiso3s:
                                        delete = False
                                        break
                        if not delete and self.years:
                            delete = True
                            for year in plan["usageYears"]:
                                if year["year"] in self.years:
                                    delete = False
                                    break
                        if delete:
                            del plans[i]
                    if len(plans) == 0:
                        save = False
            elif self.countryiso3s or self.years:
                for i, object in reversed(list(enumerate(json))):
                    if "iso3" in object:
                        countryiso3 = object["iso3"]
                        if countryiso3 is None or countryiso3 not in self.countryiso3s:
                            del json[i]
                            continue
                    if "year" in object:
                        year = str(object["year"])
                        if year is None or year not in self.years:
                            del json[i]
        else:
            json = origjson
        if save and self.testfolder and json:
            filename = self.get_testfile_path(partial_url, url)
            filepath = join(self.testfolder, filename)
            meta = origjson.get("meta")
            if meta:
                nextlink = meta.get("nextLink")
            else:
                nextlink = None
            if nextlink:
                nextname = self.get_testfile_path(None, nextlink)
                meta["nextLink"] = f"{self.test_url}{nextname}"
                save_json(origjson, filepath)
                meta["nextLink"] = nextlink
            else:
                save_json(origjson, filepath)
        return json
