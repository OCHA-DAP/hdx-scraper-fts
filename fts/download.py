import copy
from os.path import join, basename
from urllib.parse import urlsplit

from hdx.utilities.saver import save_json
from slugify import slugify


class FTSException(Exception):
    pass


class FTSDownload:
    def __init__(self, configuration, downloader, countryisos=None, years=None, testfolder=None, testpath=False):
        self.v1_url = configuration['v1_url']
        self.v2_url = configuration['v2_url']
        self.test_url = configuration['test_url']
        self.downloader = downloader
        if countryisos:
            self.countryisos = countryisos.split(',')
        else:
            self.countryisos = None
        if years:
            self.years = years.split(',')
        else:
            self.years = None
        self.testfolder = testfolder
        self.testpath = testpath

    def get_url(self, partial_url, use_v2=False):
        if use_v2:
            return f'{self.v2_url}{partial_url}'
        else:
            return f'{self.v1_url}{partial_url}'

    @staticmethod
    def get_testfile_path(partial_url=None, url=None):
        if partial_url:
            filename = slugify(partial_url)
        else:
            split = urlsplit(url)
            filename = f'{basename(split.path)}'
            if split.query:
                filename = f'{filename}_{split.query}'
        if filename[-5:] != '.json':
            filename = f'{filename}.json'
        return filename

    def download(self, partial_url=None, data=True, use_v2=False, url=None):
        if self.testpath:
            partial_url = self.get_testfile_path(partial_url, url)
        if partial_url is not None:
            url = self.get_url(partial_url, use_v2=use_v2)
        r = self.downloader.download(url)
        origjson = r.json()
        status = origjson['status']
        if status != 'ok':
            raise FTSException(f'{url} gives status {status}')
        save = True
        if data:
            json = origjson['data']
            if isinstance(json, dict):
                try:
                    # We never use these
                    del json['report1']
                    del json['report2']
                    del json['report4']
                except KeyError:
                    pass
                plans = json.get('plans')
                if plans is not None:
                    for i, plan in reversed(list(enumerate(plans))):
                        delete = False
                        if self.countryisos:
                            countries = plan['countries']
                            if countries:
                                delete = True
                                for country in countries:
                                    if country['iso3'] in self.countryisos:
                                        delete = False
                                        break
                        if not delete and self.years:
                            delete = True
                            for year in plan['usageYears']:
                                if year['year'] in self.years:
                                    delete = False
                                    break
                        if delete:
                            del plans[i]
                    if len(plans) == 0:
                        save = False
            elif self.countryisos or self.years:
                for i, object in reversed(list(enumerate(json))):
                    if 'iso3' in object:
                        countryiso = object['iso3']
                        if countryiso is None or countryiso not in self.countryisos:
                            del json[i]
                            continue
                    if 'year' in object:
                        year = str(object['year'])
                        if year is None or year not in self.years:
                            del json[i]
        else:
            json = origjson
        if save and self.testfolder and json:
            filename = self.get_testfile_path(partial_url, url)
            filepath = join(self.testfolder, filename)
            meta = origjson.get('meta')
            if meta:
                nextlink = meta.get('nextLink')
            else:
                nextlink = None
            if nextlink:
                nextname = self.get_testfile_path(None, nextlink)
                meta['nextLink'] = f'{self.test_url}{nextname}'
                save_json(origjson, filepath)
                meta['nextLink'] = nextlink
            else:
                save_json(origjson, filepath)
        return json


