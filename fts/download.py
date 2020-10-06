class FTSException(Exception):
    pass


class FTSDownload:
    def __init__(self, configuration, downloader):
        self.v1_url = configuration['v1_url']
        self.v2_url = configuration['v2_url']
        self.downloader = downloader

    def get_url(self, partial_url, use_v2=False):
        if use_v2:
            return f'{self.v2_url}{partial_url}'
        else:
            return f'{self.v1_url}{partial_url}'

    def download(self, partial_url=None, use_v2=False, url=None):
        if partial_url is not None:
            url = self.get_url(partial_url, use_v2=use_v2)
        r = self.downloader.download(url)
        json = r.json()
        status = json['status']
        if status != 'ok':
            raise FTSException(f'{url} gives status {status}')
        return json

    def download_data(self, partial_url=None, use_v2=False, url=None):
        return self.download(partial_url, use_v2, url)['data']


