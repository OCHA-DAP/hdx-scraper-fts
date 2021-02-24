import copy
import logging

from fts.helpers import hxl_names

logger = logging.getLogger(__name__)


class RequirementsFundingCovid:
    def __init__(self, downloader, plans_by_year_by_country):
        self.downloader = downloader
        self.covidfundingbyplan = dict()
        self.rows = list()
        self.get_covid_funding(plans_by_year_by_country)

    def clear_rows(self):
        self.rows = list()

    def get_covid_funding(self, plans_by_year_by_country, covidstartyear=2020):
        planids = set()
        for plans_by_year in plans_by_year_by_country.values():
            for year in plans_by_year:
                if year < covidstartyear:
                    continue
                planids.update(str(plan['id']) for plan in plans_by_year[year])
        planids = ','.join(sorted(planids))
        data = self.downloader.download(f'1/fts/flow/custom-search?emergencyid=911&planid={planids}&groupby=plan')
        for fundingobject in data['report3']['fundingTotals']['objects'][0]['singleFundingObjects']:
            self.covidfundingbyplan[fundingobject['id']] = fundingobject['totalFunding']

    def generate_plan_requirements_funding(self, inrow, requirements_clusters):
        planid = inrow['id']
        data = self.downloader.download(f'2/public/governingEntity?planId={planid}&scopes=governingEntityVersion')
        covid_ids = set()
        for clusterobj in data:
            tags = clusterobj['governingEntityVersion'].get('tags')
            if tags and 'COVID-19' in tags:
                covid_ids.add(clusterobj['id'])
        if len(covid_ids) == 0:
            logger.info('%s has no COVID component!' % planid)
            return
        row = copy.deepcopy(inrow)
        covidrequirements = 0
        for clusterid, (_, requirements) in requirements_clusters.items():
            if clusterid in covid_ids:
                covidrequirements += requirements
        row['requirements'] = covidrequirements
        covidfunding = self.covidfundingbyplan.get(planid, 0)
        row['funding'] = covidfunding
        if covidrequirements == 0:
            row['percentFunded'] = ''
        else:
            row['percentFunded'] = int(covidfunding / covidrequirements * 100 + 0.5)
        self.rows.append(row)

    def generate_resource(self, folder, dataset, country):
        if not self.rows:
            return None
        headers = list(self.rows[0].keys())
        filename = f'fts_requirements_funding_covid_{country["iso3"].lower()}.csv'
        resourcedata = {
            'name': filename,
            'description': f'FTS Annual Covid Requirements and Funding Data for {country["name"]}',
            'format': 'csv'
        }
        success, results = dataset.generate_resource_from_iterator(headers, self.rows, hxl_names, folder, filename,
                                                                   resourcedata)
        self.rows = list()
        if success:
            return results['resource']
        else:
            return None
