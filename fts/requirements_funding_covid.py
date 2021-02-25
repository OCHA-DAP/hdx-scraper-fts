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
        for fundingobject in data['report3']['fundingTotals']['objects'][0]['objectsBreakdown']:
            self.covidfundingbyplan[int(fundingobject['id'])] = fundingobject['totalFunding']

    def generate_plan_funding(self, inrow):
        planid = inrow['id']
        covidfunding = self.covidfundingbyplan.get(planid)
        if covidfunding is None:
            logger.info(f'{planid} has no COVID component!')
            return
        row = copy.deepcopy(inrow)
        del row['percentFunded']
        row['covidFunding'] = covidfunding
        row['covidPercentageOfFunding'] = int(covidfunding / inrow['funding'] * 100 + 0.5)
        self.rows.append(row)

    def generate_resource(self, folder, dataset, country):
        if not self.rows:
            return None
        headers = list(self.rows[0].keys())
        filename = f'fts_requirements_funding_covid_{country["iso3"].lower()}.csv'
        resourcedata = {
            'name': filename,
            'description': f'FTS Annual Requirements, Funding and Covid Funding Data for {country["name"]}',
            'format': 'csv'
        }
        success, results = dataset.generate_resource_from_iterator(headers, self.rows, hxl_names, folder, filename,
                                                                   resourcedata)
        self.rows = list()
        if success:
            return results['resource']
        else:
            return None
