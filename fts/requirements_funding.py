import logging

from fts.helpers import hxl_names

logger = logging.getLogger(__name__)


class RequirementsFunding:
    def __init__(self, downloader, locations, globalplanids, today):
        self.downloader = downloader
        self.locations = locations
        self.globalplanids = globalplanids
        self.today = today

    def add_country_requirements_funding(self, planid, plan, countries):
        if len(countries) == 1:
            requirements = plan.get('requirements')
            if requirements is not None:
                requirements = requirements.get('revisedRequirements')
            funding = plan.get('funding')
            if funding is None:
                progress = None
            else:
                progress = funding.get('progress')
                funding = funding.get('totalFunding')
            countries[0]['requirements'] = requirements
            countries[0]['funding'] = funding
            if progress:
                progress = int(progress + 0.5)
            countries[0]['percentFunded'] = progress
        else:
            if plan.get('customLocationCode') == 'COVD':
                return True
            funding_url = f'1/fts/flow/custom-search?planid={planid}&groupby=location'
            data = self.downloader.download(funding_url)
            requirements = data.get('requirements')
            country_requirements = dict()
            if requirements is not None:
                totalreq = requirements['totalRevisedReqs']
                countryreq_is_totalreq = True
                for req_object in requirements.get('objects', list()):
                    country_id = self.locations.get_countryid_from_object(req_object)
                    country_req = req_object.get('revisedRequirements')
                    if country_id is not None and country_req is not None:
                        country_requirements[country_id] = country_req
                        if country_req != totalreq:
                            countryreq_is_totalreq = False
                if countryreq_is_totalreq:
                    logger.info('%s has same country requirements as total requirements!' % planid)
                    return True
            fund_objects = data['report3']['fundingTotals']['objects']
            country_funding = dict()
            if len(fund_objects) == 1:
                for fund_object in fund_objects[0].get('objectsBreakdown', list()):
                    country_id = self.locations.get_countryid_from_object(fund_object)
                    country_fund = fund_object.get('totalFunding')
                    if country_id is not None and country_fund is not None:
                        country_funding[int(country_id)] = country_fund
            for country in countries:
                countryid = country['id']
                requirements = country_requirements.get(countryid)
                country['requirements'] = requirements
                funding = country_funding.get(countryid)
                country['funding'] = funding
                if requirements is not None and funding is not None:
                    country['percentFunded'] = int(funding / requirements * 100 + 0.5)
        return False

    def get_country_funding(self, countryid, plans_by_year, start_year=2010):
        funding_by_year = dict()
        if plans_by_year is not None:
            start_year = sorted(plans_by_year.keys())[0]
        for year in range(self.today.year + 5, start_year - 5, -11):
            data = self.downloader.download(f'2/country/{countryid}/summary/trends/{year}')
            for object in data:
                year = object['year']
                funding = object['totalFunding']
                if funding:
                    funding_by_year[year] = funding
        return funding_by_year

    def generate_resource(self, folder, dataset, plans_by_year, country, call_others=lambda x: None):
        countryiso = country['iso3']
        funding_by_year = self.get_country_funding(country['id'], plans_by_year)
        rows = list()

        all_years = sorted(set(plans_by_year.keys()) | set(funding_by_year.keys()), reverse=True)
        for year in all_years:
            not_specified_funding = funding_by_year.get(year, '')
            subrows = list()
            for plan in plans_by_year.get(year, list()):
                planid = plan['id']
                if planid in self.globalplanids:
                    continue
                found_other_countries = False
                for country in plan['countries']:
                    adminlevel = country.get('adminlevel', country.get('adminLevel'))
                    if adminlevel == 0 and country['iso3'] != countryiso:
                        found_other_countries = True
                        continue
                    requirements = country.get('requirements', '')
                    funding = country.get('funding', '')
                    percentFunded = country.get('percentFunded', '')
                    if not_specified_funding and funding:
                        not_specified_funding -= funding
                    row = {'countryCode': countryiso, 'id': planid, 'name': plan['name'], 'code': plan['code'],
                           'typeId': plan['planType']['id'], 'typeName': plan['planType']['id'],
                           'startDate': plan['startDate'], 'endDate': plan['endDate'], 'year': year,
                           'requirements': requirements, 'funding': funding, 'percentFunded': percentFunded}
                    subrows.append(row)

                if found_other_countries:
                    logger.warning('Plan %s spans multiple locations - ignoring in cluster breakdown!' % planid)
                    continue
            for row in sorted(subrows, key=lambda k: (k['typeId'], k['id'])):
                rows.append(row)
                call_others(row)

            rows.append({'countryCode': countryiso, 'id': '', 'name': 'Not specified', 'code': '', 'typeId': '',
                         'typeName': '', 'startDate': '', 'endDate': '', 'year': year, 'requirements': '',
                         'funding': not_specified_funding, 'percentFunded': ''})
        if not rows:
            return None
        headers = list(rows[0].keys())
        filename = f'fts_requirements_funding_{countryiso.lower()}.csv'
        resourcedata = {
            'name': filename.lower(),
            'description': f'FTS Annual Requirements and Funding Data for {country["name"]}',
            'format': 'csv'
        }
        success, results = dataset.generate_resource_from_iterator(headers, rows, hxl_names, folder, filename, resourcedata)
        return results['resource']
