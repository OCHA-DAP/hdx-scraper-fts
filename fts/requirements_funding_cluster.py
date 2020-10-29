import copy
import logging

from hdx.utilities.downloader import DownloadError

from fts.helpers import hxl_names

logger = logging.getLogger(__name__)


class RequirementsFundingCluster:
    def __init__(self, downloader, locations, planidswithonelocation, clusterlevel=''):
        self.downloader = downloader
        self.locations = locations
        self.planidswithonelocation = planidswithonelocation
        self.clusterlevel = clusterlevel
        self.rows = list()

    def get_requirements_funding_plan(self, inrow):
        planid = inrow['id']
        try:
            data = self.downloader.download(f'fts/flow?planid={planid}&groupby={self.clusterlevel}cluster')
        except DownloadError:
            logger.error(f'Problem with downloading cluster data for {planid}!')
            return None, None, None, None
        requirements_clusters = dict()
        for reqobject in data['requirements']['objects']:
            requirements = reqobject.get('revisedRequirements')
            if requirements is not None:
                clusterid = reqobject.get('id')
                if clusterid is not None:
                    requirements_clusters[clusterid] = (reqobject['name'], requirements)
        funding_clusters = dict()
        fund_objects = data['report3']['fundingTotals']['objects']
        notspecified = None
        if len(fund_objects) == 0:
            logger.warning(f'{planid} has no funding objects!')
            shared = None
        else:
            for fundobject in fund_objects[0]['objectsBreakdown']:
                funding = fundobject.get('totalFunding')
                if funding is not None:
                    clusterid = fundobject.get('id')
                    if clusterid is None or clusterid == 'undefined':
                        notspecified = funding
                    else:
                        clusterid = int(clusterid)
                        funding_clusters[clusterid] = (fundobject['name'], funding)
            shared = fund_objects[0]['totalBreakdown']['sharedFunding']
        return requirements_clusters, funding_clusters, notspecified, shared

    @staticmethod
    def create_row(base_row, clusterid='', name='', requirements='', funding='', percentFunded=''):
        row = copy.deepcopy(base_row)
        row['clusterCode'] = clusterid
        row['cluster'] = name
        row['requirements'] = requirements
        row['funding'] = funding
        row['percentFunded'] = percentFunded
        return row

    def generate_rows_requirements_funding(self, inrow, requirements_clusters, funding_clusters, notspecified, shared):
        if requirements_clusters is None and funding_clusters is None:
            return
        planid = inrow['id']
        if planid not in self.planidswithonelocation:
            return
        base_row = copy.deepcopy(inrow)
        del base_row['typeId']
        del base_row['typeName']
        del base_row['requirements']
        del base_row['funding']
        del base_row['percentFunded']
        subrows = list()
        for clusterid, (fundname, funding) in funding_clusters.items():
            requirements_cluster = requirements_clusters.get(clusterid)
            if requirements_cluster is None:
                requirements = ''
            else:
                reqname, requirements = requirements_cluster
                if not fundname:
                    fundname = reqname
            row = self.create_row(base_row, clusterid, fundname, requirements, funding)
            if requirements and funding != '':
                row['percentFunded'] = int(funding / requirements * 100 + 0.5)
            else:
                row['percentFunded'] = ''
            subrows.append(row)

        fundclusterids = list(funding_clusters.keys())
        for clusterid, (reqname, requirements) in requirements_clusters.items():
            if clusterid in fundclusterids:
                continue
            row = self.create_row(base_row, clusterid, reqname, requirements)
            subrows.append(row)

        self.rows.extend(sorted(subrows, key=lambda k: k['cluster']))

        row = self.create_row(base_row, name='Not specified', funding=notspecified)
        self.rows.append(row)
        row = self.create_row(base_row, name='Multiple clusters/sectors (shared)', funding=shared)
        self.rows.append(row)

    def generate_plan_requirements_funding(self, inrow):
        requirements_clusters, funding_clusters, notspecified, shared = self.get_requirements_funding_plan(inrow)
        self.generate_rows_requirements_funding(inrow, requirements_clusters, funding_clusters, notspecified, shared)

    def generate_resource(self, folder, dataset, country):
        if not self.rows:
            return None
        headers = list(self.rows[0].keys())
        filename = f'fts_requirements_funding_{self.clusterlevel}cluster_{country["iso3"].lower()}.csv'
        description = f'FTS Annual Requirements and Funding Data by Cluster for {country["name"]}'
        if self.clusterlevel:
            description = description.replace('Cluster', f'{self.clusterlevel.capitalize()} Cluster')
        resourcedata = {
            'name': filename,
            'description': description,
            'format': 'csv'
        }
        success, results = dataset.generate_resource_from_iterator(headers, self.rows, hxl_names, folder, filename,
                                                                   resourcedata)
        self.rows = list()
        if success:
            return results['resource']
        else:
            return None
