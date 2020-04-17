import logging
from os.path import join

from hdx.data.resource import Resource
from hdx.utilities.downloader import DownloadError
from pandas import DataFrame, json_normalize, to_numeric

from fts.helpers import download_data, plan_columns_to_keep, cluster_columns_to_keep, rename_columns, hxl_names
from fts.pandas_helpers import drop_columns_except, remove_nonenan, remove_fractions, hxlate

logger = logging.getLogger(__name__)


def generate_requirements_funding_cluster(base_url, downloader, countryiso, planids, dffundreq, all_plans):
    combined = DataFrame()
    for planid in planids:
        data = all_plans.get(planid)
        locations = data['locations']
        # when the time comes to do cluster breakdowns by emergency, the test here would be for len(emergencies) I think
        if len(locations) == 0:
            logger.warning('Plan %s spans multiple locations - ignoring in cluster breakdown!' % planid)
            continue
        else:
            found = False
            for location in data['locations']:
                adminlevel = location.get('adminlevel', location.get('adminLevel'))
                if adminlevel == 0 and location['iso3'] != countryiso:
                    found = True
                    break
            if found:
                logger.warning('Plan %s spans multiple locations - ignoring in cluster breakdown!' % planid)
                continue

        funding_url = '%sfts/flow?planid=%s&groupby=cluster' % (base_url, planid)
        try:
            data = download_data(funding_url, downloader)
            fund_objects = data['report3']['fundingTotals']['objects']
            if len(fund_objects) == 0:
                logger.warning('%s has no funding objects!' % funding_url)
                fund_data_cluster = None
            else:
                fund_data_cluster = fund_objects[0]['objectsBreakdown']
        except DownloadError:
            logger.error('Problem with downloading %s!' % funding_url)
            continue
        req_data_cluster = data['requirements']['objects']
        if req_data_cluster:
            dfreq_cluster = json_normalize(req_data_cluster)
            if 'id' not in dfreq_cluster:
                dfreq_cluster['id'] = ''
            else:
                dfreq_cluster['id'] = dfreq_cluster.id.astype(str).str.replace('\\.0', '')
            if fund_data_cluster:
                dffund_cluster = json_normalize(fund_data_cluster)
                if 'id' not in dffund_cluster:
                    dffund_cluster['id'] = ''
                df = dffund_cluster.merge(dfreq_cluster, on='id', how='outer', validate='1:1')
                df.rename(columns={'name_x': 'clusterName'}, inplace=True)
                df['clusterName'] = df.clusterName.fillna(df.name_y)
                del df['name_y']
            else:
                df = dfreq_cluster
                df['totalFunding'] = ''
                df.rename(columns={'name': 'clusterName'}, inplace=True)
        else:
            if fund_data_cluster:
                df = json_normalize(fund_data_cluster)
                df['revisedRequirements'] = ''
                df.rename(columns={'name': 'clusterName'}, inplace=True)
            else:
                logger.error('No data in %s!' % funding_url)
                continue

        df.rename(columns={'id': 'clusterCode'}, inplace=True)
        df = drop_columns_except(df, plan_columns_to_keep)
        remove_nonenan(df, 'clusterCode')
        if fund_data_cluster is None:
            shared_funding = None
        else:
            shared_funding = data['report3']['fundingTotals']['objects'][0]['totalBreakdown']['sharedFunding']
        if shared_funding:
            row = {'clusterCode': '', 'clusterName': 'zzz', 'revisedRequirements': '', 'totalFunding': shared_funding}
            df.loc[len(df)] = row
        df['id'] = planid

        combined = combined.append(df, ignore_index=True)

    if len(combined) == 0:
        logger.warning('No cluster data available')
        return None, False

    df = combined.merge(dffundreq, on='id')
    df.rename(columns={'name_x': 'name', 'revisedRequirements_x': 'revisedRequirements', 'totalFunding_x': 'totalFunding'}, inplace=True)
    df = drop_columns_except(df, cluster_columns_to_keep)
    df['percentFunded'] = ((to_numeric(df.totalFunding) / to_numeric(df.revisedRequirements) * 100) + 0.5).astype(str)
    remove_fractions(df, 'revisedRequirements')
    remove_nonenan(df, 'revisedRequirements')
    remove_fractions(df, 'totalFunding')
    remove_nonenan(df, 'totalFunding')
    remove_fractions(df, 'percentFunded')
    remove_nonenan(df, 'percentFunded')
    df['id'] = df.id.astype(str)
    remove_fractions(df, 'id')
    remove_nonenan(df, 'id')
    remove_fractions(df, 'clusterCode')
    remove_nonenan(df, 'clusterCode')
    df.sort_values(['endDate', 'name', 'clusterName'], ascending=[False, True, True], inplace=True)
    df['clusterName'].replace('zzz', 'Shared Funding', inplace=True)
    s = df['clusterName']
    hxl_resource = False
    if not s[~s.isin(['Shared Funding', 'Multi-sector', 'Not specified'])].empty:
        s = df['percentFunded'] == ''
        if not s[~s.isin([True])].empty:
            hxl_resource = True
    df.rename(index=str, columns=rename_columns, inplace=True)
    return df, hxl_resource


def generate_requirements_funding_cluster_resource(base_url, downloader, folder, countryname, countryiso, planids,
                                                   dffundreq, all_plans, dataset):
    filename = 'fts_requirements_funding_cluster_%s.csv' % countryiso.lower()
    df, hxl_resource = generate_requirements_funding_cluster(base_url, downloader, countryiso, planids, dffundreq, all_plans)
    if df is None:
        return None

    df = hxlate(df, hxl_names)
    file_to_upload = join(folder, filename)
    df.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': filename,
        'description': 'FTS Annual Requirements and Funding Data by Cluster for %s' % countryname,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)
    if hxl_resource:
        return filename
    return None