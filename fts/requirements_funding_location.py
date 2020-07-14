import logging
from os.path import join

from hdx.data.resource import Resource
from hdx.utilities.downloader import DownloadError
from pandas import DataFrame, json_normalize, to_numeric

from fts.helpers import download_data, cluster_plan_columns_to_keep, location_columns_to_keep, rename_columns, hxl_names
from fts.pandas_helpers import drop_columns_except, remove_nonenan, remove_fractions, hxlate

logger = logging.getLogger(__name__)


def generate_requirements_funding_location(base_url, downloader, planids, dffundreq):
    combined = DataFrame()
    for planid in planids:
        funding_url = '%sfts/flow?planid=%s&groupby=location' % (base_url, planid)
        try:
            data = download_data(funding_url, downloader)
            fund_objects = data['report3']['fundingTotals']['objects']
            if len(fund_objects) == 0:
                logger.warning('%s has no funding objects!' % funding_url)
                fund_data_location = None
            else:
                fund_data_location = fund_objects[0]['objectsBreakdown']
        except DownloadError:
            logger.error('Problem with downloading %s!' % funding_url)
            continue
        req_data_location = data['requirements']['objects']
        if req_data_location:
            dfreq_location = json_normalize(req_data_location)
            if 'id' not in dfreq_location:
                dfreq_location['id'] = ''
            else:
                dfreq_location['id'] = dfreq_location.id.astype(str).str.replace('\\.0', '')
            if fund_data_location:
                dffund_location = json_normalize(fund_data_location)
                if 'id' not in dffund_location:
                    dffund_location['id'] = ''
                df = dffund_location.merge(dfreq_location, on='id', how='outer', validate='1:1')
            else:
                df = dfreq_location
                df['totalFunding'] = ''
        else:
            if fund_data_location:
                df = json_normalize(fund_data_location)
                df['revisedRequirements'] = ''
            else:
                logger.error('No data in %s!' % funding_url)
                continue

        df.rename(columns={'id': 'countryCode'}, inplace=True)
        df.rename(columns={'name': 'countryName'}, inplace=True)
        df = drop_columns_except(df, cluster_plan_columns_to_keep)
        remove_nonenan(df, 'countryCode')
        if fund_data_location is None:
            shared_funding = None
        else:
            shared_funding = data['report3']['fundingTotals']['objects'][0]['totalBreakdown']['sharedFunding']
        if shared_funding:
            row = {'countryCode': '', 'countryName': 'zzz', 'revisedRequirements': '', 'totalFunding': shared_funding}
            df.loc[len(df)] = row
        df['id'] = planid

        combined = combined.append(df, ignore_index=True)

    if len(combined) == 0:
        logger.warning('No location data available')
        return None, False

    df = combined.merge(dffundreq, on='id')
    df.rename(columns={'name_x': 'name', 'revisedRequirements_x': 'revisedRequirements', 'totalFunding_x': 'totalFunding'}, inplace=True)
    df = drop_columns_except(df, location_columns_to_keep)
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
    remove_fractions(df, 'locationCode')
    remove_nonenan(df, 'locationCode')
    df.sort_values(['endDate', 'name', 'locationName'], ascending=[False, True, True], inplace=True)
    df['locationName'].replace('zzz', 'Shared Funding', inplace=True)
    s = df['locationName']
    hxl_resource = False
    if not s[~s.isin(['Shared Funding', 'Multi-sector', 'Not specified'])].empty:
        s = df['percentFunded'] == ''
        if not s[~s.isin([True])].empty:
            hxl_resource = True
    df.rename(index=str, columns=rename_columns, inplace=True)
    return df, hxl_resource


def generate_requirements_funding_location_resource(base_url, downloader, folder, emergencyid, planids,
                                                   dffundreq, dataset):
    filename = 'fts_requirements_funding_location_%s.csv' % emergencyid
    df, hxl_resource = generate_requirements_funding_location(base_url, downloader, planids, dffundreq)
    if df is None:
        return None

    df = hxlate(df, hxl_names)
    file_to_upload = join(folder, filename)
    df.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': filename,
        'description': 'FTS Annual Requirements and Funding Data by Location for %s' % countryname,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)
    if hxl_resource:
        return resource
    return None
