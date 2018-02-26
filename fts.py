#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
FTS:
---

Generates FTS datasets.

'''
import logging
from os.path import join

from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.downloader import DownloadError
from pandas import DataFrame, concat, to_numeric
from pandas.io.json import json_normalize
from slugify import slugify

logger = logging.getLogger(__name__)

funding_hxl_names = {
    'amountUSD': '#value+funding+total+usd',
    'boundary': '#value+funding+direction',
    'budgetYear': '#date+year',
    'contributionType': '#value+funding+contribution+type',
    'createdAt': '#date+created',
    'date': '#date',
    'decisionDate': '#date+decision',
    'description': '#description+notes',
    'exchangeRate': '#value+funding+fx',
    'firstReportedDate': '#date+reported',
    'flowType': '#value+funding+contribution+type',
    'id': '#activity+id+fts_internal',
    'keywords': '#description+keywords',
    'method': '#value+funding+method',
    'originalAmount': '#value+funding+total',
    'originalCurrency': '#value+funding+total+currency',
    'refCode': '#activity+code',
    'status': '#status+text',
    'updatedAt': '#date+updated',
    'organizationId': '#org+id',
    'organizationName': '#org+name',
    'organizationTypes': '#org+type'
}

hxl_names = {
    'id': '#activity+appeal+id+fts_internal',
    'country': '#country+name',
    'name': '#activity+appeal+name',
    'code': '#activity+appeal+id+external',
    'revisedRequirements': '#value+funding+required+usd',
    'totalFunding': '#value+funding+total+usd',
    'startDate': '#date+start',
    'endDate': '#date+end',
    'year': '#date+year',
    'percentFunded': '#value+funding+pct',
    'clusterCode': '#sector+code',
    'clusterName': '#sector+name'
}

country_all_columns_to_keep = ['date', 'budgetYear', 'description', 'amountUSD', 'organizationName', 'organizationTypes', 'organizationId', 'contributionType', 'flowType', 'method', 'boundary', 'status', 'firstReportedDate', 'decisionDate', 'keywords', 'originalAmount', 'originalCurrency', 'exchangeRate', 'id', 'refCode', 'createdAt', 'updatedAt']
country_columns_to_keep = ['country', 'id', 'name', 'code', 'startDate', 'endDate', 'year', 'revisedRequirements', 'totalFunding', 'percentFunded']
plan_columns_to_keep = ['clusterCode', 'clusterName', 'revisedRequirements', 'totalFunding']
cluster_columns_to_keep = ['country', 'id', 'name', 'code', 'startDate', 'endDate', 'year', 'clusterCode', 'clusterName', 'revisedRequirements', 'totalFunding']


def drop_stuff(df, columns_to_keep):
    # Drop unwanted columns.  Note that I confirmed that the two date columns are always equal.
    df = df.loc[:,columns_to_keep]
    return df


def hxlate(df, hxl_names):
    hxl_columns = [hxl_names[c] for c in df.columns]
    hxl = DataFrame.from_records([hxl_columns], columns=df.columns)
    df = concat([hxl, df])
    df.reset_index(inplace=True, drop=True)
    return df


def remove_nonenan(df, colname):
    df[colname] = df[colname].astype(str)
    df[colname].replace(['nan', 'none'], ['', ''], inplace=True)


def get_clusters(base_url, downloader):
    url = '%sglobal-cluster' % base_url
    response = downloader.download(url)
    json = response.json()
    return json['data']


def get_countries(base_url, downloader):
    url = '%slocation' % base_url
    response = downloader.download(url)
    json = response.json()
    return json['data']


def generate_dataset_and_showcase(base_url, downloader, folder, clusters, countryiso, countryname, locationid, today):
    '''
    api.hpc.tools/v1/public/fts/flow?countryISO3=CMR&Year=2016&groupby=cluster
    '''
    latestyear = str(today.year)
    title = '%s - Requirements and Funding Data' % countryname
    slugified_name = slugify('FTS Requirements and Funding Data for %s' % countryname).lower()

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e')
    dataset.set_dataset_date_from_datetime(today)
    dataset.set_expected_update_frequency('Every day')
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.error('%s has a problem! %s' % (title, e))
        return None, None

    tags = ['HXL', 'cash', 'FTS']
    dataset.add_tags(tags)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'FTS %s Summary Page' % countryname,
        'notes': 'Click the image on the right to go to the FTS funding summary page for %s' % countryname,
        'url': 'https://fts.unocha.org/countries/%s/flows/2017' % locationid,
        'image_url': 'https://fts.unocha.org/sites/default/files/styles/fts_feature_image/public/navigation_101.jpg'
    })
    showcase.add_tags(tags)

    nodata = True
    funding_url = '%sfts/flow?countryISO3=%s&year=%s' % (base_url, countryiso, latestyear)
    r = downloader.download(funding_url)
    fund_data = r.json()['data']['flows']
    dffund = json_normalize(fund_data)

    def get_organization(x):
        for infodict in x:
            if infodict['type'] == 'Organization':
                return infodict
        return {'id': '', 'name': '', 'organizationTypes': ''}

    if 'sourceObjects' in dffund:
        tmp = dffund['sourceObjects'].apply(get_organization)
        dffund['organizationId'] = tmp.apply(lambda x: x['id'])
        dffund['organizationName'] = tmp.apply(lambda x: x['name'])
        dffund['organizationTypes'] = tmp.apply(lambda x: ','.join(x['organizationTypes']))

        def get_keywords(x):
            if x:
                return ','.join(x)
            else:
                return ''

        dffund['keywords'] = dffund.keywords.apply(get_keywords)
        if not 'originalAmount' in dffund:
            dffund['originalAmount'] = ''
            dffund['originalCurrency'] = ''
        if not 'refCode' in dffund:
            dffund['refCode'] = ''
        dffund = drop_stuff(dffund, country_all_columns_to_keep)
        dffund.sort_values('date', ascending=False, inplace=True)
        dffund.date = dffund.date.str[:10]
        dffund.firstReportedDate = dffund.firstReportedDate.str[:10]
        dffund.decisionDate = dffund.decisionDate.str[:10]
        dffund.createdAt = dffund.createdAt.str[:10]
        dffund.updatedAt = dffund.updatedAt.str[:10]
        # add HXL tags
        dffund = hxlate(dffund, funding_hxl_names)

        filename = 'fts_funding_%s.csv' % countryiso.lower()
        file_to_upload = join(folder, filename)
        dffund.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

        resource_data = {
            'name': filename.lower(),
            'description': 'FTS Funding Data for %s for %s' % (countryname, latestyear),
            'format': 'csv'
        }
        resource = Resource(resource_data)
        resource.set_file_to_upload(file_to_upload)
        dataset.add_update_resource(resource)
        nodata = False

    requirements_url = '%splan/country/%s' % (base_url, countryiso)
    funding_url = '%sfts/flow?groupby=plan&countryISO3=%s' % (base_url, countryiso)
    r = downloader.download(requirements_url)
    req_data = r.json()['data']
    if len(req_data) == 0:
        if nodata:
            return None, None
        else:
            logger.error('No requirements data for %s' % title)
            return dataset, showcase
    dfreq = json_normalize(req_data)
    dfreq['country'] = dfreq['locations'].apply(lambda x: x[0]['name'])
    dfreq['year'] = dfreq['years'].apply(lambda x: x[0]['year'])
    r = downloader.download(funding_url)
    data = r.json()['data']['report3']['fundingTotals']['objects'][0]
    fund_data = data.get('objectsBreakdown')
    if fund_data:
        dffund = json_normalize(fund_data)
        if 'id' in dffund:
            dffundreq = dfreq.merge(dffund, on='id', how='outer', validate='1:1')
            dffundreq.country.fillna(method='ffill', inplace=True)
            dffundreq.name_x.fillna(dffundreq.name_y, inplace=True)
            dffundreq.fillna('', inplace=True)
            dffundreq.totalFunding += dffundreq.onBoundaryFunding
            dffundreq['percentFunded'] = (to_numeric(dffundreq.totalFunding) / to_numeric(
                dffundreq.revisedRequirements) * 100).astype(str)
        else:
            dffundreq = dfreq
            dffundreq['totalFunding'] = ''
            dffundreq['percentFunded'] = '0'
    else:
        dffundreq = dfreq
        dffundreq['totalFunding'] = ''
        dffundreq['percentFunded'] = '0'
    dffundreq.rename(columns={'name_x': 'name'}, inplace=True)
    dffundreq = drop_stuff(dffundreq, country_columns_to_keep)
    dffundreq.sort_values('endDate', ascending=False, inplace=True)
    dffundreq.startDate = dffundreq.startDate.str[:10]
    dffundreq.endDate = dffundreq.endDate.str[:10]
    # convert floats to string and trim ( formatters don't work on columns with mixed types)
    remove_nonenan(dffundreq, 'revisedRequirements')
    remove_nonenan(dffundreq, 'totalFunding')
    dffundreq['id'] = dffundreq['id'].astype(str)
    dffundreq['id'] = dffundreq.id.loc[dffundreq.id.str.contains('.')].str.split('.').str[0]
    remove_nonenan(dffundreq, 'id')
    dffundreq['percentFunded'] = dffundreq.percentFunded.loc[dffundreq.percentFunded.str.contains('.')].str.split('.').str[0]
    remove_nonenan(dffundreq, 'percentFunded')
    # sort
    dffundreq.sort_values(['endDate'], ascending=[False], inplace=True)
    # add HXL tags
    hxldffundreq = hxlate(dffundreq, hxl_names)

    filename = 'fts_requirements_funding_%s.csv' % countryiso.lower()
    file_to_upload = join(folder, filename)
    hxldffundreq.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': filename.lower(),
        'description': 'FTS Requirements and Funding Data for %s' % countryname,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)

    combined = DataFrame()
    for _, row in dffundreq.iterrows():
        planid = row['id']
        if planid == '':
            planname = row['name']
            if planname == 'Not specified':
                continue
            raise ValueError('Plan Name: %s is invalid!' % planname)
        funding_url = '%sfts/flow?planid=%s&groupby=globalcluster' % (base_url, planid)
        try:
            r = downloader.download(funding_url)
            data = r.json()['data']
            fund_objects = data['report3']['fundingTotals']['objects']
            if len(fund_objects) == 0:
                logger.error('%s has no funding objects!' % funding_url)
                fund_data = None
            else:
                fund_data = fund_objects[0]['objectsBreakdown']
        except DownloadError:
            logger.error('Problem with downloading %s!' % funding_url)
            continue
        req_data = data['requirements']['objects']
        if req_data:
            dfreq = json_normalize(req_data)
            if fund_data:
                dffund = json_normalize(fund_data)
                if not 'id' in dffund:
                    dffund['id'] = ''
                if not 'id' in dfreq:
                    dfreq['id'] = ''
                df = dffund.merge(dfreq, on='id', how='outer', validate='1:1')
                df.rename(columns={'name_x': 'clusterName'}, inplace=True)
                df.clusterName.fillna(df.name_y, inplace=True)
                del df['name_y']
            else:
                df = dfreq
                df['totalFunding'] = ''
                df.rename(columns={'name': 'clusterName'}, inplace=True)
        else:
            df = json_normalize(fund_data)
            df['revisedRequirements'] = ''
            df.rename(columns={'name': 'clusterName'}, inplace=True)
        df.rename(columns={'id': 'clusterCode'}, inplace=True)
        df = drop_stuff(df, plan_columns_to_keep)
        remove_nonenan(df, 'clusterCode')
        if fund_data is None:
            shared_funding = None
        else:
            shared_funding = data['report3']['fundingTotals']['objects'][0]['totalBreakdown']['sharedFunding']
        if shared_funding:
            row = {'clusterCode': '', 'clusterName': 'zzz', 'revisedRequirements': '', 'totalFunding': shared_funding}
            df.loc[len(df)] = row
        df['id'] = planid

        combined = combined.append(df, ignore_index=True)
    if len(combined) == 0:
        logger.error('No cluster data for %s' % title)
        return dataset, showcase

    df = combined.merge(dffundreq, on='id')
    df.rename(columns={'name_x': 'name', 'revisedRequirements_x': 'revisedRequirements', 'totalFunding_x': 'totalFunding'}, inplace=True)
    df = drop_stuff(df, cluster_columns_to_keep)
    remove_nonenan(df, 'revisedRequirements')
    remove_nonenan(df, 'totalFunding')
    df['percentFunded'] = (to_numeric(df.totalFunding) / to_numeric(
        df.revisedRequirements) * 100).astype(str)
    df['percentFunded'] = df.percentFunded.loc[df.percentFunded.str.contains('.')].str.split('.').str[0]
    remove_nonenan(df, 'percentFunded')
    df['id'] = df.id.astype(str)
    df['id'] = df.id.loc[df.id.str.contains('.')].str.split('.').str[0]
    remove_nonenan(df, 'id')
    df.sort_values(['endDate', 'name', 'clusterName'], ascending=[False, True, True], inplace=True)
    df['clusterName'].replace('zzz', 'Shared Funding', inplace=True)
    df = hxlate(df, hxl_names)

    filename = 'fts_requirements_funding_cluster_%s.csv' % countryiso.lower()
    file_to_upload = join(folder, filename)
    df.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': filename.lower(),
        'description': 'FTS Requirements and Funding Data by Cluster for %s' % countryname,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)

    return dataset, showcase
