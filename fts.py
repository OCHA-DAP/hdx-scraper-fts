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
from pandas import DataFrame, concat, to_numeric, Series
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
country_columns_to_keep = ['country', 'id', 'name', 'code', 'startDate', 'endDate', 'year', 'revisedRequirements', 'totalFunding']
cluster_columns_to_keep = ['country', 'id', 'name', 'code', 'startDate', 'endDate', 'year', 'totalFunding']


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
    title = 'FTS Requirements and Funding Data for %s' % countryname
    slugified_name = slugify(title).lower()

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

    funding_url = '%sfts/flow?countryISO3=%s&year=%s' % (base_url, countryiso, latestyear)
    r = downloader.download(funding_url)
    fund_data = r.json()['data']['flows']
    dffund = json_normalize(fund_data)

    def get_organization(x):
        for infodict in x:
            if infodict['type'] == 'Organization':
                return infodict
        return {'id': '', 'name': '', 'organizationTypes': ''}

    if 'sourceObjects' not in dffund:
        logger.error('No sourceObjects column for %s' % title)
        return None, None

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
    dffund = drop_stuff(dffund, country_all_columns_to_keep)
    dffund.sort_values('date', ascending=False, inplace=True)
    dffund.date = dffund.date.str[:10]
    dffund.firstReportedDate = dffund.firstReportedDate.str[:10]
    dffund.decisionDate = dffund.decisionDate.str[:10]
    dffund.createdAt = dffund.createdAt.str[:10]
    dffund.updatedAt = dffund.updatedAt.str[:10]
    # add HXL tags
    dffund = hxlate(dffund, funding_hxl_names)

    filename = 'fts_funding_%s.csv' % countryiso
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

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'FTS %s Summary Page' % countryname,
        'notes': 'Click the image on the right to go to the FTS funding summary page for %s' % countryname,
        'url': 'https://fts.unocha.org/countries/%s/flows/2017' % locationid,
        'image_url': 'https://fts.unocha.org/sites/default/files/styles/fts_feature_image/public/navigation_101.jpg'
    })
    showcase.add_tags(tags)

    requirements_url = '%splan/country/%s' % (base_url, countryiso)
    funding_url = '%sfts/flow?groupby=plan&countryISO3=%s' % (base_url, countryiso)
    r = downloader.download(requirements_url)
    req_data = r.json()['data']
    if len(req_data) == 0:
        logger.error('No requirements data for %s' % title)
        return dataset, showcase
    dfreq = json_normalize(req_data)
    dfreq['country'] = dfreq['locations'].apply(lambda x: x[0]['name'])
    dfreq['year'] = dfreq['years'].apply(lambda x: x[0]['year'])
    r = downloader.download(funding_url)
    fund_data = r.json()['data']['report3']['fundingTotals']['objects'][0]['singleFundingObjects']
    dffund = json_normalize(fund_data)
    if 'id' not in dffund:
        logger.error('No id column for %s' % title)
        return dataset, showcase
    dffundreq = dfreq.merge(dffund, on='id')
    dffundreq.rename(columns={'name_x': 'name'}, inplace=True)
    dffundreq = drop_stuff(dffundreq, country_columns_to_keep)
    dffundreq.sort_values('endDate', ascending=False, inplace=True)
    dffundreq.startDate = dffundreq.startDate.str[:10]
    dffundreq.endDate = dffundreq.endDate.str[:10]
    dffundreq['percentFunded'] = (to_numeric(dffundreq.totalFunding) / to_numeric(
        dffundreq.revisedRequirements) * 100).astype(str)
    # convert floats to string and trim ( formatters don't work on columns with mixed types)
    remove_nonenan(dffundreq, 'revisedRequirements')
    remove_nonenan(dffundreq, 'totalFunding')
    dffundreq['percentFunded'] = \
    dffundreq.percentFunded.loc[dffundreq.percentFunded.str.contains('.')].str.split('.').str[0]
    remove_nonenan(dffundreq, 'percentFunded')
    # add HXL tags
    dffundreq = hxlate(dffundreq, hxl_names)

    filename = 'fts_funding_requirements_%s.csv' % countryiso
    file_to_upload = join(folder, filename)
    dffundreq.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': filename.lower(),
        'description': title,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)

    funding_url = '%sfts/flow?groupby=plan&countryISO3=%s&filterBy=destinationGlobalClusterId:' % (base_url, countryiso)
    combined = DataFrame()
    for cluster in clusters:
        r = downloader.download('%s%s' % (funding_url, cluster['id']))
        fund_data_toplevel = r.json()['data']['report3']['fundingTotals']['objects']
        if not fund_data_toplevel:
            continue
        fund_data = fund_data_toplevel[0]['singleFundingObjects']
        df = json_normalize(fund_data)
        if 'id' not in df:
            continue
        df = df.merge(dffundreq, on='id')
        df['id'] = df.id.astype(int)
        df.rename(columns={'name_x': 'name', 'totalFunding_x': 'totalFunding'}, inplace=True)
        df = drop_stuff(df, cluster_columns_to_keep)
        df['clusterCode'] = cluster['code']
        df['clusterName'] = cluster['name']
        remove_nonenan(df, 'totalFunding')
        combined = combined.append(df, ignore_index=True)
    combined.sort_values(['endDate', 'id'], ascending=[False, True], inplace=True)
    df = hxlate(combined, hxl_names)

    filename = 'fts_funding_cluster_%s.csv' % countryiso
    file_to_upload = join(folder, filename)
    df.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': filename.lower(),
        'description': 'FTS Funding Data by Cluster for %s' % countryname,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)

    return dataset, showcase
