#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
FTS:
---

Generates FTS datasets.

'''
from datetime import datetime
from os.path import join

from hdx.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from pandas import DataFrame, concat, to_numeric, notnull
from pandas.io.json import json_normalize
from slugify import slugify

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
    'sector': '#sector'
}

country_columns_to_keep = ['country', 'id', 'name', 'code', 'startDate', 'endDate', 'year', 'revisedRequirements', 'totalFunding']
sector_columns_to_keep = ['country', 'id', 'name', 'code', 'startDate', 'endDate', 'year', 'totalFunding']


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


def generate_dataset(folder, downloader, clusters, countryiso, countryname):
    '''
    api.hpc.tools/v1/public/fts/flow?countryISO3=CMR&Year=2016&groupby=cluster
    '''
    base_url = Configuration.read()['base_url']

    title = 'FTS Requirements and Funding Data for %s' % countryname
    slugified_name = slugify(title).lower()

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_dataset_date_from_datetime(datetime.now())
    dataset.set_expected_update_frequency('Every day')
    dataset.add_country_location(countryiso)
    dataset.add_tags(['cash'])


    requirements_url = '%splan/country/%s' % (base_url, countryiso)
    funding_url = '%sfts/flow?groupby=plan&countryISO3=%s' % (base_url, countryiso)
    r = downloader.download(requirements_url)
    req_data = r.json()['data']
    if len(req_data) == 0:
        return None
    dfreq_norm = json_normalize(req_data)
    dfreq_loc = json_normalize(req_data, 'locations')
    dfreq_loc.rename(columns={'name': 'country'}, inplace=True)
    del dfreq_loc['id']
    dfreq_norm_loc = dfreq_norm.join(dfreq_loc)
    dfreq_year = json_normalize(req_data, 'years')
    del dfreq_year['id']
    dfreq = dfreq_norm_loc.join(dfreq_year)
    r = downloader.download(funding_url)
    fund_data = r.json()['data']['report3']['fundingTotals']['objects'][0]['singleFundingObjects']
    dffund = json_normalize(fund_data)
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
        df = df.merge(dffundreq, on='id')
        df['id'] = df.id.astype(int)
        df.rename(columns={'name_x': 'name', 'totalFunding_x': 'totalFunding'}, inplace=True)
        df = drop_stuff(df, sector_columns_to_keep)
        sector = cluster['code']
        if not sector:
            sector = cluster['name']
        df['sector'] = sector
        remove_nonenan(df, 'totalFunding')
        combined = combined.append(df, ignore_index=True)
    combined.sort_values(['endDate', 'id'], ascending=[False, True], inplace=True)
    df = hxlate(combined, hxl_names)

    filename = 'fts_funding_sector_%s.csv' % countryiso
    file_to_upload = join(folder, filename)
    df.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': filename.lower(),
        'description': '%s by sector' % title,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)

    return dataset
