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
    'country': '#country+name',
    'id': '#x_appeal+id',
    'name': '#x_appeal+name',
    'code': '#x_appeal+code',
    'revisedRequirements': '#x_requirement+x_usd+x_current',
    'endDate': '#date+end',
    'totalFunding': '#x_funding+x_usd',
    'startDate': '#date+start',
    'year': '#date+year',
    'percentFunded': '#x_requirement+x_met+x_percent'
}

columns_to_keep = ['country', 'id', 'name', 'code', 'startDate', 'endDate', 'year', 'revisedRequirements', 'totalFunding']


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


def generate_dataset(folder, downloader, countryiso, countryname):
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


    requirements_url = '%splan/country/' % base_url
    funding_url = '%sfts/flow?groupby=plan&countryISO3=' % base_url
    r = downloader.download('%s%s' % (requirements_url, countryiso))
    req_data = r.json()['data']
    if len(req_data) == 0:
        return None
    sorted_req_data = sorted(req_data, key=lambda d: d['endDate'])
    dfreq_norm = json_normalize(sorted_req_data)
    dfreq_norm = dfreq_norm[dfreq_norm.revisedRequirements > 0]
    dfreq_norm['id'].fillna('missing')
    dfreq_loc = json_normalize(sorted_req_data, 'locations')
    dfreq_loc.rename(columns={'name': 'country'}, inplace=True)
    del dfreq_loc['id']
    dfreq_norm_loc = dfreq_norm.join(dfreq_loc)
    dfreq_year = json_normalize(sorted_req_data, 'years')
    del dfreq_year['id']
    dfreq = dfreq_norm_loc.join(dfreq_year)
    r = downloader.download('%s%s' % (funding_url, countryiso))
    fund_data = r.json()['data']['report3']['fundingTotals']['objects'][0]['singleFundingObjects']
    dffund = json_normalize(fund_data)
    df = dfreq.merge(dffund, on='id')
    df = df[df.totalFunding > 0]
    df.rename(columns={'name_x': 'name'}, inplace=True)
    df = drop_stuff(df, columns_to_keep)
    df.startDate = df.startDate.str[:10]
    df.endDate = df.endDate.str[:10]
    df['percentFunded'] = (to_numeric(df.totalFunding) / to_numeric(
        df.revisedRequirements)) * 100
    df.dropna()

    # add HXL tags
    df = hxlate(df, hxl_names)

    # convert floats to string and trim ( formatters don't work on columns with mixed types)
    df['percentFunded'] = df['percentFunded'].astype(str)
    df['percentFunded'].loc[df['percentFunded'].str.contains('.')].str.split('.').str[0]

    filename = 'fts_funding_requirements_%s.csv' % countryiso
    file_to_upload = join(folder, filename)
    df.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': filename.lower(),
        'description': title,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)
    return dataset
