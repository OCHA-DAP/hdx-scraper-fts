#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
FTS:
---

Generates FTS datasets.

'''
import copy
import logging
from collections import OrderedDict
from os.path import join

from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import DownloadError
from hdx.utilities.text import multiple_replace
from pandas import DataFrame, concat, to_numeric, Series, json_normalize
from slugify import slugify

logger = logging.getLogger(__name__)

funding_hxl_names = {
    'amountUSD': '#value+funding+total+usd',
    'boundary': '#financial+direction',
    'onBoundary': '#financial+direction+type',
    'budgetYear': '#date+year+budget',
    'contributionType': '#financial+contribution+type',
    'createdAt': '#date+created',
    'date': '#date',
    'decisionDate': '#date+decision',
    'description': '#description+notes',
    'exchangeRate': '#financial+fx',
    'firstReportedDate': '#date+reported',
    'flowType': '#financial+contribution+type',
    'id': '#activity+id+fts_internal',
    'keywords': '#description+keywords',
    'method': '#financial+method',
    'originalAmount': '#value+funding+total',
    'originalCurrency': '#value+funding+total+currency',
    'refCode': '#activity+code',
    'status': '#status+text',
    'updatedAt': '#date+updated',
    'srcOrganization': '#org+name+funder',
    'srcOrganizationTypes': '#org+type+funder+list',
    'srcLocations': '#country+iso3+funder+list',
    'srcUsageYearStart': '#date+year+start+funder',
    'srcUsageYearEnd': '#date+year+end+funder',
    'destPlan': '#activity+appeal+name',
    'destPlanCode': '#activity+appeal+id+external',
    'destPlanId': '#activity+appeal+id+fts_internal',
    'destOrganization': '#org+name+impl',
    'destOrganizationTypes': '#org+type+impl+list',
    'destGlobalClusters': '#sector+cluster+name+list',
    'destLocations': '#country+iso3+impl+list',
    'destProject': '#activity+project+name',
    'destProjectCode': '#activity+project+code',
    'destEmergency': '#crisis+name',
    'destUsageYearStart': '#date+year+start+impl',
    'destUsageYearEnd': '#date+year+end+impl'
}

hxl_names = {
    'id': '#activity+appeal+id+fts_internal',
    'countryCode': '#country+code',
    'name': '#activity+appeal+name',
    'code': '#activity+appeal+id+external',
    'requirements': '#value+funding+required+usd',
    'funding': '#value+funding+total+usd',
    'startDate': '#date+start',
    'endDate': '#date+end',
    'year': '#date+year',
    'percentFunded': '#value+funding+pct',
    'clusterCode': '#sector+cluster+code',
    'cluster': '#sector+cluster+name'
}

rename_columns = {
    'totalFunding': 'funding',
    'revisedRequirements': 'requirements',
    'clusterName': 'cluster'
}

country_all_columns_to_keep = ['date', 'budgetYear', 'description', 'amountUSD', 'srcOrganization',
                               'srcOrganizationTypes', 'srcLocations', 'srcUsageYearStart', 'srcUsageYearEnd',
                               'destPlan', 'destPlanCode', 'destPlanId', 'destOrganization', 'destOrganizationTypes', 'destGlobalClusters', 'destLocations',
                               'destProject', 'destProjectCode', 'destEmergency', 'destUsageYearStart', 'destUsageYearEnd',
                               'contributionType', 'flowType', 'method', 'boundary', 'onBoundary', 'status',
                               'firstReportedDate', 'decisionDate', 'keywords', 'originalAmount', 'originalCurrency',
                               'exchangeRate', 'id', 'refCode', 'createdAt', 'updatedAt']
country_emergency_columns_to_keep = ['id', 'name', 'code', 'startDate', 'endDate', 'year', 'revisedRequirements',
                                     'totalFunding', 'percentFunded']
plan_columns_to_keep = ['clusterCode', 'clusterName', 'revisedRequirements', 'totalFunding']
cluster_columns_to_keep = ['countryCode', 'id', 'name', 'code', 'startDate', 'endDate', 'year', 'clusterCode',
                           'clusterName', 'revisedRequirements', 'totalFunding']


class FTSException(Exception):
    pass


def remove_fractions(df, colname):
    df[colname] = df[colname].astype(str)
    df[colname] = df[colname].str.split('.').str[0]


def drop_columns_except(df, columns_to_keep):
    # Remove duplicate columns
    df = df.loc[:,~df.columns.duplicated()]
    # Drop unwanted columns.
    return df.reindex(columns=columns_to_keep)


def drop_rows_with_col_word(df, columnname, word):
    # Drop unwanted rows
    pattern = r'\b%s\b' % word
    df = df[~df[columnname].str.contains(pattern, case=False)]
    return df


def lookup_values_by_key(df, lookupcolumn, key, valuecolumn):
    return df.query('%s==%s' % (lookupcolumn, key))[valuecolumn]


def hxlate(df, hxl_names):
    hxl_columns = [hxl_names[c] for c in df.columns]
    hxl = DataFrame.from_records([hxl_columns], columns=df.columns)
    df = concat([hxl, df])
    df.reset_index(inplace=True, drop=True)
    return df


def remove_nonenan(df, colname):
    df[colname] = df[colname].astype(str)
    df[colname].replace(['nan', 'none'], ['', ''], inplace=True)


def download(url, downloader):
    r = downloader.download(url)
    json = r.json()
    status = json['status']
    if status != 'ok':
        raise FTSException('%s gives status %s' % (url, status))
    return json


def download_data(url, downloader):
    return download(url, downloader)['data']


def get_countries(base_url, downloader):
    return download_data('%slocation' % base_url, downloader)


def get_dataset_and_showcase(slugified_name, title, description, today, country_emergency, showcase_url):
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
        'notes': description
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e')
    dataset.set_dataset_date_from_datetime(today)
    dataset.set_expected_update_frequency('Every day')
    dataset.set_subnational(False)
    tags = ['hxl', 'financial tracking service - fts', 'aid funding']
    dataset.add_tags(tags)
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'FTS %s Summary Page' % country_emergency,
        'notes': 'Click the image on the right to go to the FTS funding summary page for %s' % country_emergency,
        'url': showcase_url,
        'image_url': 'https://fts.unocha.org/sites/default/files/styles/fts_feature_image/public/navigation_101.jpg'
    })
    showcase.add_tags(tags)
    return dataset, showcase


def generate_flows_resources(funding_url, downloader, folder, dataset, code, country_emergency, latestyear):
    fund_boundaries_info = list()
    fund_data = list()
    while funding_url:
        json = download(funding_url, downloader)
        fund_data.extend(json['data']['flows'])
        funding_url = json['meta'].get('nextLink')

    dffunddet = json_normalize(fund_data)

    def add_objects(name):
        def flatten_objects(x):
            outputdicts = OrderedDict()
            typedicts = OrderedDict()
            for infodict in x:
                infodicttype = infodict['type']
                typedict = typedicts.get(infodicttype, OrderedDict())
                for key in infodict:
                    if key not in ['type', 'behavior', 'id'] or (infodicttype == 'Plan' and key == 'id'):
                        value = infodict[key]
                        if isinstance(value, list):
                            for element in value:
                                dict_of_lists_add(typedict, key, element)
                        else:
                            dict_of_lists_add(typedict, key, value)
                typedicts[infodicttype] = typedict
            for objectType in typedicts:
                prefix = '%s%s' % (name, objectType)
                for key in typedicts[objectType]:
                    keyname = '%s%s' % (prefix, key.capitalize())
                    values = typedicts[objectType][key]
                    replacements = {'OrganizationOrganization': 'Organization', 'Name': '', 'types': 'Types',
                                    'code': 'Code', 'source': 'src', 'destination': 'dest'}
                    keyname = multiple_replace(keyname, replacements)
                    if 'UsageYear' in keyname:
                        values = sorted(values)
                        outputdicts['%sStart' % keyname] = values[0]
                        outputstr = values[-1]
                        keyname = '%sEnd' % keyname
                    elif any(x in keyname for x in ['Cluster', 'Location', 'OrganizationTypes']):
                        if keyname[-1] != 's':
                            keyname = '%ss' % keyname
                        if 'Location' in keyname:
                            iso3s = list()
                            for country in values:
                                iso3, _ = Country.get_iso3_country_code_fuzzy(country)
                                if iso3:
                                    iso3s.append(iso3)
                            values = iso3s
                        outputstr = ','.join(sorted(values))
                    else:
                        if len(values) > 1:
                            outputstr = 'Multiple'
                            logger.error('Multiple used instead of %s for %s in %s' % (values, keyname,
                                                                                       country_emergency))
                        else:
                            outputstr = values[0]
                    outputdicts[keyname] = outputstr
            return outputdicts

        typedicts = dffunddet['%sObjects' % name].apply(flatten_objects)
        return dffunddet.join(DataFrame(list(typedicts)))

    if 'sourceObjects' in dffunddet:
        dffunddet = add_objects('source')
        dffunddet = add_objects('destination')

        def get_keywords(x):
            if x:
                return ','.join(x)
            else:
                return ''

        dffunddet['keywords'] = dffunddet.keywords.apply(get_keywords)
        if 'originalAmount' not in dffunddet:
            dffunddet['originalAmount'] = ''
        if 'originalCurrency' not in dffunddet:
            dffunddet['originalCurrency'] = ''
        if 'refCode' not in dffunddet:
            dffunddet['refCode'] = ''
        dffunddet = drop_columns_except(dffunddet, country_all_columns_to_keep)
        dffunddet.sort_values('date', ascending=False, inplace=True)
        dffunddet.date = dffunddet.date.str[:10]
        dffunddet.firstReportedDate = dffunddet.firstReportedDate.str[:10]
        dffunddet.decisionDate = dffunddet.decisionDate.str[:10]
        dffunddet.createdAt = dffunddet.createdAt.str[:10]
        dffunddet.updatedAt = dffunddet.updatedAt.str[:10]
        dffunddet.rename(index=str, columns=rename_columns, inplace=True)

        for boundary, dffundbound in dffunddet.groupby(['boundary']):
            # add HXL tags
            dffundbound = hxlate(dffundbound, funding_hxl_names)
            filename = 'fts_%s_funding_%s.csv' % (boundary, code)
            fund_boundaries_info.append((dffundbound, join(folder, filename)))

            resource_data = {
                'name': filename.lower(),
                'description': 'FTS %s Funding Data for %s for %s' % (boundary.capitalize(), country_emergency, latestyear),
                'format': 'csv'
            }
            resource = Resource(resource_data)
            resource.set_file_to_upload(fund_boundaries_info[-1][1])
            dataset.add_update_resource(resource)
    return fund_boundaries_info


def generate_requirements_funding_resource(requirements_url, funding_url, downloader, folder, name, code, columnname, dataset):
    planidcodemapping = dict()
    req_data = download_data(requirements_url, downloader)
    fund_data = download_data(funding_url, downloader)
    data = fund_data['report3']['fundingTotals']['objects']
    if len(data) == 0:
        fund_data = None
    else:
        fund_data = data[0].get('objectsBreakdown')
    columns_to_keep = copy.deepcopy(country_emergency_columns_to_keep)
    columns_to_keep.insert(0, columnname)
    if len(req_data) == 0:
        if not fund_data:
            return None, None, None, None
        logger.warning('No requirements data, only funding data available')
        dffund = json_normalize(fund_data)
        dffund = drop_columns_except(dffund, columns_to_keep)
        dffund['percentFunded'] = ''
        dffund = dffund.fillna('')
        dffundreq = dffund
        incompleteplans = list()
    else:
        dfreq = json_normalize(req_data)
        dfreq['year'] = dfreq['years'].apply(lambda x: x[0]['year'])
        if bool(dfreq['years'].apply(lambda x: len(x) != 1).any()) is True:
            logger.error('More than one year listed in a plan for %s!' % name)
        dfreq['id'] = dfreq.id.astype(str).str.replace('\\.0', '')
        dfreq.rename(columns={'planVersion.id': 'planVersion_id'}, inplace=True)
        dfreq.rename(columns=lambda x: x.replace('planVersion.', ''), inplace=True)
        incompleteplans = dfreq.id.loc[~dfreq['revisionState'].isin(['none', None])].values
        planidcodemapping.update(Series(dfreq.code.values, index=dfreq.id).to_dict())
        if fund_data:
            dffund = json_normalize(fund_data)
            if 'id' in dffund:
                dffundreq = dfreq.merge(dffund, on='id', how='outer', validate='1:1')
                dffundreq['name_x'] = dffundreq.name_x.fillna(dffundreq.name_y)
                dffundreq = dffundreq.fillna('')
                dffundreq['percentFunded'] = ((to_numeric(dffundreq.totalFunding) / to_numeric(
                    dffundreq.revisedRequirements) * 100) + 0.5).astype(str)
            else:
                logger.info('Funding data lacks plan ids')
                dffundreq = dfreq
                dffundreq = drop_columns_except(dffundreq, columns_to_keep)
                dffundreq['totalFunding'] = ''
                dffundreq['percentFunded'] = ''
                dffund = drop_columns_except(dffund, columns_to_keep)
                dffund['percentFunded'] = ''
                dffund = dffund.fillna('')
                dffundreq = dffundreq.append(dffund)
        else:
            logger.warning('No funding data, only requirements data available')
            dffundreq = dfreq
            dffundreq['totalFunding'] = ''
            dffundreq['percentFunded'] = ''
    dffundreq[columnname] = code
    dffundreq.rename(columns={'name_x': 'name'}, inplace=True)
    dffundreq = drop_columns_except(dffundreq, columns_to_keep)
    dffundreq = drop_rows_with_col_word(dffundreq, 'name', 'test')
    dffundreq = drop_rows_with_col_word(dffundreq, 'name', 'Not specified')

    dffundreq.startDate = dffundreq.startDate.str[:10]
    dffundreq.endDate = dffundreq.endDate.str[:10]
    # convert floats to string and trim ( formatters don't work on columns with mixed types)
    remove_fractions(dffundreq, 'revisedRequirements')
    remove_nonenan(dffundreq, 'revisedRequirements')
    remove_fractions(dffundreq, 'totalFunding')
    remove_nonenan(dffundreq, 'totalFunding')
    dffundreq['id'] = dffundreq['id'].astype(str)
    remove_fractions(dffundreq, 'id')
    remove_nonenan(dffundreq, 'id')
    remove_fractions(dffundreq, 'percentFunded')
    remove_nonenan(dffundreq, 'percentFunded')
    dffundreq.rename(index=str, columns=rename_columns, inplace=True)

    filename = 'fts_requirements_funding_%s.csv' % code.lower()
    file_to_upload_hxldffundreq = join(folder, filename)

    resource_data = {
        'name': filename.lower(),
        'description': 'FTS Annual Requirements and Funding Data for %s' % name,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload_hxldffundreq)
    dataset.add_update_resource(resource)
    return dffundreq, planidcodemapping, incompleteplans, file_to_upload_hxldffundreq


def generate_emergency_dataset_and_showcase(base_url, downloader, folder, emergencyid, today, notes):
    # https://api.hpc.tools/v1/public/emergency/id/911
    latestyear = str(today.year)
    emergency_url = '%semergency/id/%d' % (base_url, emergencyid)
    data = download_data(emergency_url, downloader)
    name = data['name']
    glideid = data.get('glideId')
    date = data['date']
    slugified_name = slugify('FTS Funding Data for %s' % name).lower()
    title = '%s Funding Data' % name
    description = '%s  \n  \nGlide Id=%s, Date=%s' % (notes, glideid, date)
    showcase_url = 'https://fts.unocha.org/emergencies/%d/flows/%s' % (emergencyid, latestyear)
    dataset, showcase = get_dataset_and_showcase(slugified_name, title, description, today, name, showcase_url)
    dataset.add_other_location('world')
    funding_url = '%s/fts/flow?emergencyid=%d&year=%s' % (base_url, emergencyid, latestyear)
    fund_boundaries_info = generate_flows_resources(funding_url, downloader, folder, dataset, str(emergencyid), name,
                                                    latestyear)
    for fund_boundary_info in fund_boundaries_info:
        fund_boundary_info[0].to_csv(fund_boundary_info[1], encoding='utf-8', index=False, date_format='%Y-%m-%d')
    return dataset, showcase


def generate_dataset_and_showcase(base_url, downloader, folder, country, today, notes):
    '''
    api.hpc.tools/v1/public/fts/flow?countryISO3=CMR&Year=2016&groupby=cluster
    '''
    countryname = country['name']
    if countryname == 'World':
        logger.info('Ignoring  %s' % countryname)
        return None, None, None
    logger.info('Adding FTS data for %s' % countryname)
    latestyear = str(today.year)
    slugified_name = slugify('FTS Requirements and Funding Data for %s' % countryname).lower()
    title = '%s - Requirements and Funding Data' % countryname
    showcase_url = 'https://fts.unocha.org/countries/%s/flows/%s' % (country['id'], latestyear)
    dataset, showcase = get_dataset_and_showcase(slugified_name, title, notes, today, countryname, showcase_url)

    countryiso = country['iso3']
    if countryiso is None:
        logger.error('%s has a problem! Iso3 is None!' % title)
        return None, None, None
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.error('%s has a problem! %s' % (title, e))
        return None, None, None

    funding_url = '%sfts/flow?countryISO3=%s&year=%s' % (base_url, countryiso, latestyear)
    fund_boundaries_info = generate_flows_resources(funding_url, downloader, folder, dataset, countryiso.lower(),
                                                    countryname, latestyear)
    requirements_url = '%splan/country/%s' % (base_url, countryiso)
    funding_url = '%sfts/flow?groupby=plan&countryISO3=%s' % (base_url, countryiso)
    dffundreq, planidcodemapping, incompleteplans, file_to_upload_hxldffundreq = \
        generate_requirements_funding_resource(requirements_url, funding_url, downloader, folder, countryname,
                                               countryiso, 'countryCode', dataset)
    if dffundreq is None:
        if len(fund_boundaries_info) != 0:
            logger.error('We have latest year funding data but no overall funding data for %s' % title)
            for fund_boundary_info in fund_boundaries_info:
                fund_boundary_info[0].to_csv(fund_boundary_info[1], encoding='utf-8', index=False,
                                             date_format='%Y-%m-%d')
            return dataset, showcase, None
        logger.warning('No requirements or funding data available')
        return None, None, None

    def fill_row(planid, row):
        plan_url = '%splan/id/%s' % (base_url, planid)
        try:  # Added for Haiti missing plan 237 issue
            data = download_data(plan_url, downloader)
        except DownloadError:
            logger.error('Problem with downloading %s!' % plan_url)
            return False
        error = data.get('message')
        if error:
            raise FTSException(error)
        code = data['planVersion']['code']
        planidcodemapping[planid] = code
        row['code'] = code
        row['startDate'] = str(data['planVersion']['startDate'])[:10]
        row['endDate'] = str(data['planVersion']['endDate'])[:10]
        years = data['years']
        if len(years) > 1:
            logger.error('More than one year listed in plan %s for %s!' % (planid, countryname))
        row['year'] = years[0]['year']
        locations = data['locations']
        if len(locations) == 0:
            return True
        for location in data['locations']:
            adminlevel = location.get('adminlevel', location.get('adminLevel'))
            if adminlevel == 0 and location['iso3'] != countryiso:
                return True
        return False

    combined = DataFrame()
    for i, row in dffundreq.iterrows():
        planid = row['id']
        if planid == '' or planid == 'undefined':
            planname = row['name']
            if planname == 'Not specified' or planname == '':
                continue
            raise FTSException('Plan Name: %s is invalid!' % planname)
        else:
            if planid in incompleteplans:
                logger.warning('Not reading location and cluster info for plan id %s which is incomplete!' % planid)
                continue
            loc_funding_url = '%sfts/flow?planid=%s&groupby=location' % (base_url, planid)
            try:
                loc_data = download_data(loc_funding_url, downloader)
                loc_fund_objects = loc_data['report3']['fundingTotals']['objects']
                totalfunding = row['funding']
                try:
                    origfunding = int(totalfunding)
                except ValueError:
                    origfunding = None
                totalrequirements = row['requirements']
                try:
                    origrequirements = int(totalrequirements)
                except ValueError:
                    origrequirements = None
                if len(loc_fund_objects) != 0:
                    for location in loc_fund_objects[0]['objectsBreakdown']:
                        if Country.get_iso3_country_code_fuzzy(location['name'])[0] != countryiso:
                            continue
                        totalfunding = location['totalFunding']
                        if isinstance(totalfunding, int):
                            if origfunding != totalfunding:
                                #logger.warning('Overriding funding')
                                row['funding'] = totalfunding
                        break
                loc_req_objects = loc_data['requirements']['objects']
                if loc_req_objects:
                    for location in loc_req_objects:
                        if 'name' not in location:
                            logger.warning('%s requirements object does not have a location name!' % loc_funding_url)
                            continue
                        if Country.get_iso3_country_code_fuzzy(location['name'])[0] != countryiso:
                            continue
                        totalrequirements = location['revisedRequirements']
                        if isinstance(totalrequirements, int):
                            if origrequirements != totalrequirements:
                                #logger.warning('Overriding requirements for %s' % planid)
                                row['requirements'] = totalrequirements
                        break
                if totalrequirements:
                    if totalfunding == '':
                        row['percentFunded'] = ''
                    else:
                        totalrequirements_i = int(totalrequirements)
                        if totalrequirements_i == 0:
                            row['percentFunded'] = ''
                        else:
                            row['percentFunded'] = str(int((int(totalfunding) / totalrequirements_i * 100) + 0.5))
                else:
                    row['percentFunded'] = ''
            except DownloadError:
                logger.error('Problem with downloading %s!' % loc_funding_url)

            try:
                if fill_row(planid, row):
                    logger.warning('Plan %s spans multiple locations - ignoring in cluster breakdown!' % planid)
                    continue
            except FTSException as ex:
                logger.error(ex)
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

    for fund_boundary_info in fund_boundaries_info:
        fund_boundary_info[0]['destPlanCode'] = fund_boundary_info[0].destPlanId.map(planidcodemapping).fillna(fund_boundary_info[0].destPlanCode)
        fund_boundary_info[0].to_csv(fund_boundary_info[1], encoding='utf-8', index=False, date_format='%Y-%m-%d')

    years_url = '%sfts/flow?countryISO3=%s&groupby=year' % (base_url, countryiso)
    ## get totals from year call and subtract all plans in that year
    # 691121294 - 611797140 (2018 SDN)
    data = download_data(years_url, downloader)
    data = data['report3']['fundingTotals']['objects']
    if len(data) != 0:
        years_not_specified = list()
        for year_data in data[0].get('objectsBreakdown'):
            year = year_data.get('name')
            if year:
                year_url = '%sfts/flow?countryISO3=%s&year=%s' % (base_url, countryiso, year)
                data = download_data(year_url, downloader)
                if len(data['flows']) == 0:
                    continue
                totalfunding = data['incoming']['fundingTotal']
                funding_in_year = lookup_values_by_key(dffundreq, 'year', "'%s'" % year, 'funding')
                if funding_in_year.empty:
                    not_specified = str(int(totalfunding))
                else:
                    not_specified = str(int(totalfunding - to_numeric(funding_in_year, errors='coerce').sum()))
                if year == 'Not specified':
                    year = '1000'
                years_not_specified.append({'countryCode': countryiso, 'year': year, 'name': 'Not specified',
                                            'funding': not_specified})
        df_years_not_specified = DataFrame(data=years_not_specified, columns=list(dffundreq))
        df_years_not_specified = df_years_not_specified.fillna('')
        dffundreq = dffundreq.append(df_years_not_specified)

    dffundreq.sort_values(['year', 'endDate', 'name'], ascending=[False, False, True], inplace=True)
    dffundreq['year'] = dffundreq['year'].replace('1000', 'Not specified')
    hxldffundreq = hxlate(dffundreq, hxl_names)
    hxldffundreq.to_csv(file_to_upload_hxldffundreq, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    if len(combined) == 0:
        logger.warning('No cluster data available')
        return dataset, showcase, None

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
    hxl_resource = None
    filename = 'fts_requirements_funding_cluster_%s.csv' % countryiso.lower()
    resource_name = filename.lower()
    if not s[~s.isin(['Shared Funding', 'Multi-sector', 'Not specified'])].empty:
        s = df['percentFunded'] == ''
        if not s[~s.isin([True])].empty:
            hxl_resource = resource_name
    df.rename(index=str, columns=rename_columns, inplace=True)
    df = hxlate(df, hxl_names)

    file_to_upload = join(folder, filename)
    df.to_csv(file_to_upload, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': resource_name,
        'description': 'FTS Annual Requirements and Funding Data by Cluster for %s' % countryname,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)

    return dataset, showcase, hxl_resource
