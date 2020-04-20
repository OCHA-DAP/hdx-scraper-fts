from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase

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
    'emergency_id': '#crisis+code',
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
columnlookup = {'location': 'countryCode', 'emergency': 'emergency_id'}
urllookup = {'location': 'locationid', 'emergency': 'emergencyid'}


class FTSException(Exception):
    pass


def download(url, downloader):
    r = downloader.download(url)
    json = r.json()
    status = json['status']
    if status != 'ok':
        raise FTSException('%s gives status %s' % (url, status))
    return json


def download_data(url, downloader):
    return download(url, downloader)['data']


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