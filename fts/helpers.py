from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase

funding_hxl_names = {
    'date': '#date',
    'budgetYear': '#date+year+budget',
    'description': '#description+notes',
    'amountUSD': '#value+funding+total+usd',
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
    'destUsageYearEnd': '#date+year+end+impl',
    'contributionType': '#financial+contribution+type',
    'flowType': '#financial+contribution+type',
    'method': '#financial+method',
    'boundary': '#financial+direction',
    'onBoundary': '#financial+direction+type',
    'status': '#status+text',
    'firstReportedDate': '#date+reported',
    'decisionDate': '#date+decision',
    'keywords': '#description+keywords',
    'originalAmount': '#value+funding+total',
    'originalCurrency': '#value+funding+total+currency',
    'exchangeRate': '#financial+fx',
    'id': '#activity+id+fts_internal',
    'refCode': '#activity+code',
    'createdAt': '#date+created',
    'updatedAt': '#date+updated',
}

hxl_names = {
    'countryCode': '#country+code',
    'id': '#activity+appeal+id+fts_internal',
    'name': '#activity+appeal+name',
    'code': '#activity+appeal+id+external',
    'typeId': '#activity+appeal+type+id+fts_internal',
    'typeName': '#activity+appeal+type+name',
    'startDate': '#date+start',
    'endDate': '#date+end',
    'year': '#date+year',
    'clusterCode': '#sector+cluster+code',
    'cluster': '#sector+cluster+name',
    'requirements': '#value+funding+required+usd',
    'funding': '#value+funding+total+usd',
    'percentFunded': '#value+funding+pct'
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


def get_dataset_and_showcase(slugified_name, title, description, today, country, showcase_url, additional_tags=list()):
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
    tags.extend(additional_tags)
    dataset.add_tags(tags)
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'FTS %s Summary Page' % country,
        'notes': 'Click the image on the right to go to the FTS funding summary page for %s' % country,
        'url': showcase_url,
        'image_url': 'https://fts.unocha.org/sites/default/files/styles/fts_feature_image/public/navigation_101.jpg'
    })
    showcase.add_tags(tags)
    return dataset, showcase