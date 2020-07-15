import logging
from collections import OrderedDict
from os.path import join

from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import multiple_replace
from pandas import DataFrame, json_normalize

from fts.helpers import urllookup, download, country_all_columns_to_keep, rename_columns, funding_hxl_names

logger = logging.getLogger(__name__)

srcdestmap = {'sourceObjects': 'src', 'destinationObjects': 'dest'}


def flatten_objects(objs, shortened, newrow):
    objinfo_by_type = dict()
    for obj in objs:
        objtype = obj['type']
        objinfo = objinfo_by_type.get(objtype, dict())
        for key in obj:
            if key not in ['type', 'behavior', 'id'] or (objtype == 'Plan' and key == 'id'):
                value = obj[key]
                if isinstance(value, list):
                    for element in value:
                        dict_of_lists_add(objinfo, key, element)
                else:
                    dict_of_lists_add(objinfo, key, value)
        objinfo_by_type[objtype] = objinfo
    for objtype in objinfo_by_type:
        prefix = '%s%s' % (shortened, objtype)
        for key in objinfo_by_type[objtype]:
            keyname = '%s%s' % (prefix, key.capitalize())
            values = objinfo_by_type[objtype][key]
            replacements = {'OrganizationOrganization': 'Organization', 'Name': '', 'types': 'Types',
                            'code': 'Code'}
            keyname = multiple_replace(keyname, replacements)
            if 'UsageYear' in keyname:
                values = sorted(values)
                newrow['%sStart' % keyname] = values[0]
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
                                                                               name))
                else:
                    outputstr = values[0]
            newrow[keyname] = outputstr


def generate_flows_resources(base_url, downloader, folder, dataset, code, name, latestyear, filecode):
    fund_boundaries_info = list()
    fund_data = list()
    base_funding_url = '%sfts/flow?locationid=%s&' % (base_url, code)
    funding_url = '%syear=%s' % (base_funding_url, latestyear)
    while funding_url:
        json = download(funding_url, downloader)
        fund_data.extend(json['data']['flows'])
        funding_url = json['meta'].get('nextLink')

    rows = list()
    for row in fund_data:
        newrow = dict()
        for key in row:
            if key == 'reportDetails':
                continue
            value = row[key]
            shortened = srcdestmap.get(key)
            if shortened:
                flatten_objects(value, shortened, newrow)
                continue
            if key == 'keywords':
                if value:
                    newrow[key] = ','.join(value)
                else:
                    newrow[key] = ''
                continue
            if key in ['date', 'firstReportedDate', 'decisionDate', 'createdAt', 'updatedAt']:
                if value:
                    newrow[key] = value[:10]
                else:
                    newrow[key] = ''
                continue
            renamed_column = rename_columns[key]
            if renamed_column:
                newrow[renamed_column] = value
                continue
            if key in country_all_columns_to_keep:
                newrow[key] = value
        if 'originalAmount' not in newrow:
            newrow['originalAmount'] = ''
        if 'originalCurrency' not in newrow:
            newrow['originalCurrency'] = ''
        if 'refCode' not in newrow:
            newrow['refCode'] = ''
        rows.append(newrow)
    rows = sorted(rows, key=lambda k: k['date'], reverse=True)


    #
    #     typedicts = dffunddet['%sObjects' % objectName].apply(flatten_objects)
    #     return dffunddet.join(DataFrame(list(typedicts)))
    #
    # if 'sourceObjects' in dffunddet:
    #     dffunddet = add_objects('source')
    #     dffunddet = add_objects('destination')
    #
    #     def get_keywords(x):
    #         if x:
    #             return ','.join(x)
    #         else:
    #             return ''
    #
    #     dffunddet['keywords'] = dffunddet.keywords.apply(get_keywords)
    #     if 'originalAmount' not in dffunddet:
    #         dffunddet['originalAmount'] = ''
    #     if 'originalCurrency' not in dffunddet:
    #         dffunddet['originalCurrency'] = ''
    #     if 'refCode' not in dffunddet:
    #         dffunddet['refCode'] = ''
    #     dffunddet = drop_columns_except(dffunddet, country_all_columns_to_keep)
    #     dffunddet.sort_values('date', ascending=False, inplace=True)
    #     dffunddet.date = dffunddet.date.str[:10]
    #     dffunddet.firstReportedDate = dffunddet.firstReportedDate.str[:10]
    #     dffunddet.decisionDate = dffunddet.decisionDate.str[:10]
    #     dffunddet.createdAt = dffunddet.createdAt.str[:10]
    #     dffunddet.updatedAt = dffunddet.updatedAt.str[:10]
    #     dffunddet.rename(index=str, columns=rename_columns, inplace=True)
    #
    #     for boundary, dffundbound in dffunddet.groupby(['boundary']):
    #         # add HXL tags
    #         dffundbound = hxlate(dffundbound, funding_hxl_names)
    #         filename = 'fts_%s_funding_%s.csv' % (boundary, filecode.lower())
    #         fund_boundaries_info.append((dffundbound, join(folder, filename)))
    #
    #         resource_data = {
    #             'name': filename,
    #             'description': 'FTS %s Funding Data for %s for %s' % (boundary.capitalize(), name, latestyear),
    #             'format': 'csv'
    #         }
    #         resource = Resource(resource_data)
    #         resource.set_file_to_upload(fund_boundaries_info[-1][1])
    #         dataset.add_update_resource(resource)
    return None #fund_boundaries_info


def generate_flows_files(fund_boundaries_info, planidcodemapping):
    for fund_boundary_info in fund_boundaries_info:
        fund_boundary_info[0]['destPlanCode'] = fund_boundary_info[0].destPlanId.map(planidcodemapping).fillna(fund_boundary_info[0].destPlanCode)
        fund_boundary_info[0].to_csv(fund_boundary_info[1], encoding='utf-8', index=False, date_format='%Y-%m-%d')