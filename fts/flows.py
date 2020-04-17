import logging
from collections import OrderedDict
from os.path import join

from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import multiple_replace
from pandas import DataFrame
from pandas.io.json import json_normalize

from fts.helpers import urllookup, download, country_all_columns_to_keep, rename_columns, funding_hxl_names
from fts.pandas_helpers import drop_columns_except, hxlate

logger = logging.getLogger(__name__)


def generate_flows_resources(objecttype, base_url, downloader, folder, dataset, code, name, latestyear):
    fund_boundaries_info = list()
    fund_data = list()
    base_funding_url = '%sfts/flow?%s=%s&' % (base_url, urllookup[objecttype], code)
    funding_url = '%syear=%s' % (base_funding_url, latestyear)
    while funding_url:
        json = download(funding_url, downloader)
        fund_data.extend(json['data']['flows'])
        funding_url = json['meta'].get('nextLink')

    dffunddet = json_normalize(fund_data)

    def add_objects(objectName):
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
                prefix = '%s%s' % (objectName, objectType)
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
                                                                                       name))
                        else:
                            outputstr = values[0]
                    outputdicts[keyname] = outputstr
            return outputdicts

        typedicts = dffunddet['%sObjects' % objectName].apply(flatten_objects)
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
            filename = 'fts_%s_funding_%s.csv' % (boundary, code.lower())
            fund_boundaries_info.append((dffundbound, join(folder, filename)))

            resource_data = {
                'name': filename,
                'description': 'FTS %s Funding Data for %s for %s' % (boundary.capitalize(), name, latestyear),
                'format': 'csv'
            }
            resource = Resource(resource_data)
            resource.set_file_to_upload(fund_boundaries_info[-1][1])
            dataset.add_update_resource(resource)
    return fund_boundaries_info


def generate_flows_files(fund_boundaries_info, planidcodemapping):
    for fund_boundary_info in fund_boundaries_info:
        fund_boundary_info[0]['destPlanCode'] = fund_boundary_info[0].destPlanId.map(planidcodemapping).fillna(fund_boundary_info[0].destPlanCode)
        fund_boundary_info[0].to_csv(fund_boundary_info[1], encoding='utf-8', index=False, date_format='%Y-%m-%d')