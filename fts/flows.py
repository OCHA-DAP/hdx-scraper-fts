import logging

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import multiple_replace

from fts.helpers import country_all_columns_to_keep, rename_columns, funding_hxl_names

logger = logging.getLogger(__name__)

srcdestmap = {'sourceObjects': 'src', 'destinationObjects': 'dest'}


class Flows:
    def __init__(self, downloader, locations, planidcodemapping):
        self.downloader = downloader
        self.locations = locations
        self.planidcodemapping = planidcodemapping

    def flatten_objects(self, objs, shortened, newrow):
        objinfo_by_type = dict()
        plan_id = None
        destPlanId = None
        for obj in objs:
            objtype = obj['type']
            objinfo = objinfo_by_type.get(objtype, dict())
            for key in obj:
                if objtype == 'Plan' and key == 'id':
                    plan_id = obj[key]
                    dict_of_lists_add(objinfo, key, plan_id)
                    if shortened == 'dest':
                        destPlanId = plan_id
                if key not in ['type', 'behavior', 'id']:
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
                            iso3 = self.locations.get_countryiso_from_name(country)
                            if iso3:
                                iso3s.append(iso3)
                        values = iso3s
                    outputstr = ','.join(sorted(values))
                else:
                    if len(values) > 1:
                        outputstr = 'Multiple'
                        logger.error('Multiple used instead of %s for %s in %s (%s)' % (values, keyname, plan_id, shortened))
                    else:
                        outputstr = values[0]
                if keyname in country_all_columns_to_keep:
                    newrow[keyname] = outputstr
            return destPlanId

    def generate_flows_resources(self, folder, dataset, latestyear, country):
        fund_boundaries_info = dict()
        fund_data = list()
        base_funding_url = 'fts/flow?locationid=%s&' % country['id']
        funding_url = self.downloader.get_url(f'{base_funding_url}year={latestyear}')
        while funding_url:
            json = self.downloader.download(url=funding_url)
            fund_data.extend(json['data']['flows'])
            funding_url = json['meta'].get('nextLink')

        for row in fund_data:
            newrow = dict()
            destPlanId = None
            for key in row:
                if key == 'reportDetails':
                    continue
                value = row[key]
                shortened = srcdestmap.get(key)
                if shortened:
                    destPlanId = self.flatten_objects(value, shortened, newrow)
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
                renamed_column = rename_columns.get(key)
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
            newrow['destPlanCode'] = self.planidcodemapping.get(destPlanId, '')
            boundary = row['boundary']
            rows = fund_boundaries_info.get(boundary, list())
            rows.append(newrow)
            fund_boundaries_info[boundary] = rows

        for boundary, rows in fund_boundaries_info.items():
            rows = sorted(rows, key=lambda k: k['date'], reverse=True)
            headers = rows[0].keys()
            filename = 'fts_%s_funding_%s.csv' % (boundary, country['iso3'].lower())
            resourcedata = {
                'name': filename,
                'description': 'FTS %s Funding Data for %s for %s' % (boundary.capitalize(), country['name'], latestyear),
                'format': 'csv'
            }
            dataset.generate_resource_from_iterator(headers, rows, funding_hxl_names, folder, filename, resourcedata)
        return len(fund_boundaries_info)