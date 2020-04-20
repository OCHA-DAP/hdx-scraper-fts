import copy
import logging
from os.path import join

from hdx.data.resource import Resource
from hdx.utilities.downloader import DownloadError
from pandas import DataFrame, json_normalize, to_numeric, Series

from fts.helpers import download_data, country_emergency_columns_to_keep, rename_columns, FTSException, urllookup, \
    columnlookup, hxl_names
from fts.pandas_helpers import drop_columns_except, drop_rows_with_col_word, remove_fractions, remove_nonenan, \
    lookup_values_by_key, hxlate

logger = logging.getLogger(__name__)


def generate_requirements_funding(plans, base_funding_url, downloader, name, code, columnname):
    planidcodemapping = dict()
    funding_url = '%sgroupby=plan' % base_funding_url
    fund_data = download_data(funding_url, downloader)
    data = fund_data['report3']['fundingTotals']['objects']
    if len(data) == 0:
        fund_data = None
    else:
        fund_data = data[0].get('objectsBreakdown')
    columns_to_keep = copy.deepcopy(country_emergency_columns_to_keep)
    columns_to_keep.insert(0, columnname)
    if len(plans) == 0:
        incompleteplans = list()
        if not fund_data:
            return None, planidcodemapping, incompleteplans
        logger.warning('No requirements data, only funding data available')
        dffund = json_normalize(fund_data)
        dffund = drop_columns_except(dffund, columns_to_keep)
        dffund['percentFunded'] = ''
        dffund = dffund.fillna('')
        dffundreq = dffund
    else:
        dfreq = json_normalize(plans)
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
    return dffundreq, planidcodemapping, incompleteplans


def row_correction_requirements_funding(objecttype, base_url, downloader, dffundreq, all_plans, incompleteplans, planidcodemapping, name, code):
    planids = list()
    for i, row in dffundreq.iterrows():
        planid = row['id']
        if planid == '' or planid == 'undefined':
            planname = row['name']
            if planname == 'Not specified' or planname == '':
                continue
            raise FTSException('Plan Name: %s is invalid!' % planname)
        if planid in incompleteplans:
            logger.warning('Not reading %s info for plan id %s which is incomplete!' % (objecttype, planid))
            continue

        data = all_plans.get(planid)
        if data is None:
            logger.error('Missing plan id %s!' % planid)
            continue
        error = data.get('message')
        if error:
            logger.error(error)
            continue
        planversioncode = data['planVersion']['code']
        planidcodemapping[planid] = planversioncode
        dffundreq.at[i, 'code'] = planversioncode
        dffundreq.at[i, 'startDate'] = str(data['planVersion']['startDate'])[:10]
        dffundreq.at[i, 'endDate'] = str(data['planVersion']['endDate'])[:10]
        years = data['years']
        if len(years) > 1:
            logger.error('More than one year listed in plan %s for %s!' % (planid, name))
        dffundreq.at[i, 'year'] = years[0]['year']

        funding_url = '%sfts/flow?planid=%s&groupby=%s' % (base_url, planid, objecttype)
        try:
            data = download_data(funding_url, downloader)
            fund_objects = data['report3']['fundingTotals']['objects']
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
            if len(fund_objects) != 0:
                for object in fund_objects[0]['objectsBreakdown']:
                    if 'id' not in object:
                        continue
                    if str(object['id']) != code:
                        continue
                    totalfunding = object['totalFunding']
                    if isinstance(totalfunding, int):
                        if origfunding != totalfunding:
                            #logger.warning('Overriding funding')
                            dffundreq.at[i, 'funding'] = totalfunding
                    break
            req_objects = data['requirements']['objects']
            if req_objects:
                for object in req_objects:
                    if 'name' not in object:
                        logger.warning('%s requirements object does not have a %s name!' % (funding_url, objecttype))
                        continue
                    if 'id' not in object:
                        continue
                    if str(object['id']) != code:
                        continue
                    totalrequirements = object['revisedRequirements']
                    if isinstance(totalrequirements, int):
                        if origrequirements != totalrequirements:
                            #logger.warning('Overriding requirements for %s' % planid)
                            dffundreq.at[i, 'requirements'] = totalrequirements
                    break
            if totalrequirements:
                if totalfunding == '':
                    dffundreq.at[i, 'percentFunded'] = ''
                else:
                    totalrequirements_i = int(totalrequirements)
                    if totalrequirements_i == 0:
                        dffundreq.at[i, 'percentFunded'] = ''
                    else:
                        dffundreq.at[i, 'percentFunded'] = str(int((int(totalfunding) / totalrequirements_i * 100) + 0.5))
            else:
                dffundreq.at[i, 'percentFunded'] = ''
        except DownloadError:
            logger.error('Problem with downloading %s!' % funding_url)
        planids.append(planid)

    return dffundreq, planids


def add_not_specified(base_funding_url, downloader, code, columnname, dffundreq):
    years_url = '%sgroupby=year' % base_funding_url
    ## get totals from year call and subtract all plans in that year
    # 691121294 - 611797140 (2018 SDN)
    data = download_data(years_url, downloader)
    data = data['report3']['fundingTotals']['objects']
    if len(data) != 0:
        years_not_specified = list()
        for year_data in data[0].get('objectsBreakdown'):
            year = year_data.get('name')
            if year:
                year_url = '%syear=%s' % (base_funding_url, year)
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
                years_not_specified.append({columnname: code, 'year': year, 'name': 'Not specified',
                                            'funding': not_specified})
        df_years_not_specified = DataFrame(data=years_not_specified, columns=list(dffundreq))
        df_years_not_specified = df_years_not_specified.fillna('')
        dffundreq = dffundreq.append(df_years_not_specified)

    dffundreq.sort_values(['year', 'endDate', 'name'], ascending=[False, False, True], inplace=True)
    dffundreq['year'] = dffundreq['year'].replace('1000', 'Not specified')
    return dffundreq


def generate_requirements_funding_resource(objecttype, base_url, all_plans, plans, downloader, folder, name, code, dataset, outputcode):
    base_funding_url = '%sfts/flow?%s=%s&' % (base_url, urllookup[objecttype], code)
    columnname = columnlookup[objecttype]
    dffundreq, planidcodemapping, incompleteplans = generate_requirements_funding(plans, base_funding_url, downloader, name, outputcode, columnname)
    if dffundreq is None:
        return None, None, planidcodemapping, incompleteplans
    dffundreq, planids = row_correction_requirements_funding(objecttype, base_url, downloader, dffundreq, all_plans, incompleteplans, planidcodemapping, name, code)

    dffundreq = add_not_specified(base_funding_url, downloader, outputcode, columnname, dffundreq)

    hxldffundreq = hxlate(dffundreq, hxl_names)
    filename = 'fts_requirements_funding_%s.csv' % outputcode.lower()
    file_to_upload_hxldffundreq = join(folder, filename)
    hxldffundreq.to_csv(file_to_upload_hxldffundreq, encoding='utf-8', index=False, date_format='%Y-%m-%d')

    resource_data = {
        'name': filename.lower(),
        'description': 'FTS Annual Requirements and Funding Data for %s' % name,
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(file_to_upload_hxldffundreq)
    dataset.add_update_resource(resource)
    return dffundreq, planids, planidcodemapping, incompleteplans