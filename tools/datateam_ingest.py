#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author Mike Smith
@email michaesm@marine.rutgers.edu
change username, api_key, api_token, and csv_path (lines 242 - 265)
"""

import glob
import netrc
import os
import pickle
import pprint
import re
import requests
import urllib.request, urllib.error, urllib.parse

import datetime as dt
import netCDF4 as nc
import pandas as pd

from pandas.io.json import json_normalize

# initialize requests session
HTTP_STATUS_OK = 200
HEADERS = {
    'Content-Type': 'application/json'
}


def file_date():
    date_cutoff = input('Please enter file cutoff date in the form (yyyy-mm-dd): ')
    reg_ex = re.compile(r'\d{4}-\d{2}-\d{2}')

    if date_cutoff:
        if reg_ex.match(date_cutoff) is None:
            print('Incorrect date format entered.')
            date_cutoff = file_date()
    else:
        'No date entered.'
        date_cutoff = file_date()
    return date_cutoff


def csv_select(csvs, serve):
    df = pd.DataFrame()
    recovered = [x for x in csvs if 'R000' in x]
    telemetered = [x for x in csvs if 'D000' in x]
    telemetered.sort(reverse=True)
    csvs = telemetered + recovered
    if 'telemetered' in serve:
        print('Please select csv(s) for active telemetered ingestion. Latest telemetered deployment CSV listed first')
        for csv in telemetered:
            yes = input('Ingest {}? y/<n>: '.format(csv)) or 'n'
            if 'y' in yes:
                t_df = load_ingestion_sheet(csv)
                t_df['username'] = username
                t_df['deployment'] = get_deployment_number(csv)
                t_df['type'] = 'TELEMETERED'
                t_df['state'] = 'RUN'
                t_df['priority'] = priority
                begin = input('Set a beginning file date to exclude any files modified before this date? y/<n>') or 'n'
                if 'y' in begin:
                    t_df['beginFileDate'] = file_date()
                df = df.append(t_df, ignore_index=True)
            else:
                continue
    else:
        print('Please select csv(s) for recovered ingestion.')
        for csv in csvs:
            yes = input('Ingest {}? <y>/n: '.format(csv)) or 'y'
            if 'y' in yes:
                t_df = load_ingestion_sheet(csv)
                t_df['username'] = username
                t_df['deployment'] = get_deployment_number(csv)
                t_df['type'] = 'RECOVERED'
                t_df['state'] = 'RUN'
                t_df['priority'] = priority
                end = input('Set an ending file date to exclude files modified after this date? y/<n>') or 'n'
                if 'y' in end:
                    end_date = file_date()
                    t_df['endFileDate'] = end_date
                df = df.append(t_df, ignore_index=True)
            else:
                continue

    if df.empty:
        print('At least one csv must be selected. Please select csv')
        df = csv_select(csvs, serve)
    return df


def change_recurring_ingestion(key, token, recurring):
    state_change = pd.DataFrame()
    if not recurring.empty:
        print('Recurring ingestions found for these reference designators.')
        print(recurring)
        state = input('Persist, cancel, or suspend these ingests? <persist>/cancel/suspend: ') or 'persist'
        if state in ('cancel', 'suspend'):
            for i in recurring.iter():
                r = change_ingest_state(base_url, key, token, i, state.upper())
                if r.status_code == HTTP_STATUS_OK:
                    state_json = r.json()
                else:
                    print(('Status Code: {}, Response: {}'.format(r.status_code, r.content)))
                    continue
                tdf = pd.DataFrame([state_json], columns=list(state_json.keys()))
                state_change = state_change.append(tdf, ignore_index=True)
                print(state_change)

    return state_change


def get_active_ingestions(url, key, token):
    r = requests.get('{}/api/m2m/12589/ingestrequest/jobcounts?active=true&groupBy=refDes,status'.format(url),
                     auth=(key, token))
    if r.ok:
        return r
    else:
        pass


def get_active_ingestions_for_refdes(url, key, token, ref_des):
    r = requests.get('{}/api/m2m/12589/ingestrequest/jobcounts?refDes={}&groupBy=refDes,status'.format(url, ref_des),
                     auth=(key, token))
    if r.ok:
        return r
    else:
        pass


def get_all_ingest_requests(url, key, token):
    r = requests.get('{}/api/m2m/12589/ingestrequest/'.format(url), auth=(key, token))
    if r.ok:
        return r
    else:
        pass


def build_ingest_dict(ingest_info):
    option_dict = {}
    keys = list(ingest_info.keys())

    adict = {k: ingest_info[k] for k in ('parserDriver', 'fileMask', 'dataSource', 'deployment',
                                         'refDes', 'refDesFinal') if k in ingest_info}
    request_dict = dict(username=ingest_info['username'],
                        state=ingest_info['state'],
                        ingestRequestFileMasks=[adict],
                        type=ingest_info['type'],
                        priority=ingest_info['priority'])

    for k in ['beginFileDate', 'endFileDate']:
        if k in keys:
            option_dict[k] = ingest_info[k]

    if option_dict:
        request_dict['options'] = dict(option_dict)

    return request_dict


def ingest_data(url, key, token, data_dict):
    r = requests.post('{}/api/m2m/12589/ingestrequest/'.format(url), json=data_dict, headers=HEADERS, auth=(key, token))
    if r.ok:
        return r
    else:
        pass


def change_ingest_state(url, key, token, ingest_id, state):
    r = requests.put('{}/api/m2m/12589/ingestrequest/{}'.format(url, ingest_id),
                     json=dict(id=ingest_id, state=state),
                     auth=(key, token))
    if r.ok:
        return r
    else:
        pass


def check_ingest_request(url, key, token, ingest_id):
    r = requests.get('{}/api/m2m/12589/ingestrequest/{}'.format(url, ingest_id),
                     auth=(key, token))
    if r.ok:
        return r
    else:
        pass


def check_ingest_file_status(url, key, token, ingest_id):
    r = requests.get('{}/api/m2m/12589/ingestrequest/jobcounts?ingestRequestId={}&groupBy=status'.format(url, ingest_id),
                     auth=(key, token))
    if r.ok:
        return r
    else:
        pass


def purge_data(url, key, token, ref_des_dict):
    r = requests.put('{}/api/m2m/12589/ingestrequest/purgerecords'.format(url),
                     json=ref_des_dict,
                     auth=(key, token))
    if r.ok:
        return r
    else:
        pass


def uframe_routes():
    """
    This function loads a pickle file containing all uframe_routes to their proper drivers
    :return: dictionary containing the uframe_routes to driver
    :rtype: dictionary
    """
    fopen = urllib.request.urlopen('https://raw.githubusercontent.com/ooi-data-review/parse_spring_files/master/uframe_routes.pkl')
    ingest_dict = pickle.load(fopen)
    return ingest_dict


def load_ingestion_sheet(csv):
    t_df = pd.read_csv(csv, usecols=[0, 1, 2, 3])
    return t_df


def get_deployment_number(csv):
    split_csv = csv.split('_')
    deployment_number = int(re.sub('.*?([0-9]*)$', r'\1', split_csv[1]))
    return deployment_number


def splitall(path):
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts


desired_width = 320
pd.set_option('display.width', desired_width)

# Load uframe_routes from github.
ingest_dict = uframe_routes()

# Initialize empty Pandas DataFrames
purge_df = pd.DataFrame()
ingest_df = pd.DataFrame()
recurring = pd.DataFrame()

# OOINet base URL and credentials
base_url = 'https://ooinet.oceanobservatories.org'
cred = netrc.netrc()
api_key, username, api_token = cred.authenticators('ooinet.oceanobservatories.org')

priority = 3
csv_path = os.path.abspath('C:/Users/cwingard/Documents/GitHub/ingestion-csvs')

# Avoid these platforms right now
cabled = ['RS', 'CE02SHBP', 'CE02SHSP', 'CE04OSBP', 'CE04OSPD', 'CE04OSPS']
cabled_reg_ex = re.compile('|'.join(cabled))

# Enter OOINet username. Doesn't need to be an email
username = input('Enter ooinet username: {} '.format(username)) or username
api_key = input('Enter ooinet key: {} '.format(api_key)) or api_key
api_token = input('Enter ooinet token: {} '.format(api_token)) or api_token

serve = input('\nIs this a recovered or telemetered (recurring) ingest? <recovered>/telemetered: ') or 'recovered'
arrays = input('\nSelect an array for ingestion - <CE>, CP, GA, GI, GS, GP: ') or 'CE'
arrays = arrays.upper().split(', ')
reg_ex = re.compile('|'.join(arrays))
result = [y for x in os.walk(csv_path) for y in glob.glob(os.path.join(x[0], '*.csv')) if reg_ex.search(y) and not cabled_reg_ex.search(y)]
platforms = [splitall(x)[-2] for x in result]
platforms = set(platforms)
platforms = list(platforms)
platforms.sort()

print('The following platforms from the previously selected arrays have ingestion csvs.')
for platform in platforms:
    print(platform)
selected_platform = input('\nPlease select (comma separated) the platform(s) that you would like to ingest: ')
selected_platform = selected_platform.upper().split(', ')
reg_ex = re.compile('|'.join(selected_platform))
csvs = [y for x in os.walk(csv_path) for y in glob.glob(os.path.join(x[0], '*.csv')) if reg_ex.search(y)]
csvs.sort()

df = csv_select(csvs, serve)
df = df.sort_values(['deployment', 'reference_designator'])
df = df.rename(columns={'filename_mask': 'fileMask', 'reference_designator': 'refDes', 'data_source': 'dataSource',
                        'parser': 'parserDriver'})
df = df[pd.notnull(df['fileMask'])]

unique_ref_des = list(pd.unique(df.refDes.ravel()))
unique_ref_des.sort()

# Get ingestion requests with ID so that we can check if we are ingesting data that already has a recurring ingestion
all_ingest = get_all_ingest_requests(base_url, api_key, api_token)
all_ingest = all_ingest.json()

# change dictionary keys because there are duplicate key names in the dictionary
for l in all_ingest:
    l['ingest_status'] = l.pop('status')
    l['ingest_id'] = l.pop('id')
    l['entryDateStr'] = nc.num2date(l['entryDate'], 'milliseconds since 1970-01-01').strftime('%Y-%m-%d %H:%M:%S')
    l['modifiedDateStr'] = nc.num2date(l['modifiedDate'], 'milliseconds since 1970-01-01').strftime('%Y-%m-%d %H:%M:%S')

# load the json into a pandas dataframe
all_ingest = json_normalize(all_ingest, 'ingestRequestFileMasks', ['username', 'type', 'ingest_status', 'ingest_id',
                                                                   'state', 'priority', 'entryDateStr',
                                                                   'modifiedDateStr'])
all_ingest = pd.concat([all_ingest.drop(['refDes'], axis=1), all_ingest['refDes'].apply(pd.Series)], axis=1)
all_ingest['refDes'] = all_ingest['subsite'] + '-' + all_ingest['node'] + '-' + all_ingest['sensor']
all_ingest = all_ingest.drop(['node', 'sensor', 'subsite'], axis=1)
df_telemetered = all_ingest.loc[(all_ingest['state'] == 'RUN') & (all_ingest['type'] == 'TELEMETERED')]
df_telemetered['changed_status'] = None
df_telemetered['purged'] = None

print('\nUnique Reference Designators in these ingestion CSV files')
for rd in unique_ref_des:
    print(rd)
    recurring = recurring.append(df_telemetered[df_telemetered['refDes'] == rd], ignore_index=True)

yes = input('\nPurge reference designators from the system? y/<n>: ') or 'n'

if 'y' in yes:
    print('\nThese reference designators may have ongoing, recurring ingests which MUST be ' +
          'cancelled/suspended before purging.')
    state_change = change_recurring_ingestion(api_key, api_token, recurring)
    for rd in unique_ref_des:
        split = rd.split('-')
        ref_des = dict(subsite=split[0], node=split[1], sensor='{}-{}'.format(split[2], split[3]))
        purge_info = purge_data(base_url, api_key, api_token, ref_des)
        purge_json = purge_info.json()
        tdf = pd.DataFrame([purge_json], columns=list(purge_json.keys()))
        tdf['ReferenceDesignator'] = rd
        purge_df = purge_df.append(tdf)
    print('Purge Completed')
    print(purge_df)
else:
    if 'telemetered' in serve:
        state_change = change_recurring_ingestion(api_key, api_token, recurring)

print('\nProceeding with data ingestion\n')

# add refDesFinal
wcard_refdes = ['GA03FLMA-RIM01-02-CTDMOG000', 'GA03FLMB-RIM01-02-CTDMOG000',
                'GI03FLMA-RIM01-02-CTDMOG000', 'GI03FLMB-RIM01-02-CTDMOG000',
                'GP03FLMA-RIM01-02-CTDMOG000', 'GP03FLMB-RIM01-02-CTDMOG000',
                'GS03FLMA-RIM01-02-CTDMOG000', 'GS03FLMB-RIM01-02-CTDMOG000']

df['refDesFinal'] = ''
pp = pprint.PrettyPrinter(indent=2)
for row in df.iterrows():

    if '#' in row[1]['parserDriver']:
        continue
    elif row[1]['parserDriver']:
        rd = row[1].refDes
        if rd in wcard_refdes:
            # the CTDMO decoder will be invoked
            row[1].refDesFinal = 'false'
        else:
            # the CTDMO decoder will not be invoked
            row[1].refDesFinal = 'true'

        ingest_dict = build_ingest_dict(row[1].to_dict())
        pp.pprint(ingest_dict)
        review = input('Review ingest request. Is this correct? <y>/n: ') or 'y'
        if 'y' in review:
            r = ingest_data(base_url, api_key, api_token, ingest_dict)
            print(r)
            ingest_json = r.json()
            tdf = pd.DataFrame([ingest_json], columns=list(ingest_json.keys()))
            tdf['ReferenceDesignator'] = row[1]['refDes']
            tdf['state'] = row[1]['state']
            tdf['type'] = row[1]['type']
            tdf['deployment'] = row[1]['deployment']
            tdf['username'] = row[1]['username']
            tdf['priority'] = row[1]['priority']
            tdf['refDesFinal'] = row[1]['refDesFinal']
            tdf['fileMask'] = row[1]['fileMask']
            ingest_df = ingest_df.append(tdf)
        else:
            print('Skipping this ingest request')
            continue
    else:
        continue

print(ingest_df)
utc_time = dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

purge_df.to_csv('{}_purged.csv'.format(utc_time), index=False)
ingest_df.to_csv('{}_ingested.csv'.format(utc_time), index=False)
