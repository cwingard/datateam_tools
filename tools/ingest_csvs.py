#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import netrc
import os
import pprint
import re
import requests
import sys

import datetime as dt
import pandas as pd

from pathlib import Path

# initialize requests session
HTTP_STATUS_OK = 200
HEADERS = {
    'Content-Type': 'application/json'
}
PRIORITY = 3

# initialize user credentials and the OOINet base URL
BASE_URL = 'https://ooinet.oceanobservatories.org'
credentials = netrc.netrc()
API_KEY, USERNAME, API_TOKEN = credentials.authenticators('ooinet.oceanobservatories.org')

def load_ingest_sheet(ingest_csv, ingest_type):
    df = pd.read_csv(ingest_csv, usecols=[0, 1, 2, 3])
    df['username'] = USERNAME
    df['deployment'] = get_deployment_number(ingest_csv)
    df['state'] = 'RUN'
    df['priority'] = PRIORITY
    if 'telemetered' in ingest_type:
        df['type'] = 'TELEMETERED'
    else:
        df['type'] = 'RECOVERED'

    return df

def get_deployment_number(csv):
    split_csv = csv.split('_')
    deployment_number = int(re.sub('.*?([0-9]*)$', r'\1', split_csv[1]))
    return deployment_number

def get_active_ingestions_for_refdes(url, key, token, ref_des):
    r = requests.get('{}/api/m2m/12589/ingestrequest/jobcounts?refDes={}&groupBy=ingestRequestId,status'.format(url, ref_des),
                     auth=(key, token))
    if r.ok:
        return r
    else:
        pass

def change_recurring_ingestion(key, token, recurring):
    state_change = pd.DataFrame()
    if not recurring.empty:
        print('Recurring ingestions found for these reference designators.')
        print(recurring)
        state = input('Persist, cancel, or suspend these ingests? <persist>/cancel/suspend: ') or 'persist'
        if state in ('cancel', 'suspend'):
            for i in recurring.iter():
                r = change_ingest_state(BASE_URL, key, token, i, state.upper())
                if r.status_code == HTTP_STATUS_OK:
                    state_json = r.json()
                else:
                    print(('Status Code: {}, Response: {}'.format(r.status_code, r.content)))
                    continue
                tdf = pd.DataFrame([state_json], columns=list(state_json.keys()))
                state_change = state_change.append(tdf, ignore_index=True)
                print(state_change)

    return state_change

def change_ingest_state(url, key, token, ingest_id, state):
    r = requests.put('{}/api/m2m/12589/ingestrequest/{}'.format(url, ingest_id),
                     json=dict(id=ingest_id, state=state),
                     auth=(key, token))
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

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    # initialize argument parser
    parser = argparse.ArgumentParser(description="""Sets the source file for the ingests and the type""")

    # assign input arguments.
    parser.add_argument("-c", "--csvfile", dest="csvfile", type=Path, required=True)
    parser.add_argument("-t", "--ingest_type", dest="ingest_type", type=str, choices=('recovered', 'telemetered'),
                        required=True)

    # parse the input arguments and create a parser object
    args = parser.parse_args(argv)

    # assign the annotations type and csv file
    ingest_csv = args.csvfile
    ingest_type = args.ingest_type

    # Initialize empty Pandas DataFrames
    pd.set_option('display.width', 1600)
    ingest_df = pd.DataFrame()

    # load the csv file for the ingests
    df = load_ingest_sheet(ingest_csv, ingest_type)
    df = df.sort_values(['deployment', 'reference_designator'])
    df = df.rename(columns={'filename_mask': 'fileMask', 'reference_designator': 'refDes',
                            'data_source': 'dataSource', 'parser': 'parserDriver'})
    df = df[pd.notnull(df['fileMask'])]

    unique_ref_des = list(pd.unique(df.refDes.ravel()))
    unique_ref_des.sort()

    # set cabled platforms to exclude from this process, those use a different method
    cabled = ['RS', 'CE02SHBP', 'CE04OSBP', 'CE04OSPD', 'CE04OSPS']
    cabled_reg_ex = re.compile('|'.join(cabled))
    cabled_ref_des = []
    for rd in unique_ref_des:
        if re.match(cabled_reg_ex, rd):
            cabled_ref_des.append(rd)

    # if the list of unique reference designators contains cabled instruments,
    # remove them from further consideration (they use a different system)
    if cabled_ref_des:
        for x in cabled_ref_des:
            unique_ref_des = [s for s in unique_ref_des if s != x]
            df.drop(df[df['refDes'] == x].index, inplace=True)

    # if all of the reference designators were for cabled systems, we are done
    if df.empty:
        print('Removed cabled array reference designators from the ingestion, no other systems left.')
        return None

    # add refDesFinal
    wcard_refdes = ['GA03FLMA-RIM01-02-CTDMOG000', 'GA03FLMB-RIM01-02-CTDMOG000',
                    'GI03FLMA-RIM01-02-CTDMOG000', 'GI03FLMB-RIM01-02-CTDMOG000',
                    'GP03FLMA-RIM01-02-CTDMOG000', 'GP03FLMB-RIM01-02-CTDMOG000',
                    'GS03FLMA-RIM01-02-CTDMOG000', 'GS03FLMB-RIM01-02-CTDMOG000']

    df['refDesFinal'] = ''
    pp = pprint.PrettyPrinter(indent=2)
    for row in df.iterrows():
        # skip commented out entries
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
                r = ingest_data(BASE_URL, API_KEY, API_TOKEN, ingest_dict)
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

    # save the results
    print(ingest_df)
    utc_time = dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    ingest_df.to_csv('{}_ingested.csv'.format(utc_time), index=False)

if __name__ == '__main__':
    main()
