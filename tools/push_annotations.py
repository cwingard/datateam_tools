#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Nov 10 2017

@author: lgarzio
@brief: This script is used to push new annotations or update existing annotations from a csv to uFrame via the M2M API
@usage:
anno_csv: csv with annotations to push to uFrame with headers: id, subsite(r), node, sensor, stream, method, parameters,
          beginDate(r), endDate, beginDT, endDT, exclusionFlag(r), qcFlag, SOURCE, annotation(r): (r) = required
SOURCE: email address to associate with annotation
USERNAME: USERNAME to access the OOI API
TOKEN: password to access the OOI API
URL: annotation endpoint
"""
import argparse
import ast
import json
import netCDF4 as nc
import netrc
import numpy as np
import os
import pandas as pd
import requests
import sys

from datetime import datetime
from pathlib import Path

# import credentials and set globals
nrc = netrc.netrc()
USERNAME, SOURCE, TOKEN = nrc.authenticators('ooinet.oceanobservatories.org')
URL = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/'
SESSION = requests.session()


def check_dates(beginDate, endDate):
    begin_DT = datetime.strptime(beginDate, '%Y-%m-%dT%H:%M:%SZ')
    beginDT = int(nc.date2num(begin_DT, 'seconds since 1970-01-01')*1000)
    try:
        end_DT = datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%SZ')
        endDT = int(nc.date2num(end_DT, 'seconds since 1970-01-01')*1000)
        if endDT >= beginDT:
            return beginDT, endDT
        else:
            raise Exception('beginDate (%s) is after endDate (%s)' % (begin_DT, end_DT))
    except (TypeError, ValueError):
        endDT = ''
        return beginDT, endDT


def check_exclusionFlag(exclusionFlag):
    if exclusionFlag:
        exclusionFlag = 1
    else:
        exclusionFlag = 0
    return exclusionFlag


def check_qcFlag(qcFlag):
    qcFlag_list = ['', 'not_operational', 'not_available', 'pending_ingest', 'not_evaluated', 'suspect', 'fail', 'pass']
    if qcFlag in qcFlag_list:
        return qcFlag
    else:
        raise Exception('Invalid qcFlag: %s' % qcFlag)


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    # initialize argument parser
    parser = argparse.ArgumentParser(description="""Set the source of annotations""",
                                     epilog="""Annotations CSV file""")

    # assign input arguments.
    parser.add_argument("-c", "--csvfile", dest="csvfile", type=Path, required=True)

    # parse the input arguments and create a parser object
    args = parser.parse_args(argv)

    # pull in the annotations csv
    anno_csv = args.csvfile

    # load the annotations
    df = pd.read_csv(anno_csv)
    df = df.replace(np.nan, '', regex=True)
    df['beginDate'] = pd.to_datetime(df['beginDate'])
    df['beginDate'] = df['beginDate'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    df['endDate'] = pd.to_datetime(df['endDate'])
    df['endDate'] = df['endDate'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['status_code'] = ''
    df['message'] = ''

    for index, row in df.iterrows():
        d = {'@class': '.AnnotationRecord'}
        d['subsite'] = row['subsite']
        d['node'] = row['node']
        d['sensor'] = row['sensor']
        d['stream'] = row['stream']
        d['method'] = row['method']
        if row['parameters'] == '':
            d['parameters'] = []
        else:
            d['parameters'] = ast.literal_eval(row['parameters'])

        beginDate = row['beginDate']
        endDate = row['endDate']
        beginDT, endDT = check_dates(beginDate, endDate)
        d['beginDT'] = beginDT
        d['endDT'] = endDT

        d['exclusionFlag'] = check_exclusionFlag(row['exclusionFlag'])
        d['qcFlag'] = check_qcFlag(row['qcFlag'])
        d['annotation'] = row['annotation']

        if row['source']:  # if source is specified in the csv, use that source
            d['source'] = row['source']
        else:  # if no source is specified, use the global source defined above
            d['source'] = SOURCE

        if row['id']:  # if an id is specified in the csv, update the annotation
            d['id'] = int(row['id'])
            jsond = json.dumps(d).replace('""', 'null')
            print(jsond)
            uurl = URL + str(int(row['id']))
            r = SESSION.put(uurl, data=jsond, auth=(USERNAME, TOKEN))
            response = r.json()

        if not row['id']:  # if no id is specified in the csv, creates a new annotation
            jsond = json.dumps(d).replace('""', 'null')
            print(jsond)
            r = SESSION.post(URL, data=jsond, auth=(USERNAME, TOKEN))
            response = r.json()

        df.loc[row.name, 'status_code'] = r.status_code
        df.loc[row.name, 'message'] = str(response['message'])
        try:
            df.loc[row.name, 'id'] = response['id']
        except KeyError:
            df.loc[row.name, 'id'] = ''

    base = os.path.splitext(anno_csv)[0]
    df.to_csv(os.path.abspath(base + '_run.csv'), index=False)


if __name__ == '__main__':
    main()
