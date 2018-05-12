import pandas as pd
import numpy as np
import glob, os
import sys
import re

from math import acos
from math import isnan
from math import sqrt
from math import pi
from math import hypot
from collections import OrderedDict

def get_data_from_source(src, events):
    events = events.sort_values(by='EventId')

    events_grouped = events.groupby('EventId').agg({
        'Systime': ['first', 'last'],
        'EventId': ['first']
    })
    events_grouped['EventId_flat'] = events_grouped['EventId']['first']
    del events_grouped['EventId']

    events_grouped['StartTime100'] = events_grouped['Systime']['first'] - 100
    events_grouped['EndTime100'] = events_grouped['Systime']['last'] + 100
    # print events_grouped['StartTime100']
    a = pd.DataFrame()


    a['x'] = events_grouped['StartTime100']
    a['y'] = events_grouped['Systime']['first']

    for c in range(0,2):
        # b.append(src[src.Systime >= a['x'] & src.Systime< = a["y"] ])
        b = src[(src['Systime'] >= a['x'].iloc[c]) & (src['Systime'] <= a['y'].iloc[c])]
        print b
        print "---------------------------"


    # b = pd.merge_asof(a, events_grouped, left_on='EventId', right_on='EventId_flat')

    sys.exit(0)
    del events_grouped['Systime']

    events = pd.merge_asof(events, events_grouped, left_on='EventId', right_on='EventId_flat')
    events = events.fillna(method='ffill')

    events['StartTime100_flat'] = events['StartTime100', '']
    events['EndTime100_flat'] = events['EndTime100', '']

    # sys.exit(0)

    events = events.sort_values('StartTime100_flat')

    merged_before = pd.merge_asof(events, src, left_on='StartTime100_flat', right_on='Systime')



    events = events.sort_values('EndTime100_flat')
    merged_after = pd.merge_asof(events, src, left_on='EndTime100_flat', right_on='Systime')

    events = events.sort_values('Systime')
    merged = pd.merge_asof(events, src, on='Systime')

    aggs_before = merged_before.groupby('EventId').agg({
        'EventId': 'first',
        'X_y': ['mean', 'std', 'max'],
        'Y_y': ['mean', 'std', 'max'],
        'Z': ['mean', 'std', 'max'],
        'Magnitude': ['mean', 'std', 'max']
    })
    aggs_before['EventID'] = aggs_before['EventId']['first']

    aggs = merged.groupby('EventId').agg({
        'EventId': 'first',
        'X_y': ['mean', 'std', 'max'],
        'Y_y': ['mean', 'std', 'max'],
        'Z': ['mean', 'std', 'max'],
        'Magnitude': ['mean', 'std', 'max']
    })
    aggs['EventID'] = aggs['EventId']['first']

    aggs_after = merged_after.groupby('EventId').agg({
        'EventId': 'first',
        'X_y': ['mean', 'std', 'max'],
        'Y_y': ['mean', 'std', 'max'],
        'Z': ['mean', 'std', 'max'],
        'Magnitude': ['mean', 'std', 'max']
    })
    aggs_after['EventID'] = aggs_after['EventId']['first']

    aggs = aggs.fillna(0)
    aggs_before = aggs_before.fillna(0)
    aggs_after = aggs_after.fillna(0)

    before_after = pd.merge(aggs_before, aggs_after, on='EventID', suffixes=("_before", "_after"))
    before_after['Xdiff'] = before_after['X_y_after']['mean'] - before_after['X_y_before']['mean']
    before_after['Ydiff'] = before_after['Y_y_after']['mean'] - before_after['Y_y_before']['mean']
    before_after['Zdiff'] = before_after['Z_after']['mean'] - before_after['Z_before']['mean']
    before_after['Mdiff'] = before_after['Magnitude_after']['mean'] - before_after['Magnitude_before']['mean']

    before_now = pd.merge(aggs_before, aggs, on='EventID', suffixes=("_before", "_now"))
    print before_after['X_y_before']['mean']
    before_now['Xdiff'] = before_now['X_y_now']['mean'] - before_now['X_y_before']['mean']
    before_now['Ydiff'] = before_now['Y_y_now']['mean'] - before_now['Y_y_before']['mean']
    before_now['Zdiff'] = before_now['Z_now']['mean'] - before_now['Z_before']['mean']
    before_now['Mdiff'] = before_now['Magnitude_now']['mean'] - before_now['Magnitude_before']['mean']
    before_now['Xmaxdiff'] = before_now['X_y_now']['max'] - before_now['X_y_before']['mean']
    before_now['Ymaxdiff'] = before_now['Y_y_now']['max'] - before_now['Y_y_before']['mean']
    before_now['Zmaxdiff'] = before_now['Z_now']['max'] - before_now['Z_before']['mean']
    before_now['Mmaxdiff'] = before_now['Magnitude_now']['max'] - before_now['Magnitude_before']['mean']

    res = pd.merge(before_now, before_after, on='EventID', suffixes=("_before_now", "_before_after"))
    del res['EventId_after']
    del res['EventId_before_before_after']
    del res['EventId_before_before_now']
    del res['EventId_now']
    del res['Magnitude_after']
    del res['Magnitude_before_before_after']
    del res['Magnitude_before_before_now']
    del res['X_y_after']
    del res['X_y_before_before_after']
    del res['X_y_before_before_now']
    del res['Y_y_after']
    del res['Y_y_before_before_after']
    del res['Y_y_before_before_now']
    del res['Z_after']
    del res['Z_before_before_after']
    del res['Z_before_before_now']

    return res


subfolders = glob.glob("100669/*/")

sessions = glob.glob("/home/tong/Desktop/stuff/*/")
for tl in sessions:
    num = int(re.search(r'/(\d*)/$', tl).group(1))
    for sf in glob.glob("/home/tong/Desktop/stuff/{}/*/".format(num)):
        print "Processing {}".format(sf)

        acc = None
        gyro = None
        magneto = None

        touch = None
        scroll = None
        try:
            acc = pd.read_csv('{}Accelerometer.csv'.format(sf), header=None,
                              names=['Systime', 'EventTime', 'ActivityId', 'X', 'Y', 'Z', 'PhoneOrientation'])
            gyro = pd.read_csv('{}Gyroscope.csv'.format(sf), header=None,
                               names=['Systime', 'EventTime', 'ActivityId', 'X', 'Y', 'Z', 'PhoneOrientation'])
            magneto = pd.read_csv('{}Magnetometer.csv'.format(sf), header=None,
                                  names=['Systime', 'EventTime', 'ActivityId', 'X', 'Y', 'Z', 'PhoneOrientation'])

            touch = pd.read_csv('{}TouchEvent.csv'.format(sf), header=None,
                                names=['Systime', 'EventId', 'ActivityId', 'PointerCount', 'PointerID',
                                       'ActionID', 'X', 'Y', 'Pressure', 'ContactSize', 'PhoneOrientation'])
            scroll = pd.read_csv('{}ScrollEvent.csv'.format(sf), header=None,
                                 names=['Systime', 'BeginTime', 'CurrentTime', 'ActivityId', 'EventId',
                                        'StartActionType',
                                        'StartX', 'StartY', 'StartPressure', 'StartSize', 'CurrentActionType', 'X',
                                        'Y', 'CurrentPressure', 'CurrentSize', 'DistanceX', 'DistanceY',
                                        'PhoneOrientation'])
        except IOError:
            print "Data file not found"
            continue

        acc['Magnitude'] = np.sqrt(acc['X'] * acc['X'] + acc['Y'] * acc['Y'] + acc['Z'] * acc['Z'])
        gyro['Magnitude'] = np.sqrt(gyro['X'] * gyro['X'] + gyro['Y'] * gyro['Y'] + gyro['Z'] * gyro['Z'])
        magneto['Magnitude'] = np.sqrt(
            magneto['X'] * magneto['X'] + magneto['Y'] * magneto['Y'] + magneto['Z'] * magneto['Z'])

        # get_data_from_source(acc, touch).to_csv("{}acc_touch_out.csv".format(sf))
        # get_data_from_source(gyro, touch).to_csv("{}gyro_touch_out.csv".format(sf))
        # get_data_from_source(magneto, touch).to_csv("{}magneto_touch_out.csv".format(sf))

        get_data_from_source(acc, scroll).to_csv("{}acc_scroll_out.csv".format(sf))
        # print get_data_from_source(acc, scroll)[]
        # get_data_from_source(gyro, scroll).to_csv("{}gyro_scroll_out.csv".format(sf))
        # get_data_from_source(magneto, scroll).to_csv("{}magneto_scroll_out.csv".format(sf))