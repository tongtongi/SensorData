import glob
import os
import re
import sys
import time
from collections import OrderedDict
from math import acos
from math import hypot
from math import isnan
from math import pi
from math import sqrt

import numpy as np
import pandas as pd


def get_data_from_source(src, events):
    # events = events.sort_values(by='EventId')

    events_grouped = events.groupby('EventId').agg({
        'Systime': ['first', 'last'],
        'EventId': ['first']
    })
    events_grouped['EventId_flat'] = events_grouped['EventId']['first']
    del events_grouped['EventId']

    events_grouped['StartTime100'] = events_grouped['Systime']['first'] - 100
    events_grouped['StartTime'] = events_grouped['Systime']['first']
    events_grouped['EndTime'] = events_grouped['Systime']['last']
    events_grouped['EndTime100'] = events_grouped['Systime']['last'] + 100
    # merged = pd.merge_asof(events, src, on='Systime')

    del events_grouped['Systime']
    # print events_grouped
    # print src
    before = pd.DataFrame()
    after = pd.DataFrame()
    for num in range(0, len(events_grouped)):
        b = src[(src['Systime'] >= events_grouped['StartTime100'].iloc[num]) & (src['Systime'] <= events_grouped['StartTime'].iloc[num])]
        b['EventId'] = num

        c = src[(src['Systime'] >= events_grouped['EndTime'].iloc[num]) & (
                    src['Systime'] <= events_grouped['EndTime100'].iloc[num])]
        c['EventId'] = num
        before = before.append(b)
        after = after.append(c)

    before = before.groupby('EventId').agg({
        'EventId' : 'first',
        'X': [('beforeMeanX','mean')],
        'Y': [('beforeMeanY','mean')],
        'Z': [('beforeMeanZ','mean')],
        'Magnitude': [('beforeMeanM','mean')]
    })



    after = after.groupby('EventId').agg({
        'EventId' : 'first',
        'X': [('afterMeanX','mean')],
        'Y': [('afterMeanY','mean')],
        'Z': [('afterMeanZ','mean')],
        'Magnitude': [('afterMeanM','mean')]

    })

    before['EventID'] = before['EventId']['first']
    after['EventID'] = after['EventId']['first']
    before_after = pd.merge(before, after, on='EventID')




    before_after['Xdiff'] = before_after['X']['afterMeanX'] - before_after['X']['beforeMeanX']
    before_after['Ydiff'] = before_after['Y']['afterMeanY'] - before_after['Y']['beforeMeanY']
    before_after['Zdiff'] = before_after['Z']['afterMeanZ'] - before_after['Z']['beforeMeanZ']
    before_after['Mdiff'] = before_after['Magnitude']['afterMeanM'] - before_after['Magnitude']['beforeMeanM']
    # print before_after

    merged = pd.merge_asof(events, src, on='Systime')

    aggs = merged.groupby('EventId').agg({
        'EventId': 'first',
        'X_y': ['mean', 'std', 'max'],
        'Y_y': ['mean', 'std', 'max'],
        'Z': ['mean', 'std', 'max'],
        'Magnitude': ['mean', 'std', 'max']
    })
    aggs['EventID'] = aggs['EventId']['first']
    aggs = aggs.fillna(0)
    res = pd.merge(aggs, before_after, on='EventID', suffixes=("after_before", "_now"))

    res['before_nowXdiff'] = res['X_y']['mean'] - res['X']['beforeMeanX']
    res['before_nowYdiff'] = res['Y_y']['mean'] - res['Y']['beforeMeanY']
    res['before_nowZdiff'] = res['Z']['mean'] - res['Z']['beforeMeanZ']
    res['before_nowMdiff'] = res['Magnitude']['mean'] - res['Magnitude']['beforeMeanM']
    res['before_nowXmaxdiff'] = res['X_y']['max'] - res['X']['beforeMeanX']
    res['before_nowYmaxdiff'] = res['Y_y']['max'] - res['Y']['beforeMeanY']
    res['before_nowZmaxdiff'] = res['Z']['max'] - res['Z']['beforeMeanZ']
    res['before_nowMmaxdiff'] = res['Magnitude']['max'] -res['Magnitude']['beforeMeanM']


    # aggs = pd.concat([aggs, before], axis=1)
    # aggs = pd.concat([aggs, after], axis=1)

    res.drop(('X', 'beforeMeanX'), axis=1, inplace=True)
    res.drop(('Y', 'beforeMeanY'), axis=1, inplace=True)
    res.drop(('Z', 'beforeMeanZ'), axis=1, inplace=True)
    res.drop(('Magnitude', 'beforeMeanM'), axis=1, inplace=True)
    res.drop(('X', 'afterMeanX'), axis=1, inplace=True)
    res.drop(('Y', 'afterMeanY'), axis=1, inplace=True)
    res.drop(('Z', 'afterMeanZ'), axis=1, inplace=True)
    res.drop(('Magnitude', 'afterMeanM'), axis=1, inplace=True)
    res.drop(('EventId', 'first'), axis=1, inplace=True)
    res.drop(('EventId_y', 'first'), axis=1, inplace=True)

    return res



def get_data_from_sourceTouch(src, events):
    events = events.sort_values(by='EventId')
    events = events.sort_values(by='Systime')
    events_before = events.copy()
    events_before['Systime'] -= 100

    events_after = events.copy()
    events_after['Systime'] += 100

    events_after = events_after.sort_values('Systime')
    events_before = events_before.sort_values('Systime')

    merged_before = pd.merge_asof(events_before, src, on='Systime')
    merged = pd.merge_asof(events, src, on='Systime')
    merged_after = pd.merge_asof(events_after, src, on='Systime')

    aggs_before = merged_before.groupby('EventId').agg({
        'EventId': 'first',
        'X_y': ['mean', 'std', 'max'],
        'Y_y': ['mean', 'std', 'max'],
        'Z'  : ['mean', 'std', 'max'],
        'Magnitude': ['mean', 'std', 'max']
    })
    aggs_before['EventID'] = aggs_before['EventId']['first']

    aggs = merged.groupby('EventId').agg({
        'EventId': 'first',
        'X_y': ['mean', 'std', 'max'],
        'Y_y': ['mean', 'std', 'max'],
        'Z'  : ['mean', 'std', 'max'],
        'Magnitude': ['mean', 'std', 'max']
    })

    aggs['EventID'] = aggs['EventId']['first']

    aggs_after = merged_after.groupby('EventId').agg({
        'EventId': 'first',
        'X_y': ['mean', 'std', 'max'],
        'Y_y': ['mean', 'std', 'max'],
        'Z'  : ['mean', 'std', 'max'],
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




sessions = glob.glob("/home/tong/Desktop/stuff/*/")
start = time.time()
for tl in sessions:
    num = int(re.search(r'/(\d*)/$', tl).group(1))
    print num
    for sf in glob.glob("/home/tong/Desktop/stuff/{}/*/".format(num)):
        print "Processing {}".format(sf)

        acc = None
        gyro = None
        magneto = None

        touch = None
        scroll = None
        stroke = None
        try:
            # print ('{}Accelerometer.csv'.format(sf))
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

        if(len(touch) > 0):
            accTouch = get_data_from_sourceTouch(acc, touch)
            accTouch['UserId'] = num
            accTouch.to_csv("{}acc_touch_out.csv".format(sf))
            gyroTouch = get_data_from_sourceTouch(gyro, touch)
            gyroTouch['UserId'] = num
            gyroTouch.to_csv("{}gyro_touch_out.csv".format(sf))
            magnetoTouch = get_data_from_sourceTouch(magneto, touch)
            magnetoTouch['UserId'] = num
            magnetoTouch.to_csv("{}magneto_touch_out.csv".format(sf))

        if(len(scroll)>0):
            accScroll = get_data_from_source(acc, scroll)
            accScroll['UserId'] = num
            accScroll.to_csv("{}acc_scroll_out.csv".format(sf))
            gyroScroll = get_data_from_source(gyro, scroll)
            gyroScroll['UserId'] = num
            gyroScroll.to_csv("{}gyro_scroll_out.csv".format(sf))
            magnetoScroll = get_data_from_source(magneto, scroll)
            magnetoScroll['UserId'] = num
            magnetoScroll.to_csv("{}magneto_scroll_out.csv".format(sf))
