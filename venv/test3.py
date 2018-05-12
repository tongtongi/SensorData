from math import acos
from math import isnan
from math import sqrt
from math import pi
from math import hypot
from collections import OrderedDict
import pandas as pd
import numpy as np
import glob, os
import time
import re
#
# accDf = pd.read_csv('/home/tong/Desktop/100669/100669_session_1/Accelerometer.csv', header=None)
# touchDf = pd.read_csv('/home/tong/Desktop/100669/100669_session_1/TouchEvent.csv', header=None)
#
# accDf[len(accDf.columns)] = np.sqrt(accDf[3]*accDf[3] + accDf[4]*accDf[4] + accDf[5]*accDf[5])
#
# touchDf
#
# merged = pd.merge_asof(touchDf, accDf, on=0)
#
# last = merged.groupby('1_x').agg({
#     '3_y': ['mean', 'std'],
#     '4_y': ['mean', 'std'],
#     '5_y': ['mean', 'std'],
#     '7_y': ['mean', 'std']})
#
# last = last.fillna(0)
# last.to_csv("/home/tong/Desktop/allah.csv", sep=',', encoding='utf-8')
# print last


def get_data_from_source(src, events):
    events_before = events.copy()
    events_before['Systime'] -= 100

    events_after = events.copy()
    events_after['Systime'] += 100

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

subfolders = glob.glob("100669/*/")

sessions = glob.glob("/home/tong/Desktop/stuff/*/")
for tl in sessions:
    num2 = int(re.search(r'/(\d*)/$', tl).group(1))
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
                names=['Systime', 'BeginTime', 'CurrentTime', 'ActivityId', 'EventId', 'StartActionType',
                'StartX', 'StartY', 'StartPressure', 'StartSize', 'CurrentActionType', 'X',
                'Y', 'CurrentPressure', 'CurrentSize', 'DistanceX', 'DistanceY', 'PhoneOrientation'])
        except IOError:
            print "Data file not found"
            continue

        acc['Magnitude'] = np.sqrt(acc['X']*acc['X'] + acc['Y']*acc['Y'] + acc['Z']*acc['Z'])
        gyro['Magnitude'] = np.sqrt(gyro['X']*gyro['X'] + gyro['Y']*gyro['Y'] + gyro['Z']*gyro['Z'])
        magneto['Magnitude'] = np.sqrt(magneto['X']*magneto['X'] + magneto['Y']*magneto['Y'] + magneto['Z']*magneto['Z'])

        get_data_from_source(acc, touch).to_csv("{}acc_touch_out.csv".format(sf))
        get_data_from_source(gyro, touch).to_csv("{}gyro_touch_out.csv".format(sf))
        get_data_from_source(magneto, touch).to_csv("{}magneto_touch_out.csv".format(sf))

        get_data_from_source(acc, scroll).to_csv("{}acc_scroll_out.csv".format(sf))
        get_data_from_source(gyro, scroll).to_csv("{}gyro_scroll_out.csv".format(sf))
        get_data_from_source(magneto, scroll).to_csv("{}magneto_scroll_out.csv".format(sf))