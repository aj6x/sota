#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 11:02:01 2024

@author: blaj
"""

import numpy as np
import pandas as pd
import requests
import time

fname = 'data/AJ6X_activator_20240813.csv'
sota_summits_url = 'https://www.sotadata.org.uk/summitslist.csv'
pota_parks_url = 'https://pota.app/all_parks_ext.csv'
peakbagger_url = 'https://www.peakbagger.com/'

pause = 2

def get_sota_summits():
    sota_summits_dtype = {
        'SummitCode': 'string',
        'AssociationName': 'string',
        'RegionName': 'string',
        'SummitName': 'string',
        'AltM': 'int',
        'AltFt': 'int',
        'GridRef1': 'string',
        'GridRef2': 'string',
        'Longitude': 'float',
        'Latitude': 'float',
        'Points': 'int',
        'BonusPoints': 'int',
        'ValidFrom': 'string',
        'ValidTo': 'string',
        'ActivationCount': 'int',
        'ActivationDate': 'string',
        'ActivationCall': 'string',
        }
    
    try:
        sota_summits = pd.read_csv('data/summitslist.csv',dtype='string')
    except:
        sota_summits = pd.read_csv(sota_summits_url,dtype='string')
        sota_summits.to_csv('data/summitslist.csv')
    finally:
        sota_summits = pd.read_csv('data/summitslist.csv',header=1,dtype=sota_summits_dtype)
    return sota_summits

def get_activator_log(fname):
    column_names = ['V2','MyCallsign','SummitCode','Date','Time','Band','Mode','Callsign','Unknown','Comment',]
    activator_log = pd.read_csv(fname,names=column_names,index_col=False).drop(columns=['V2',])
    return activator_log

def get_pota_parks():
    pota_parks_dtype = {
        'Index': 'int',
        'Reference': 'string',
        'Name': 'string',
        'Active': 'int',
        'EntityID': 'int',
        'locationDesc': 'string',
        'Latitude': 'float',
        'Longitude': 'float',
        'Grid': 'string',
        }
    
    try:
        pota_parks = pd.read_csv('data/parkslist.csv',dtype='string')
    except:
        pota_parks = pd.read_csv(pota_parks_url,dtype='string')
        pota_parks.to_csv('data/parkslist.csv')
    finally:
        pota_parks = pd.read_csv('data/parkslist.csv',header=1,names=list(pota_parks_dtype.keys()),dtype='string')
        pota_parks = pota_parks.drop(columns=['Index',])
    pota_parks = pota_parks.replace('Lake Tahoe Basin Management Unit National Forest','Lake Tahoe Basin Management Unit')
    return pota_parks

def parse_properties(html):
    peakbagger_properties = {
        'country':   '<tr><td valign=top>Country</td><td>',
        'state':     '<tr><td valign=top>State/Province</td><td>',
        'city':      '<tr><td valign=top>City/Town</td><td>',
        'ownership': '<tr><td valign=top>Ownership</td><td>',
        }
    peak = {}
    for key in peakbagger_properties:
        test = html.split(peakbagger_properties[key])
        if len(test)>1:
            peak[key] = test[1].split('</td>')[0]
        else:
            peak[key] = ''
    return peak
    
def get_peakbagger_info(summit,lat,lon):
    response = requests.get(url=f'{peakbagger_url}search.aspx?tid=R&lat={lat}&lon={lon}&ss=')
    pre='<th>Prom-Ft</th><th>Radius Search</th></tr><tr><td><a href="'
    post='">'
    peak_url = peakbagger_url+response.text.split(pre)[1].split(post)[0]
    time.sleep(np.random.rand()*pause)
    response = requests.get(peak_url)
    time.sleep(np.random.rand()*pause)
    peak = parse_properties(response.text)
    peak['summit'] = summit
    return peak

pota_parks = get_pota_parks()
sota_summits = get_sota_summits()
activator_log = get_activator_log(fname)
summit_list = activator_log['SummitCode'].unique()

peaks = []
for summit in summit_list:
    row = sota_summits.loc[sota_summits['SummitCode'] == summit]
    lat,lon = row.iloc[0][['Latitude','Longitude']]
    peak = get_peakbagger_info(summit,lat,lon)
    peaks.append(peak)
    print(summit,peak['ownership'])

#%%

for peak in peaks:
    owner = peak['ownership']
    own = ''; sa = ''
    if len(owner)>0:
        test1 = owner.split('Land: ')
        if len(test1)>1:
            test1 = test1[1]
            test2 = test1.split('<br/>Wilderness/Special Area: ')
            if len(test2)>1:
                own = test2[0]
                sa = test2[1]
            else:
                own = test1
    if len(own)>0:
        own = own.replace(' (Highest Point)','').split('/')
        pota_designators = []
        for item in own:
            row = pota_parks.loc[pota_parks['Name']==own[0]]
            if len(row)>0:
                pota_designators.append(row.iloc[0]['Reference'])
        peak['pota'] = pota_designators
    else:
        peak['pota'] = ''
        
    print(peak['summit'],peak['pota'],own)

#%%






 