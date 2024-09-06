#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 19 22:22:35 2024

@author: Gabriel AJ6X
"""

import argparse
import pandas as pd

parser = argparse.ArgumentParser(prog="AJ6X SOTA to POTA", description='Automatic conversion of SOTA logs (in csv format) to POTA logs (including S2S resulting in P2P info).', epilog='Outputs: individual files for each activated POTA park.')
parser.add_argument("--activator", default='', type=str, help="Activator file name downloaded from https://www.sotadata.org.uk/en/logs/activator > Download complete log")
parser.add_argument("--s2s", default='', type=str, help="S2S file name downloaded from https://www.sotadata.org.uk/en/logs/s2s > Download complete log")
parser.add_argument("--date", default='00000000', type=str, help="Earliest date to include in POTA format: YYYYMMDD. default 00000000 (everything)")
args = parser.parse_args()

def get_activator_log(fname):
    column_names = ['Version','MyCallsign','SummitCode','Date','Time','Band','Mode','Callsign','Unknown','Comment',]
    activator_log = pd.read_csv(fname,names=column_names,index_col=False).drop(columns=['Version','Unknown',])
    activator_log['Comment'] = activator_log['Comment'].fillna('')
    return activator_log

def get_s2s_log(fname):
    column_names = ['Version','MyCallsign','SummitCode','Date','Time','Band','Mode','Callsign','OtherSummit','Comment','ChaserPoints','ActivatorPoints',]
    activator_log = pd.read_csv(fname,names=column_names,index_col=False).drop(columns=['Version',])
    activator_log['Comment'] = activator_log['Comment'].fillna('')
    return activator_log

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
        'ParkName': 'string',
        'Pota': 'string',
        }
    sota_summits = pd.read_csv('data/sota_pota.csv',header=0,dtype=sota_summits_dtype)
    sota_summits['Pota'] = sota_summits['Pota'].fillna('')
    return sota_summits

def uniques_nn(mylist):
    uniquelist = []
    for item in mylist:
        if (len(item)>0) and (item not in uniquelist):
            uniquelist.append(item)
    return uniquelist

freq_to_band = {
    '1.8MHz' :'160M',
    '3.5MHz' :'80M',
    '5MHz'   :'60M',
    '7MHz'   :'40M',
    '10MHz'  :'30M',
    '14MHz'  :'20M',
    '18MHz'  :'17M',
    '21MHz'  :'15M',
    '24MHz'  :'12M',
    '28MHz'  :'10M',
    '50MHz'  :'6M',
    '144MHz' :'2M',
    '433MHz' :'70CM',
    '1240MHz':'23CM',
    }

def mode_fix(mode):
    if mode == 'DATA':
        mode = 'FT8'
    elif mode == 'DV':
        mode = 'DIGITALVOICE'
    return mode

sota_to_pota_dict = {
    'MyCallsign' : 'OPERATOR',
    'Callsign'   : 'CALL',
    'Date'       : 'QSO_DATE',   # 8-digit number in YYYYMMDD format
    'Time'       : 'TIME_ON',    #entered as HHMM
    'Band'       : 'BAND',
    'Mode'       : 'MODE',
    #'State'     : 'MY_STATE',  # confusing for now. Exact location from PB data?
    }

pota_keys = ['OPERATOR', 'QSO_DATE', 'TIME_ON', 'BAND', 'MODE', 'CALL', 'MY_SIG', 'MY_SIG_INFO', 'SIG', 'SIG_INFO'];

def sota_to_pota_date(date):
    day,month,year = date.split('/')
    return f'{int(year):04d}{int(month):02d}{int(day):02d}'

def sota_to_pota_time(time):
    hour,minute = time.split(':')
    return f'{int(hour):02d}{int(minute):02d}'

def export_adif(dataframe,filename):
    with open(filename,'w+') as file_handle:
        file_handle.write('AJ6X ADIF SOTA to POTA Conversion\n<PROGRAMID:9>AJ6X SOTA\n<PROGRAMVERSION:5>0.0.1\n\n<EOH>\n\n')
        nqsos = len(dataframe)
        for iqso in range(nqsos):
            qso = dataframe.iloc[iqso].to_dict()
            record = ''
            for key in qso.keys():
                value = qso[key]
                if len(value)>0:
                    record += f'<{key}:{len(value)}>{value} '
            record += '<EOR>\n'
            file_handle.write(record)
    
activator_log = get_activator_log(args.activator)
s2s_log = get_s2s_log(args.s2s)
sota_summits = get_sota_summits()

print('\nCollecting POTA QSOs:',flush=True)
pota_qsos_list = []
for irow in range(len(activator_log)):
    qso = activator_log.iloc[irow].to_dict()
    summit = qso['SummitCode']
    potas = sota_summits.loc[sota_summits['SummitCode']==summit]['Pota'].iloc[0]
    sigs = ''
    if len(potas)>0:
        qso['MY_SIG']='POTA'
        check_s2s = s2s_log.loc[(s2s_log['Callsign']==qso['Callsign'])&(s2s_log['Date']==qso['Date'])&(s2s_log['Time']==qso['Time'])&(s2s_log['Band']==qso['Band'])&(s2s_log['Mode']==qso['Mode'])]
        if len(check_s2s)>0:
            remote_summit = check_s2s.iloc[0]['OtherSummit']
            remote_potas = sota_summits.loc[sota_summits['SummitCode']==remote_summit]['Pota'].iloc[0]
            sigs = uniques_nn(remote_potas.split('/'))
        my_sigs = uniques_nn(potas.split('/'))
        for my_sig in my_sigs:
            qso1 = qso.copy()
            qso1['MY_SIG_INFO'] = my_sig
            if len(sigs) == 0:
                pota_qsos_list.append(qso1)
                #print(qso)
            else:
                qso1['SIG'] = 'POTA'
                for sig in sigs: 
                    qso2 = qso1.copy()
                    qso2['SIG_INFO'] = sig
                    pota_qsos_list.append(qso2)
    if (irow+1)%32 == 0:
        print('.',end='',flush=True)
print(f'\nDone. Found {len(pota_qsos_list)} QSO x POTA combinations.',flush=True)

pota_qsos = pd.DataFrame(pota_qsos_list).rename(columns=sota_to_pota_dict)[pota_keys]
pota_qsos['BAND'] = pota_qsos['BAND'].map(freq_to_band)
pota_qsos['QSO_DATE'] = pota_qsos['QSO_DATE'].map(sota_to_pota_date)
pota_qsos['TIME_ON'] = pota_qsos['TIME_ON'].map(sota_to_pota_time)
pota_qsos['MODE'] = pota_qsos['MODE'].map(mode_fix)
pota_qsos['SIG_INFO'] = pota_qsos['SIG_INFO'].fillna('')
pota_qsos['SIG'] = pota_qsos['SIG'].fillna('')

#keep only QSOs after args.date
pota_qsos = pota_qsos.loc[pota_qsos['QSO_DATE']>=args.date]

print('\nSaving POTA log files:',flush=True)
activated_parks = uniques_nn(pota_qsos['MY_SIG_INFO'].values)
for activated_park in activated_parks:
    park_qsos = pota_qsos.loc[pota_qsos['MY_SIG_INFO']==activated_park]
    operators = uniques_nn(park_qsos['OPERATOR'].values)
    for operator in operators:
        operator_park_qsos = park_qsos.loc[park_qsos['OPERATOR']==operator]
        first_date = operator_park_qsos['QSO_DATE'].values.min()
        filename = f'{operator.replace('/','-')}@{activated_park}-{first_date}.adi'
        export_adif(operator_park_qsos,'out/'+filename)
        print(f'\t{operator} \t{activated_park} \t=> \t{filename}',flush=True)
print('Done.\n73 de AJ6X\n')


