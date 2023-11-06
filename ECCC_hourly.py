import os
import datetime
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np

default_date_format = '%Y%m%d'

# default is to use today's date
default_days_from_present = int(os.getenv('DEFAULT_DAYS_FROM_PRESENT', 0))
current_date = datetime.datetime.now() - datetime.timedelta(days=default_days_from_present)

# config local file path

local_file_path_root = os.getenv('MPOML_DATA_DIR', f'./raw_data')
# the sync function will automatically prepend the date onto the local path
local_file_path = os.path.join(local_file_path_root, 'ECCC_hourly')
local_file_date_fmt = '%Y%m%d'
if not os.path.exists(local_file_path):
    os.makedirs(local_file_path)


var_names = ['air_temp','avg_air_temp_pst1hr','pcpn_amt_pst1hr']

dt_range = pd.date_range(start = current_date.strftime('%Y/%m/%d 00:00'), end = current_date.strftime('%Y/%m/%d 23:00'), freq = 'H')
dt_range_utc = dt_range.tz_localize('US/Pacific').tz_convert('UTC')
US/Pacific.replace(tzinfo=None)

stn_str = ['CYYJ']
output_ind = pd.MultiIndex.from_product([stn_str,dt_range], names=["Station", "DateTime"])
output = pd.DataFrame(data=None,index=output_ind,columns=var_names)

stn = stn_str[0]
for dt in dt_range_utc:
    dt_str = dt.strftime('%Y-%m-%d-%H00')
    date_str = dt.strftime(default_date_format)
    remote_location=f'http://hpfx.collab.science.gc.ca/{date_str}/WXO-DD/observations/swob-ml/{date_str}/'
    fname_man = f'{stn}/{dt_str}-{stn}-MAN-swob.xml'
    fname_auto = f'{stn}/{dt_str}-{stn}-AUTO-swob.xml'
    web_path_auto = os.path.join(remote_location,fname_auto)
    web_path_man = os.path.join(remote_location,fname_man)
    local_filename = os.path.join(local_file_path,f'{stn}-{dt_str}.xml')

            #Download file and write to local file name:
    with requests.get(web_path_auto, stream=True) as r:
        #r.raise_for_status()
        if r.status_code == requests.codes.ok:
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
        else:
            with requests.get(web_path_man, stream=True) as r2:
                if r2.status_code == requests.codes.ok:
                    with open(local_filename, 'wb') as f:
                        for chunk in r2.iter_content(chunk_size=8192): 
                            f.write(chunk)
    if os.path.exists(local_filename):
        output.loc[stn,dt.tz_convert('US/Pacific').replace(tzinfo=None)] = retrieve_xml_values(local_filename,var_names).values
    


    

def retrieve_xml_values(xml_file,var_names):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    output = pd.DataFrame(data=None,columns=var_names)
    for child in root.iter():
        if 'name' in child.attrib:
            if child.attrib['name'] in var_names:
                output.loc[0,child.attrib['name']] = child.attrib['value']
    return output


