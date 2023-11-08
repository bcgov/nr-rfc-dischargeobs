import os
import datetime
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import NRUtil.NRObjStoreUtil as NRObjStoreUtil

def isnumber(x):
    try:
        if str(float(x)) != 'nan':
            return True
        else:
            return False
    except:
        return False

def retrieve_xml_values(xml_file,var_names):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    output = pd.DataFrame(data=None,columns=var_names)
    for child in root.iter():
        if 'name' in child.attrib:
            if child.attrib['name'] in var_names:
                output.loc[0,child.attrib['name']] = child.attrib['value']
    return output

if __name__ == '__main__':
    ostore = NRObjStoreUtil.ObjectStoreUtil()
    default_date_format = '%Y%m%d'

    # default is to use today's date
    days_back = int(os.getenv('DEFAULT_DAYS_FROM_PRESENT', 0))
    #Github actions runs in UTC, convert to PST for script to work in github:
    current_date = datetime.datetime.now() - datetime.timedelta(hours=8) - datetime.timedelta(days=days_back)
    if days_back>0:
        current_date.replace(hour=23)


    stn_list_local = 'raw_data/ECCC_stationlist.csv'
    ostore.get_object(local_path=stn_list_local, file_path='RFC_DATA/ECCC/metadata/ECCC_stationlist.csv')
    stn_metadata = pd.read_csv(stn_list_local)
    stn_list = stn_metadata.TC_ID

    # config local file path
    local_file_path_root = os.getenv('MPOML_DATA_DIR', f'./raw_data')
    # the sync function will automatically prepend the date onto the local path
    local_file_path = os.path.join(local_file_path_root, 'ECCC_hourly')
    local_file_date_fmt = '%Y%m%d'
    if not os.path.exists(local_file_path):
        os.makedirs(local_file_path)


    var_names = ['air_temp','avg_air_temp_pst1hr','pcpn_amt_pst1hr']

    dt_txt = current_date.strftime('%Y%m%d')
    dt_range = pd.date_range(start = current_date.strftime('%Y/%m/%d 00:00'), end = current_date.strftime('%Y/%m/%d 23:00'), freq = 'H')
    #Conver dt_range to UTC since data on datamart is in UTC:
    #Limit dt_range_utc to < current time so that it is not trying to grab non-existant data:
    dt_range_utc = dt_range[0:current_date.hour+1] + datetime.timedelta(hours=8)
    stn_str = 'C' + stn_list

    local_data_fpath = f'raw_data/ECCC_hourly/{dt_txt}.parquet'
    all_data_objfolder = 'RFC_DATA/ECCC/hourly/parquet/'
    all_data_objpath = os.path.join(all_data_objfolder,f'{dt_txt}.parquet')
    all_data_objs = ostore.list_objects(all_data_objfolder,return_file_names_only=True)
    if all_data_objpath in all_data_objs:
        ostore.get_object(local_path=local_data_fpath, file_path=all_data_objpath)
        output = pd.read_parquet(local_data_fpath)
    else:
        output_ind = pd.MultiIndex.from_product([stn_str,dt_range], names=["Station", "DateTime"])
        output = pd.DataFrame(data=None,index=output_ind,columns=var_names+['f_read'])


    for stn in stn_str:
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

            if output.loc[(stn,dt - datetime.timedelta(hours=8)),'f_read']!=True:
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
                    output.loc[(stn,dt - datetime.timedelta(hours=8)),var_names] = retrieve_xml_values(local_filename,var_names).values
                    output.loc[stn,dt - datetime.timedelta(hours=8)].iloc[-1] = True
            

    output.to_parquet(local_data_fpath)
    ostore.put_object(local_path=local_data_fpath, ostore_path=all_data_objpath)

    TA = output.loc[:,'air_temp'].unstack(0)
    PC = output.loc[:,'pcpn_amt_pst1hr'].unstack(0)



    PC[~PC.applymap(isnumber)] = ''

    local_folder = 'processed_data/'
    obj_folder = 'RFC_DATA/ECCC/hourly/csv/'
    TA_local = os.path.join(local_folder,f'TA_{dt_txt}.csv')
    PC_local = os.path.join(local_folder,f'PC_{dt_txt}.csv')
    TA_obj = os.path.join(obj_folder,f'TA_{dt_txt}.csv')
    PC_obj = os.path.join(obj_folder,f'PC_{dt_txt}.csv')
    TA.columns = TA.columns.str[1:]
    PC.columns = PC.columns.str[1:]

    TA.to_csv(TA_local)
    PC.to_csv(PC_local)

    ostore.put_object(local_path=TA_local, ostore_path=TA_obj)
    ostore.put_object(local_path=PC_local, ostore_path=PC_obj)

