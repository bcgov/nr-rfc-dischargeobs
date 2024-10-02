import pandas as pd
import numpy as np
import requests
import constants
import os
import datetime
import NRUtil.NRObjStoreUtil as NRObjStoreUtil
import pyarrow
from dateutil.parser import parse
import dataretrieval.nwis as nwis
import minio.error
import logging
import logging.config

LOGGER = logging.getLogger(__name__)

#Jan/Feb/Mar: output to instant1 file, Apr/May/Jun: output to instant2 file, etc.
#Open instant file or create it if it doesn't exist

#To Do:
#Save raw data to object store with data and time. Check whether data is newer than existing data on objectstore before downloading
#Create new files in object store if they do not already exist (save_instantaneous_data)
#Ability to import multiple days worth of data in case of issue (e.g. datamart outage)
#Ensure code is flexible to easily allow addition/removal of stations
#Automatically grab data from alternative file sources if needed


#Download WSC data from datamart:
def download_WSC_data(dest_folder):
    #Loop through datamart file paths listed in constants.py file:
    for fname in constants.SOURCE_HYDRO_DATA:
        #Use filename (removing remainder of url) for saving file locally:
        local_filename = os.path.join(dest_folder,fname.split("/")[-1])
        #Download file and write to local file name:
        LOGGER.info(f"Downloading {fname} to {local_filename}") 
        with requests.get(fname, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)

#Set custom start/end download dates?
def download_USGS_data():
    current_datetime = datetime.datetime.now()
    start_datetime = current_datetime.replace(second=0, hour=0, minute=0) - datetime.timedelta(days=2)
    current_date_text = current_datetime.strftime('%Y-%m-%d')
    start_date_text = start_datetime.strftime('%Y-%m-%d')

    #Load USGS station list from csv:
    USGS_stn_list = pd.read_csv('USGS_station_list.csv')

    # specify the USGS site code for which we want data.
    #sites = ['12401500','12404500']
    RFC_ID = USGS_stn_list['BC RFC ID']
    sites = [str.replace('U', '00') for str in RFC_ID]
    # get instantaneous values (iv)
    LOGGER.info(f"Downloading USGS data for sites: {sites}")
    df = nwis.get_record(sites=sites, service='iv', start=start_date_text, end=current_date_text)
    #Select discharge data only (parameter 00060), unstack stations to seperate columns, and convert cfs to cms:
    Q_df = round(df['00060'].unstack(level='site_no')/35.3147,3)
    #Select stage data only (parameter 00065), unstack stations to seperate columns, and convert feet to metres:
    H_df = round(df['00065'].unstack(level='site_no')/3.28084,3)
    Q_df.index = Q_df.index.tz_convert('US/Pacific').tz_localize(None)
    H_df.index = H_df.index.tz_convert('US/Pacific').tz_localize(None)
    #Remove duplicated values (duplicate index will cause error in later steps)
    #Note daylight savings results in duplicate values. Consider alternate approach
    Q_df = Q_df[~Q_df.index.duplicated()]
    H_df = H_df[~H_df.index.duplicated()]
    return Q_df, H_df

def download_provincial_data(dest_folder):
    for fname in constants.PROV_HYDRO_SRC:
        #Use filename (removing remainder of url) for saving file locally:
        local_filename = os.path.join(dest_folder, fname.split("/")[-1])
        #Download file and write to local file name:
        LOGGER.info(f"Downloading {fname} to {local_filename}")
        with requests.get(fname, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)

def format_provincial_data(src_file):
    #Set columns of dataframes containing station ID, data values, and datetimes:
    col_datetime = 5
    col_ID = 0
    col_val = 7
    #Open discharge.csv (discharge data from provincial non-integrated network)
    df_prov = pd.read_csv(src_file)
    prov_stn_list = pd.read_csv('provincial_station_list.csv')
    #Filter dataframe to only contain stations in provincial station list:
    df_prov = df_prov[df_prov.iloc[:,col_ID].isin(prov_stn_list.ID)]
    #Replace orginial station ID's with RFC ID's: (This line of code is heinous, replace with something better!)
    df_prov.iloc[:,col_ID] = df_prov.iloc[:,col_ID].map(prov_stn_list.set_index('ID').T.to_dict('records')[0])
    #Convert datetimes to PST:
    df_prov.iloc[:,col_datetime] = pd.to_datetime(df_prov.iloc[:,col_datetime]).copy().dt.tz_localize('UTC').dt.tz_convert('US/Pacific').apply(lambda x: x.replace(tzinfo=None))

    #Should current_datetime be calculated at time of data download?
    current_datetime = datetime.datetime.now()
    start_datetime = current_datetime.replace(second=0, hour=0, minute=0) - datetime.timedelta(days=2)

    #Remove data outside of import data range:
    df_prov = df_prov[df_prov.iloc[:,col_datetime]>start_datetime]
    df_prov = df_prov[df_prov.iloc[:,col_datetime]<current_datetime]
    #Pivot data:
    df_prov = df_prov.drop_duplicates(subset=['Location ID',' Date/Time(UTC)'], keep='first')
    df_prov = df_prov.pivot(index = list(df_prov)[col_datetime],columns = list(df_prov)[col_ID], values = list(df_prov)[col_val])
    return df_prov


def read_instantaneous_data_xlsx(src_file):
    f = open(src_file,'rb')
    Q_inst = pd.read_excel(f,sheet_name='ALL_Q')
    H_inst = pd.read_excel(f,sheet_name='ALL_H')
    f.close()

    Q_inst.iloc[:,0:2] = Q_inst.iloc[:,0:2].fillna(method='ffill')
    H_inst.iloc[:,0:2] = H_inst.iloc[:,0:2].fillna(method='ffill')
    Year = datetime.datetime.today().strftime('%Y')
    Date = pd.to_datetime(Year + "-" + Q_inst.iloc[:,0] + " " + Q_inst.iloc[:,1].astype(int).astype(str) + ":" + Q_inst.iloc[:,2].astype(str))
    Q_inst = Q_inst.set_index(Date)
    H_inst = H_inst.set_index(Date)

    return Q_inst, H_inst

#To Do: Restrict WSC data to import timeframe:
def format_WSC_data(src_folder):
    for fname in constants.SOURCE_HYDRO_DATA:
        local_filename = os.path.join(src_folder, fname.split("/")[-1])
        LOGGER.info(f"reading file to dataframe: {local_filename}")

        #Read in WSC data from file:
        df = pd.read_csv(local_filename)
        #Convert dates in dataframe to datetime format:
        df.Date = pd.to_datetime(df.Date)
        #Remove timezone (multiple timezones within the datetime column prevent the column from having datetime datatype)
        df.Date = df.Date.apply(lambda x: x.replace(tzinfo=None))
        #Round datetimes to nearest 5 min interval:
        df.Date = df.Date.round("5min")
        if fname==constants.SOURCE_HYDRO_DATA[0]:
            new_data = df
        else:
            new_data = pd.concat([new_data,df])


    new_data.drop_duplicates(subset=[new_data.columns[0],new_data.columns[1]],inplace=True)
    #Convert WSC data table into pivot table with datetime as index, station ID as columns, and discharge as values:
    Q_inst = new_data.pivot(index = list(df)[1],columns = list(df)[0], values = list(df)[6])
    H_inst = new_data.pivot(index = list(df)[1],columns = list(df)[0], values = list(df)[2])

    return Q_inst, H_inst

def read_csv_data(src_file):
    ext = os.path.splitext(src_file)[1]
    if ext=='.csv':
        df = pd.read_csv(src_file)
    elif ext == '.parquet':
        df = pd.read_parquet(src_file)

    #Fill in missing rows with previous date value:
    datefill = df.iloc[:,0:2].fillna(method='ffill')
    #Grab current year (year is not specified within file):
    Year = datetime.datetime.today().strftime('%Y')
    #Obtain datetime for each row (column 0 = Month, column 1 = Day, column 2 = Hour)
    Date = pd.to_datetime(Year + "-" + datefill.iloc[:,0] + " " + datefill.iloc[:,1].astype(int).astype(str) + ":" + df.iloc[:,2].astype(str))
    df = df.set_index(Date)

    return df

def update_instantaneous_data(new_data, local_path, obj_path, datatype):
    #Write new discharge values into DischargeOBS instantaneous
    #.combine_first may not overwrite existing values. May need to set prior data to NA to ensure revised data gets written to table

    #Data is stored in seperate parquet filea for each year and month
    #Read data from all files which overlap in date range with new data:
    inst_data = get_instantaneous_data(new_data.index, datatype, local_path, obj_path,'raw')
    #Combine new data into existing data:
    if not new_data.empty and not inst_data.empty:
        inst_updated = inst_data.combine_first(new_data)
    elif not inst_data.empty:
        inst_updated = inst_data
    elif not new_data.empty:
        inst_updated = new_data
          
    #Save data back into separate year-month parquet files:
    save_instantaneous_data(inst_updated, datatype, local_path, obj_path)

def get_instantaneous_data(new_data, datatype, local_path, obj_path, qc):
    #Grab year and month from index of new data (index must be datetime type):
    dt_stamp = new_data.strftime("%Y%m")
    #Produces set of unique year-month values from index:
    dt_set = set(dt_stamp)
    first = True
    #Loops though unique year-month values from new data. Load instantaneous data files associated with these year-months:
    ostore_objs = ostore.list_objects(obj_path,return_file_names_only=True)
    for i in dt_set:
        #File naming convention set here:
        if qc == 'raw':
            filename = 'DischargeOBS_'+ i + '_' + datatype + '.parquet'
        elif qc == 'qc':
            filename = 'DischargeOBS_qc_'+ i + '_' + datatype + '.parquet'
        filepath = os.path.join(local_path,filename)
        obj_filepath = os.path.join(obj_path,filename)
        if obj_filepath in ostore_objs:
            ostore.get_object(local_path=filepath, file_path=obj_filepath)
            data_chunk = pd.read_parquet(filepath)
            #Combine data from all files into single dataframe:
            if first:
                data = data_chunk
                first = False
            else:
                data = pd.concat([data,data_chunk])
    if first:
        return pd.DataFrame
    else:
        return data

current_datetime = datetime.datetime.now()
start_datetime = current_datetime.replace(second=0, hour=0, minute=0) - datetime.timedelta(days=2)
enddate = (current_datetime.replace(second=0, hour=0, minute=0) + datetime.timedelta(days=1)).strftime('%Y/%m/%d')
startdate = start_datetime.strftime('%Y/%m/%d')
datatype = 'Q'

def qc_instantaneous_data(startdate,enddate,datatype):
    local_path = constants.LOCAL_DATA_PATH
    raw_inst_path = constants.PROCESSED_OBJPATH
    qc_inst_path = constants.INST_QC_OBJPATH

    qc_range = pd.date_range(start = startdate, end = enddate, freq = '5min')
    raw_data = get_instantaneous_data(qc_range, datatype, local_path, raw_inst_path,'raw')
    qc_data = get_instantaneous_data(qc_range, datatype, local_path, qc_inst_path,'qc')


#To do: Check if files exists, create file if it does not.
def save_instantaneous_data(data, datatype, local_path, obj_path):
    #Grab year and month from index of dataframe (index must be datetime type):
    dt_stamp = data.index.strftime("%Y%m")
    #Produces set of unique year-month values from index:
    dt_set = set(dt_stamp)
    #Loops though unique year-month values from dataframe. Save instantaneous data files associated with these year-months:
    for i in dt_set:
        data_chunk = data[dt_stamp==i]
        filename = 'DischargeOBS_'+ i + '_' + datatype + '.parquet'
        filepath = os.path.join(local_path,filename)
        obj_filepath = os.path.join(obj_path,filename)
        data_chunk.to_parquet(filepath)
        LOGGER.debug(f"update instantaneous data to ostore {obj_filepath}")
        try:
            ostore.put_object(local_path=filepath, ostore_path=obj_filepath)
        except minio.error.S3Error as e:
            LOGGER.error(f"error putting object to ostore: {e}")
            LOGGER.info("going to delete versions of the file, and retry...")
            delete_all_non_current_version(obj_filepath)
            ostore.put_object(local_path=filepath, ostore_path=obj_filepath)


def return_data_path(url):
    r = requests.head(url)
    url_time = r.headers['last-modified']
    url_date = parse(url_time)
    dt_stamp = url_date.strftime("%Y%m%d%H%M")

def csv_to_parquet(local_path,obj_path):
    ostore.get_object(local_path=local_path, file_path=obj_path)
    df = pd.read_csv(local_path)
    local_parquet_path = os.path.splitext(local_path)[0] + '.parquet'
    obj_parquet_path = os.path.splitext(obj_path)[0] + '.parquet'
    df.to_parquet(local_parquet_path)
    try:
        ostore.put_object(local_path=local_parquet_path, ostore_path=obj_parquet_path)
    except minio.error.S3Error as e:
        LOGGER.error(f"error putting object to ostore: {e}")
        LOGGER.info("going to delete versions of the file, and retry...")
        delete_all_non_current_version(obj_parquet_path)
        ostore.put_object(local_path=local_parquet_path, ostore_path=obj_parquet_path)

def delete_all_non_current_version(ostore_path):
    """
    it looks like the versions can get layered on top of one another in a stack like structure.
    When one version gets deleted thenext one in the stack will show up.  

    This function will iterate over all the versions, deleting all but the latest version, all 
    the way to the bottom of the stack. Can take a while if there are a lot of versions.

    :param ostore_path: the path in object store who's versions you want to delete
    :type ostore_path: str, path
    """

    keys = ["Versions", "DeleteMarkers"]
    bucket = ostore.obj_store_bucket
    ostore.createBotoClient()
    s3 = ostore.boto_client

    while True:
        response = s3.list_object_versions(Bucket=bucket, Prefix=ostore_path)
        versions_to_delete = []
        
        for k in keys:
            if k in response:
                data = response[k]
                for item in data:
                    # print("item: ", item)
                    if item["Key"] == ostore_path and not item['IsLatest']:
                        versions_to_delete.append({
                            'Key': ostore_path,
                            'VersionId': item['VersionId'],
                            'LastModified': item['LastModified']
                        })
        
        if not versions_to_delete:
            break
        version_string = '\n'.join([v['VersionId'] + ' ' + str(v['LastModified']) for v in versions_to_delete])
        LOGGER.info(f'deleteing versions: {version_string}')

        versions_to_delete_send = []
        for ver in versions_to_delete:
            del ver['LastModified']
            versions_to_delete_send.append(ver)

        delete_response = s3.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': versions_to_delete_send,
                'Quiet': True
            }
        )

def write_PVDD(prov_Q_path,prov_H_path):
    #Set columns of dataframes containing station ID, data values, and datetimes:
    col_datetime = 5
    col_ID = 0
    col_val = 7
    Q_data = pd.read_csv(prov_Q_path).iloc[:,[col_datetime,col_ID,col_val]]
    H_data = pd.read_csv(prov_H_path).iloc[:,[col_datetime,col_ID,col_val]]
    Q_data.rename({" Value": "Discharge"}, axis='columns', inplace=True)
    H_data.rename({" Value": "Stage"}, axis='columns', inplace=True)
    prov_stn_list = pd.read_csv('provincial_station_list.csv',index_col=0)
    #Filter dataframe to only contain stations in provincial station list:
    for stn in prov_stn_list.index:
        stn_Q = Q_data[Q_data.iloc[:,1]==stn]
        stn_H = H_data[H_data.iloc[:,1]==stn]
        stn_Q.index = stn_Q.iloc[:,0]
        stn_H.index = stn_H.iloc[:,0]
        #stn_data = pd.concat([stn_Q,stn_H],axis=1)
        stn_data = pd.merge(stn_Q,stn_H,left_index=True,right_index=True,how='outer')
        stn_data.loc[:,"Time_PST"] = pd.to_datetime(stn_data.index).copy().tz_localize('UTC').tz_convert('US/Pacific').tz_localize(None)
        stn_data.loc[:,"id"] = stn
        output = stn_data.loc[:,["id","Time_PST","Stage","Discharge"]]
        local_PVDD_path = os.path.join(constants.LOCAL_DATA_PATH,f'{prov_stn_list.loc[stn].values[0]}.csv')
        obj_PVDD_path = os.path.join("dischargeOBS/PVDD",f'{prov_stn_list.loc[stn].values[0]}.csv')
        output.to_csv(local_PVDD_path,index=False)

        # check for versions of the file in object store
        LOGGER.info(f"uploading {obj_PVDD_path} to object store")
        ostore.put_object(local_path=local_PVDD_path, ostore_path=obj_PVDD_path)
        # delete all non current versions
        LOGGER.info(f"checking for redundant versions of {obj_PVDD_path}")
        delete_all_non_current_version(obj_PVDD_path)

if __name__ == '__main__':

    # setup logging
    log_config_path = os.path.join(os.path.dirname(__file__), 'logging.config')
    logging.config.fileConfig(log_config_path, disable_existing_loggers=False)
    logger_name = os.path.splitext(os.path.basename(__file__))[0]
    print(f"logger name: {logger_name}")
    LOGGER = logging.getLogger(logger_name)

    ostore = NRObjStoreUtil.ObjectStoreUtil()

    Q_file = 'DischargeOBS_2023_instant2_Q.csv'
    H_file = 'DischargeOBS_2023_instant2_H.csv'
    LOGGER.info(f"Q_file src: {Q_file}")
    LOGGER.info(f"H_file src: {H_file}")

    data_folder = constants.RAW_DATA_FOLDER
    dest_folder = constants.LOCAL_DATA_PATH

    obj_path = 'dischargeOBS/processed_data/'
    Q_path = os.path.join(dest_folder, Q_file)
    H_path = os.path.join(dest_folder, H_file)
    Q_obj_path = os.path.join('dischargeOBS/processed_data/',Q_file)
    H_obj_path = os.path.join('dischargeOBS/processed_data/',H_file)
    LOGGER.info(f"Q_obj_path in object store: {Q_obj_path}")
    LOGGER.info(f"H_obj_path in object store: {H_obj_path}")

    prov_Q_path = os.path.join(data_folder, constants.PROV_HYDRO_SRC[0].split("/")[-1])
    prov_H_path = os.path.join(data_folder, constants.PROV_HYDRO_SRC[1].split("/")[-1])
    stn_list = pd.read_excel('STN_list.xlsx')
    
    if not os.path.exists(data_folder):
        # Create data directory if it does not already exist:
        os.makedirs(data_folder)

    download_WSC_data(data_folder)
    download_provincial_data(data_folder)

    Q_WSC, H_WSC = format_WSC_data(data_folder)
    Q_USGS, H_USGS = download_USGS_data()
    write_PVDD(prov_Q_path,prov_H_path)
    Q_prov = format_provincial_data(prov_Q_path)
    H_prov = format_provincial_data(prov_H_path)
    #H_prov = format_provincial_data(constants.PROV_HYDRO_SRC[1].split("/")[-1])

    update_instantaneous_data(pd.concat([Q_WSC,Q_prov,Q_USGS],axis=1),dest_folder,obj_path,'Q')
    update_instantaneous_data(pd.concat([H_WSC,H_prov,H_USGS],axis=1),dest_folder,obj_path,'H')


