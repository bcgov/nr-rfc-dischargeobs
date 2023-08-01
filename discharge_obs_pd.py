import pandas as pd
import numpy as np
import requests
import constants
import os
import datetime
import NRUtil.NRObjStoreUtil as NRObjStoreUtil
import pyarrow
from dateutil.parser import parse

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
        with requests.get(fname, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)

def download_provincial_data(dest_folder):
    for fname in constants.PROV_HYDRO_SRC:
        #Use filename (removing remainder of url) for saving file locally:
        local_filename = os.path.join(dest_folder, fname.split("/")[-1])
        #Download file and write to local file name:
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

def format_WSC_data(src_folder):
    for fname in constants.SOURCE_HYDRO_DATA:
        local_filename = os.path.join(src_folder, fname.split("/")[-1])

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
    inst_data = get_instantaneous_data(new_data, datatype, local_path, obj_path)
    #Combine new data into existing data:
    inst_updated = inst_data.combine_first(new_data)
    #Save data back into separate year-month parquet files:
    save_instantaneous_data(inst_updated, datatype, local_path, obj_path)

def get_instantaneous_data(new_data, datatype, local_path, obj_path):
    #Grab year and month from index of new data (index must be datetime type):
    dt_stamp = new_data.index.strftime("%Y%m")
    #Produces set of unique year-month values from index:
    dt_set = set(dt_stamp)
    first = True
    #Loops though unique year-month values from new data. Load instantaneous data files associated with these year-months:
    ostore_objs = ostore.list_objects(obj_path,return_file_names_only=True)

    for i in dt_set:
        #File naming convention set here:
        filename = 'DischargeOBS_'+ i + '_' + datatype + '.parquet'
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

    return data

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
    ostore.put_object(local_path=local_parquet_path, ostore_path=obj_parquet_path)



#def write_hydro_data(infile,outfile):
    '''
    start = "2023-04-01"
    stop = "2023-07-01"
    DateSeq5min = pd.date_range(start, stop, freq="5min")
    DischargeOBS5min = pd.DataFrame(np.nan, index=DateSeq5min, columns=stn_list.ID)
    discharge_obs_pivot = df.pivot(index = list(df)[1],columns = list(df)[0], values = list(df)[6])
    DischargeOBS5min_updated = DischargeOBS5min.combine_first(discharge_obs_pivot)
    DischargeOBS5min_updated.to_csv('out.csv')
    '''
    #Pandas does not support pendulum :(

if __name__ == '__main__':
    # 
    ostore = NRObjStoreUtil.ObjectStoreUtil()

    Q_file = 'DischargeOBS_2023_instant2_Q.csv'
    H_file = 'DischargeOBS_2023_instant2_H.csv'
    data_folder = constants.RAW_DATA_FOLDER
    dest_folder = constants.DEST_DATA_FOLDER
    obj_path = 'dischargeOBS/processed_data/'
    Q_path = os.path.join(dest_folder, Q_file)
    H_path = os.path.join(dest_folder, H_file)
    Q_obj_path = os.path.join('dischargeOBS/processed_data/',Q_file)
    H_obj_path = os.path.join('dischargeOBS/processed_data/',H_file)
    prov_Q_path = os.path.join(data_folder, constants.PROV_HYDRO_SRC[0].split("/")[-1])
    prov_H_path = os.path.join(data_folder, constants.PROV_HYDRO_SRC[1].split("/")[-1])
    stn_list = pd.read_excel('STN_list.xlsx')
    
    if not os.path.exists(data_folder):
        # Create data directory if it does not already exist:
        os.makedirs(data_folder)

    download_WSC_data(data_folder)
    download_provincial_data(data_folder)

    Q_WSC, H_WSC = format_WSC_data(data_folder)
    Q_prov = format_provincial_data(prov_Q_path)
    H_prov = format_provincial_data(prov_H_path)
    #H_prov = format_provincial_data(constants.PROV_HYDRO_SRC[1].split("/")[-1])

    update_instantaneous_data(pd.concat([Q_WSC,Q_prov],axis=1),dest_folder,obj_path,'Q')
    update_instantaneous_data(pd.concat([H_WSC,H_prov],axis=1),dest_folder,obj_path,'H')

