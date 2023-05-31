import pandas as pd
import numpy as np
import requests
import constants
import os
import datetime
import time


#Jan/Feb/Mar: output to instant1 file, Apr/May/Jun: output to instant2 file, etc.
#Open instant file or create it if it doesn't exist

#Thoughts:
#Save WSC/provincial data with current date in filename? Delete data older than  3 days?
#Excel vs csv: Read/write from csv's for speed but also export as excel for compatability with existing processes
#Ability to import multiple days worth of data in case of issue (e.g. datamart outage)
#Add provincial hydrometric stations
#Ensure code is flexible to easily allow addition/removal of stations
#Automatically grab data from alternative file sources if needed


def get_hydro_data():
    #this is going to make a web request to retrieve the hydrometric data
    
    if not os.path.exists(local_filename):
    
        # NOTE the stream=True parameter below
        with requests.get(constants.SOURCE_HYDRO_DATA, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    #if chunk: 
                    f.write(chunk)

#Download WSC data from datamart:
def download_WSC_data():
    #Loop through datamart file paths listed in constants.py file:
    for fname in constants.SOURCE_HYDRO_DATA:
        #Use filename (removing remainder of url) for saving file locally:
        local_filename = fname.split("/")[-1]
        #Download file and write to local file name:
        with requests.get(fname, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)

def download_provincial_data():
    for fname in constants.PROV_HYDRO_SRC:
        #Use filename (removing remainder of url) for saving file locally:
        local_filename = fname.split("/")[-1]
        #Download file and write to local file name:
        with requests.get(fname, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)

def format_provincial_data(pfile):
    col_datetime = 5
    col_ID = 0
    col_val = 7
    #Open discharge.csv (discharge data from provincial non-integrated network)
    df_prov = pd.read_csv(pfile)
    prov_stn_list = pd.read_csv('provincial_station_list.csv')
    #Filter dataframe to only contain stations in provincial station list:
    df_prov = df_prov[df_prov.iloc[:,col_ID].isin(prov_stn_list.ID)]
    #Replace orginial station ID's with RFC ID's: (This line of code is heinous, replace with something better!)
    df_prov.iloc[:,col_ID] = df_prov.iloc[:,col_ID].map(prov_stn_list.set_index('ID').T.to_dict('records')[0])
    #Convert datetimes to PST:
    df_prov.iloc[:,col_datetime] = pd.to_datetime(df_prov.iloc[:,col_datetime]).copy().dt.tz_localize('UTC').dt.tz_convert('US/Pacific').apply(lambda x: x.replace(tzinfo=None))

    #Should current_datetime be calculated at time of data download?
    current_datetime = datetime.datetime.now()
    start_datetime = current_datetime.replace(second=0, hour=0, minute=0) - datetime.timedelta(days=1)

    df_prov = df_prov[df_prov.iloc[:,col_datetime]>start_datetime]
    df_prov = df_prov[df_prov.iloc[:,col_datetime]<current_datetime]
    df_prov = df_prov.pivot(index = list(df_prov)[col_datetime],columns = list(df_prov)[col_ID], values = list(df_prov)[col_val])
    return df_prov


instfile = "DischargeOBS_2023_instant2.xlsx"
def read_instantaneous_data_xlsx(instfile):
    f = open(instfile,'rb')
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

def format_WSC_data():
    for fname in constants.SOURCE_HYDRO_DATA:
        local_filename = fname.split("/")[-1]

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

    #Convert WSC data table into pivot table with datetime as index, station ID as columns, and discharge as values:
    Q_inst = new_data.pivot(index = list(df)[1],columns = list(df)[0], values = list(df)[6])
    H_inst = new_data.pivot(index = list(df)[1],columns = list(df)[0], values = list(df)[2])

    return Q_inst, H_inst

def read_instantaneous_data(infile):
    
    df = pd.read_csv(infile)

    #Fill in missing rows with previous date value:
    datefill = df.iloc[:,0:2].fillna(method='ffill')
    #Grab current year (year is not specified within file):
    Year = datetime.datetime.today().strftime('%Y')
    #Obtain datetime for each row (column 0 = Month, column 1 = Day, column 2 = Hour)
    Date = pd.to_datetime(Year + "-" + datefill.iloc[:,0] + " " + datefill.iloc[:,1].astype(int).astype(str) + ":" + df.iloc[:,2].astype(str))
    df = df.set_index(Date)

    return df

def update_instantaneous_data():

    #Write new discharge values into DischargeOBS instantaneous
    #.combine_first may not overwrite existing values. May need to set prior data to NA to ensure revised data gets written to table
    Q_inst_updated = Q_inst.combine_first(Q_inst_new_pivot)
    Q_inst_updated.to_csv('out.csv')
    #Rearrange columns to order expected by excel dischargeOBS?




def write_hydro_data(infile,outfile):
    start = "2023-04-01"
    stop = "2023-07-01"
    DateSeq5min = pd.date_range(start, stop, freq="5min")
    DischargeOBS5min = pd.DataFrame(np.nan, index=DateSeq5min, columns=stn_list.ID)
    discharge_obs_pivot = df.pivot(index = list(df)[1],columns = list(df)[0], values = list(df)[6])
    DischargeOBS5min_updated = DischargeOBS5min.combine_first(discharge_obs_pivot)
    DischargeOBS5min_updated.to_csv('out.csv')

    #Pandas does not support pendulum :(

if __name__ == '__main__':
    # 
    Q_file = 'DischargeOBS_2023_instant2_Q.csv'
    H_file = 'DischargeOBS_2023_instant2_H.csv'
    stn_list = pd.read_excel('STN_list.xlsx')


    download_WSC_data()
    download_provincial_data()

    Q_WSC, H_WSC = format_WSC_data()
    Q_prov = format_provincial_data(constants.PROV_HYDRO_SRC[0].split("/")[-1])
    #H_prov = format_provincial_data(constants.PROV_HYDRO_SRC[1].split("/")[-1])
    Q_inst = read_instantaneous_data(Q_file)
    #H_inst = read_instantaneous_data(H_file)
    
    #Write new data to instantaneous file:
    Q_inst_updated = Q_inst.combine_first(pd.concat([Q_WSC,Q_prov],axis=1))
    #Re-order columns (0:3 are date/hour/minute, stations ordered based on station list)
    col_list = pd.concat([Q_inst.columns[0:3].to_series(),stn_list.ID]) 
    Q_inst_updated = Q_inst_updated.reindex(columns=col_list)

    #Save instantaneous file with updated data:
    Q_inst_updated.to_csv(Q_file,index=False)

