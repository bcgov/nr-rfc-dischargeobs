import pandas as pd
from pandas.io.formats import excel
excel.ExcelFormatter.header_style = None

import numpy as np
import requests
import constants
import os
import datetime
import NRUtil.NRObjStoreUtil as NRObjStoreUtil
import discharge_obs_pd
import xlsxwriter
import xlwt

#def write_instantaneous_xl():
def get_instantaneous_data(new_data, datatype, local_path, obj_path):
    #Grab year and month from index of new data (index must be datetime type):
    dt_stamp = new_data.strftime("%Y%m")
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

def Write_Instant():
    dt_range = pd.date_range(start = '2023/7/1', end = '2023/10/1', freq = '5min')
    dt_mmdd = dt_range.strftime("%m-%d").to_series(index = dt_range)
    dt_mmdd[dt_mmdd.eq(dt_mmdd.shift())] = ""
    dt_HH = pd.to_numeric(dt_range.strftime("%H").to_series(index = dt_range))
    dt_HH[dt_HH.eq(dt_HH.shift())] = ""
    dt_MM = pd.to_numeric(dt_range.strftime("%M").to_series(index = dt_range))
    Q_data = pd.DataFrame(data=None,index=dt_range)
    H_data = Q_data.copy()

    local_path = constants.DEST_DATA_FOLDER
    obj_path = 'dischargeOBS/processed_data/'
    Qstored_data = get_instantaneous_data(dt_range, 'Q', local_path, obj_path)
    Hstored_data = get_instantaneous_data(dt_range, 'H', local_path, obj_path)

    Q_data = Q_data.combine_first(Qstored_data)
    Q_data.insert(loc = 0, column = "Day", value = dt_mmdd)
    Q_data.insert(loc = 1, column = "Hour", value = dt_HH)
    Q_data.insert(loc = 2, column = "Minute", value = dt_MM)

    H_data = H_data.combine_first(Hstored_data)
    H_data.insert(loc = 0, column = "Day", value = dt_mmdd)
    H_data.insert(loc = 1, column = "Hour", value = dt_HH)
    H_data.insert(loc = 2, column = "Minute", value = dt_MM)

    write_path = os.path.join(local_path,"DischargeOBS_2023_instant3.xlsx")
    with pd.ExcelWriter(write_path, engine = 'xlsxwriter') as writer:  
        Q_data.to_excel(writer, sheet_name='ALL_Q', index = False)
        H_data.to_excel(writer, sheet_name='ALL_H', index = False)

def Write_COFFEE_Instant():
    dt_range = pd.date_range(start = '2023/1/1', end = '2024/1/2', freq = 'H')
    dt_range = dt_range[:-1]
    dt_mmdd = dt_range.strftime("%m-%d").to_series(index = dt_range)
    dt_mmdd[dt_mmdd.eq(dt_mmdd.shift())] = ""
    dt_HH = pd.to_numeric(dt_range.strftime("%H").to_series(index = dt_range))
    dt_HH[dt_HH.eq(dt_HH.shift())] = ""

    #Read List of COFFEE stations
    #!!!Investigate alternative methods of storing Coffee Station List!!!
    stationlist = pd.read_csv('Model_Station_List.csv').astype(str).COFFEE.dropna()
    stationlist = stationlist[stationlist != 'nan']
    stations = stationlist.str.slice(start = 0, stop = 7)
    obstype = stationlist.str.slice(start = 8, stop = 9)    

    output_fname = "DISCHARGE_OBS_INST.xlsx"
    local_path = constants.DEST_DATA_FOLDER
    obj_path = 'dischargeOBS/processed_data/'
    coffee_objpath = constants.COFFEE_OUTPUT_OBJPATH
    coffee_obj_fpath = os.path.join(coffee_objpath,output_fname)

    output = pd.DataFrame(data=None,index=dt_range,columns=stationlist)
    Qstored_data = get_instantaneous_data(dt_range, 'Q', local_path, obj_path)
    Hstored_data = get_instantaneous_data(dt_range, 'H', local_path, obj_path)

    for col in range(len(stationlist)):
        if obstype[col] == 'Q':
            if stations[col] in Qstored_data.columns:
                stn_data = Qstored_data.loc[:,stations[col]].resample('H')
            else: continue
        else:
            if stations[col] in Hstored_data.columns:
                stn_data = Hstored_data.loc[:,stations[col]].resample('H')
            else: continue
        hrly_max = stn_data.max()
        hrly_last = stn_data.last()
        
        #!!!Check behaviour of this code during hours with NaN values in data!!!
        #!!!Investigate discrepancies between this output and the excel output!!!
        #If hourly maximum is greater than previous value (flow/level rising), set value to maximum for that hour
        hrly_val = hrly_max
        #else if flow falling, set calue to last value of that hour:
        for i in range(1,len(hrly_val)):
            if hrly_max[i] <= hrly_val[i-1]:
                hrly_val[i] = hrly_last[i]

        output.iloc[:,col] = hrly_val.reindex_like(output)
    
    output.loc[:,'08HB017-Q'] = 1.1907*output.loc[:,'08HB023-Q'] + 1.9845*output.loc[:,'08HB008-Q'] + 20.819

    output.insert(loc = 0, column = "Date", value = dt_mmdd)
    output.insert(loc = 1, column = "Hour", value = dt_HH)

    write_path = os.path.join(local_path,output_fname)
    with pd.ExcelWriter(write_path) as writer:  
        output.to_excel(writer, sheet_name='2023', index = False)
    ostore.put_object(local_path=write_path, ostore_path=coffee_obj_fpath)

#INCOMPLETE
#Both DischargeOBS files must contain tabs with data from previous year for when model is run in early January and must use data from both years
year = '2023'
def Write_COFFEE_Daily(year):
    year = str(year)
    local_path = constants.DEST_DATA_FOLDER
    dly_obj_path = 'dischargeOBS/processed_data/daily'
    coffee_objpath = constants.COFFEE_OUTPUT_OBJPATH
    coffee_obj_fpath = os.path.join(coffee_objpath,"DISCHARGE_OBS.xlsx")
    
    dly_Q_name = f'DischargeOBS_{year}_Q_daily.parquet'
    dly_Q_filepath = os.path.join(local_path,dly_Q_name)
    dly_Q_objpath = os.path.join(dly_obj_path,dly_Q_name)
    dly_H_name = f'DischargeOBS_{year}_H_daily.parquet'
    dly_H_filepath = os.path.join(local_path,dly_H_name)
    dly_H_objpath = os.path.join(dly_obj_path,dly_H_name)

    year2 = str(int(year)+1)  
    dt_range = pd.date_range(start = f'{year}/1/1', end = f'{year2}/1/2', freq = 'D')
    dt_range = dt_range[:-1]
    dt_yyyymmdd = dt_range.strftime("%Y-%m-%d").to_series(index = dt_range)
    dt_yyyymmdd[dt_yyyymmdd.eq(dt_yyyymmdd.shift())] = ""

    #Read List of COFFEE stations
    #!!!Investigate alternative methods of storing Coffee Station List!!!
    stationlist = pd.read_csv('Model_Station_List.csv').astype(str).COFFEE.dropna()
    stationlist = stationlist[stationlist != 'nan']
    stations = stationlist.str.slice(start = 0, stop = 7)
    obstype = stationlist.str.slice(start = 8, stop = 9)    

    output = pd.DataFrame(data=None,index=dt_range,columns=stationlist)

    ostore.get_object(local_path=dly_Q_filepath, file_path=dly_Q_objpath)
    Qstored_data = pd.read_parquet(dly_Q_filepath)
    ostore.get_object(local_path=dly_H_filepath, file_path=dly_H_objpath)
    Hstored_data = pd.read_parquet(dly_H_filepath)

    for col in range(len(stationlist)):
        if obstype[col] == 'Q':
            if stations[col] in Qstored_data.columns:
                output.iloc[:,col] = Qstored_data.loc[:,stations[col]]
                #output.iloc[:,col] = output.iloc[:,col].update(Qstored_data.loc[:,stations[col]])
            else: continue
        else:
            if stations[col] in Hstored_data.columns:
                output.iloc[:,col] = Hstored_data.loc[:,stations[col]]
                #output.iloc[:,col] = output.iloc[:,col].update(Hstored_data.loc[:,stations[col]])
            else: continue        

    output.loc[:,'08HB017-Q'] = 1.1907*output.loc[:,'08HB023-Q'] + 1.9845*output.loc[:,'08HB008-Q'] + 20.819

    output.insert(loc = 0, column = "DATE", value = dt_yyyymmdd)

    write_path = os.path.join(local_path,"DISCHARGE_OBS.xlsx")
    with pd.ExcelWriter(write_path) as writer:  
        output.to_excel(writer, sheet_name='2023', index = False)
    ostore.put_object(local_path=write_path, ostore_path=coffee_obj_fpath)

def Write_CLEVER_Daily(year):
    year = str(year)
    local_path = constants.DEST_DATA_FOLDER
    dly_obj_path = 'dischargeOBS/processed_data/daily'
    
    dly_Q_name = f'DischargeOBS_{year}_Q_daily.parquet'
    dly_Q_filepath = os.path.join(local_path,dly_Q_name)
    dly_Q_objpath = os.path.join(dly_obj_path,dly_Q_name)
    dly_H_name = f'DischargeOBS_{year}_H_daily.parquet'
    dly_H_filepath = os.path.join(local_path,dly_H_name)
    dly_H_objpath = os.path.join(dly_obj_path,dly_H_name)

    year2 = str(int(year)+1)  
    dt_range = pd.date_range(start = f'{year}/1/1', end = f'{year2}/1/2', freq = 'D')
    dt_range = dt_range[:-1]
    dt_yyyymmdd = dt_range.strftime("%Y-%m-%d").to_series(index = dt_range)
    dt_yyyymmdd[dt_yyyymmdd.eq(dt_yyyymmdd.shift())] = ""

    #Read List of CLEVER stations
    #!!!Investigate alternative methods of storing Coffee Station List!!!
    stationlist = pd.read_csv('Model_Station_List.csv').astype(str).CLEVER.dropna()
    stationlist = stationlist[stationlist != 'nan']
    stations = stationlist.str.slice(start = 0, stop = 7)
    obstype = stationlist.str.slice(start = 8, stop = 9)    

    output = pd.DataFrame(data=None,index=dt_range,columns=stationlist)

    ostore.get_object(local_path=dly_Q_filepath, file_path=dly_Q_objpath)
    Qstored_data = pd.read_parquet(dly_Q_filepath)
    ostore.get_object(local_path=dly_H_filepath, file_path=dly_H_objpath)
    Hstored_data = pd.read_parquet(dly_H_filepath)

    for col in range(len(stationlist)):
        if obstype[col] == 'Q':
            if stations[col] in Qstored_data.columns:
                output.iloc[:,col] = Qstored_data.loc[:,stations[col]]
                #output.iloc[:,col] = output.iloc[:,col].update(Qstored_data.loc[:,stations[col]])
            else: continue
        else:
            if stations[col] in Hstored_data.columns:
                output.iloc[:,col] = Hstored_data.loc[:,stations[col]]
                #output.iloc[:,col] = output.iloc[:,col].update(Hstored_data.loc[:,stations[col]])
            else: continue        

    #Use eval() function to evaluate functions stored in text or csv file
    output.loc[:,'08HB017-Q'] = 1.1907*output.loc[:,'08HB023-Q'] + 1.9845*output.loc[:,'08HB008-Q'] + 20.819

    output.insert(loc = 0, column = "DATE", value = dt_yyyymmdd)

    write_path = os.path.join(local_path,"DISCHARGE_OBS.xlsx")
    with pd.ExcelWriter(write_path) as writer:  
        output.to_excel(writer, sheet_name='2023', index = False)

#Remove negative discharges
#Interpolate 5-min data - seperate 5-min 'QCd' product
#Interpolate hourly data, fill forward to current day
#Export to excel
#Automated QC?
#Compute discharge from stage if stage available but discharge missing
#Open hourly and daily data files if they exist. If not, create them
#Ensure stations from station list included in hourly and daily data files
#Calculate hourly averages from 5-min data, save hourly data files
#Calculate daily averages from hourly data, save daily data files
#Compute missing stations (CLEVER) from equation list
data_type = 'Q'
startdate = '2023/9/11'
enddate = '2023/9/13'
def Update_dischargeOBS_hourly(startdate,enddate,data_type): #data_type 'Q' or 'H'
    import_range = pd.date_range(start = startdate, end = enddate, freq = 'H')
    import_range = import_range[:-1]
    year = startdate[0:4]
    if year != enddate[0:4]:
        raise Exception("startdate and enddate must be from same year")
    
    #Read List of COFFEE stations
    #!!!Investigate alternative methods of storing Coffee Station List!!!
    stationlist = pd.read_csv('Model_Station_List.csv').astype(str).COFFEE.dropna()
    stationlist = stationlist[stationlist != 'nan']
    stations = stationlist.str.slice(start = 0, stop = 7) 

    local_path = constants.DEST_DATA_FOLDER
    obj_path = 'dischargeOBS/processed_data'
    hrly_path = 'dischargeOBS/processed_data/hourly'
    hrly_objs = ostore.list_objects(hrly_path,return_file_names_only=True)

    hrly_fname = f'DischargeOBS_{year}_{data_type}_hourly.parquet'
    filepath = os.path.join(local_path,hrly_fname)
    hrly_objpath = os.path.join(hrly_path,hrly_fname)

    inst_data = get_instantaneous_data(import_range, data_type, local_path, obj_path)
    #Restrict data to within import start date and end date:
    inst_data = inst_data[np.all([inst_data.index>startdate,inst_data.index<enddate],axis=0)]
    inst_names = inst_data.columns.to_series()

    #Create blank hourly dataframe for year if it doesn't already exist:
    if hrly_objpath not in hrly_objs:
        dt_range = pd.date_range(start = f'{year}/1/1', end = f'{str(int(year)+1)}/1/1', freq = 'H')
        dt_range = dt_range[:-1]
        dt_mmdd = dt_range.strftime("%m-%d").to_series(index = dt_range)
        dt_mmdd[dt_mmdd.eq(dt_mmdd.shift())] = ""
        dt_HH = pd.to_numeric(dt_range.strftime("%H").to_series(index = dt_range))
        dt_HH[dt_HH.eq(dt_HH.shift())] = ""

        stationlist_diff = stations[~stations.isin(inst_names)]
        stn_names = pd.concat([inst_names,stationlist_diff])
        output = pd.DataFrame(data=None,index=dt_range,columns=stn_names)
    else:
        #Open existing hourly data:
        ostore.get_object(local_path=filepath, file_path=hrly_objpath)
        output = pd.read_parquet(filepath)
        #Check existing column names. Add any new stations from instantaneous data or from station list:
        hrly_names = output.columns.to_series()
        stationlist_diff = stations[~stations.isin(hrly_names)]
        inst_station_diff = inst_names[~inst_names.isin(hrly_names)]
        name_diff = pd.concat([stationlist_diff,inst_station_diff])
        #Add blank column to dataframe for each new station:
        for stn in name_diff:
            output[stn] = np.nan

    #Average hourly data from instantaneous over specified time period and save to file
    #Option to only update specific stations?
    #Option to specify Q or H?

    colnames = output.columns
    for col in range(len(colnames)):
        if colnames[col] in inst_data.columns:
            stn_data = inst_data.loc[:,colnames[col]].resample('H')

            hrly_mean = stn_data.mean()
            hrly_last = stn_data.last()
        
            #!!!Check behaviour of this code during hours with NaN values in data!!!
            #!!!Investigate discrepancies between this output and the excel output!!!
            #If hourly maximum is greater than previous value (flow/level rising), set value to maximum for that hour
            hrly_val = hrly_mean
            #If not all null, set last hourly value to last valid measurement rather than hourly mean:
            if not hrly_val.isnull().all():
                lastvalidindex = hrly_val.last_valid_index()
                hrly_val.loc[lastvalidindex] = hrly_last.loc[lastvalidindex]

            output.iloc[:,col].update(hrly_val)

    #Fill in missing values up until current day:
    cd_ind = (output.index == pd.Timestamp('now').floor('d')).argmax()
    output[0:cd_ind+1] = output[0:cd_ind+1].fillna(method='ffill')
    
    output = output.round(3)

    output.to_parquet(filepath)
    ostore.put_object(local_path=filepath, ostore_path=hrly_objpath)

data_type = 'H'
year = '2023'
def Update_dischargeOBS_daily(year,data_type): #data_type 'Q' or 'H'
    local_path = constants.DEST_DATA_FOLDER
    dly_path = 'dischargeOBS/processed_data/daily'
    hrly_path = 'dischargeOBS/processed_data/hourly'

    hrly_fname = f'DischargeOBS_{year}_{data_type}_hourly.parquet'
    dly_fname = f'DischargeOBS_{year}_{data_type}_daily.parquet'
    dly_filepath = os.path.join(local_path,dly_fname)
    dly_objpath = os.path.join(dly_path,dly_fname)
    hrly_filepath = os.path.join(local_path,hrly_fname)
    hrly_objpath = os.path.join(hrly_path,hrly_fname)

    ostore.get_object(local_path=hrly_filepath, file_path=hrly_objpath)
    hrly_data = pd.read_parquet(hrly_filepath)
    
    resampled_data = hrly_data.resample('D')
    dly_mean = resampled_data.mean()
    output = dly_mean
    dly_last = resampled_data.last()

    last_valid_date = dly_mean.apply(lambda series: series.last_valid_index())
    for stn in output.columns:
        if not pd.isnull(last_valid_date[stn]):
            output.loc[last_valid_date[stn],stn] = dly_last.loc[last_valid_date[stn],stn]
    
    output = output.round(3)

    output.to_parquet(dly_filepath)
    ostore.put_object(local_path=dly_filepath, ostore_path=dly_objpath)

src_file = 'processed_data/DischargeOBS_2023_hourly.xlsx'
year = '2023'
def read_hourly_data_xlsx(src_file,year,data_type):
    f = open(src_file,'rb')
    hrly_data = pd.read_excel(f,sheet_name=f'ALL_{data_type}')
    f.close()

    hrly_data.iloc[:,0] = hrly_data.iloc[:,0].fillna(method='ffill')
    Date = pd.to_datetime(year + "-" + hrly_data.iloc[:,0] + " " + hrly_data.iloc[:,1].astype(int).astype(str) + ":00")
    date_duplicates = Date.duplicated(keep='first')
    Date[date_duplicates] = Date[date_duplicates] + pd.offsets.DateOffset(years=1)
    hrly_data = hrly_data.set_index(Date)

    return hrly_data

def update_hourly_data_from_xl(year, data_type):
    local_path = constants.DEST_DATA_FOLDER
    hrly_path = 'dischargeOBS/processed_data/hourly'

    hrly_fname = f'DischargeOBS_{year}_{data_type}_hourly.parquet'
    filepath = os.path.join(local_path,hrly_fname)
    hrly_objpath = os.path.join(hrly_path,hrly_fname)
    src_file = 'processed_data/DischargeOBS_2023_hourly.xlsx'

    new_data = read_hourly_data_xlsx(src_file,year,data_type)
    new_data = new_data.drop(['DATE','HOUR'],axis=1)

    ostore.get_object(local_path=filepath, file_path=hrly_objpath)
    output = pd.read_parquet(filepath)

    output = output.combine_first(new_data)

    output.to_parquet(filepath)
    ostore.put_object(local_path=filepath, ostore_path=hrly_objpath)


if __name__ == '__main__':
    # 
    ostore = NRObjStoreUtil.ObjectStoreUtil()

    current_datetime = datetime.datetime.now()
    start_datetime = current_datetime.replace(second=0, hour=0, minute=0) - datetime.timedelta(days=2)
    current_date_text = (current_datetime.replace(second=0, hour=0, minute=0) + datetime.timedelta(days=1)).strftime('%Y/%m/%d')
    start_date_text = start_datetime.strftime('%Y/%m/%d')
    year = current_datetime.strftime('%Y')
    
    Update_dischargeOBS_hourly(start_date_text,current_date_text,'Q')
    Update_dischargeOBS_hourly(start_date_text,current_date_text,'H')
    Update_dischargeOBS_daily(year,'Q')
    Update_dischargeOBS_daily(year,'H')

    Write_COFFEE_Instant()
    Write_COFFEE_Daily(year)
    #Write_Instant()

    