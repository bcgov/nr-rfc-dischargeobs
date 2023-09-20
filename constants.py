
SOURCE_HYDRO_DATA = ['https://dd.weather.gc.ca/hydrometric/csv/BC/hourly/BC_hourly_hydrometric.csv',
                     'https://dd.weather.gc.ca/hydrometric/csv/YT/hourly/YT_10AA001_hourly_hydrometric.csv',
                     'https://dd.weather.gc.ca/hydrometric/csv/YT/hourly/YT_10AA004_hourly_hydrometric.csv',
                     'https://dd.weather.gc.ca/hydrometric/csv/YT/hourly/YT_10AA006_hourly_hydrometric.csv',
                     'https://dd.weather.gc.ca/hydrometric/csv/YT/hourly/YT_10AB001_hourly_hydrometric.csv',
                     'https://dd.weather.gc.ca/hydrometric/csv/YT/hourly/YT_10AD002_hourly_hydrometric.csv',
                     ]
PROV_HYDRO_SRC = ['http://www.env.gov.bc.ca/wsd/data_searches/water/Discharge.csv',
                  'http://www.env.gov.bc.ca/wsd/data_searches/water/Stage.csv']

RAW_DATA_FOLDER = 'raw_data'
LOCAL_DATA_PATH = 'processed_data'
PROCESSED_OBJPATH = 'dischargeOBS/processed_data'
HOURLY_OBJPATH = 'dischargeOBS/processed_data/hourly'
DAILY_OBJPATH = 'dischargeOBS/processed_data/daily'
COFFEE_OUTPUT_OBJPATH = 'dischargeOBS/output/coffee'

SOURCE_HYDRO_DATETIME_FORMAT = 'YYYY-MM-DD HH:mm:ssZZ'

DEMO_VAR = 3