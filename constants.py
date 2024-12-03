
DATAMART_URL = 'https://hpfx.collab.science.gc.ca/%Y%m%d/WXO-DD/'
SOURCE_HYDRO_DATA = ['hydrometric/csv/BC/hourly/BC_hourly_hydrometric.csv',
                     'hydrometric/csv/YT/hourly/YT_10AA001_hourly_hydrometric.csv',
                     'hydrometric/csv/YT/hourly/YT_10AA004_hourly_hydrometric.csv',
                     'hydrometric/csv/YT/hourly/YT_10AA006_hourly_hydrometric.csv',
                     'hydrometric/csv/YT/hourly/YT_10AB001_hourly_hydrometric.csv',
                     'hydrometric/csv/YT/hourly/YT_10AD002_hourly_hydrometric.csv',
                     ]
PROV_HYDRO_SRC = ['http://www.env.gov.bc.ca/wsd/data_searches/water/Discharge.csv',
                  'http://www.env.gov.bc.ca/wsd/data_searches/water/Stage.csv']

RAW_DATA_FOLDER = 'raw_data'
LOCAL_DATA_PATH = 'processed_data'
PROCESSED_OBJPATH = 'dischargeOBS/processed_data'
INST_QC_OBJPATH = 'dischargeOBS/processed_data/instantaneous_qc'
HOURLY_OBJPATH = 'dischargeOBS/processed_data/hourly'
DAILY_OBJPATH = 'dischargeOBS/processed_data/daily'
COFFEE_OUTPUT_OBJPATH = 'dischargeOBS/output/coffee'
CLEVER_OUTPUT_OBJPATH = 'dischargeOBS/output/clever'
FRASER_OUTPUT_OBJPATH = 'dischargeOBS/output/warns/fraser'
SKEENA_OUTPUT_OBJPATH = 'dischargeOBS/output/warns/skeena'

SOURCE_HYDRO_DATETIME_FORMAT = 'YYYY-MM-DD HH:mm:ssZZ'

DEMO_VAR = 3