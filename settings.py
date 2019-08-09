import platform
import datetime

if 'Linux' in platform.platform():
    ACCESS_G_PATH = '/OSM/CBR/LW_SATSOILMOIST/source/BOM-ACCESS-G/ACCESS_G_12z/'  # access-g write path
    SMIPS_PATH = '/OSM/CBR/LW_SATSOILMOIST/processed/SMIPSv0.5/thredds/public/SMIPS/'  # path to source SMIPS container
    SMIPS_DEST_PATH = '//OSM/CBR/LW_SOILDATAREPO/work/SMIPSRegrid/'  # smips write path
    SMIPS_AGG = '//OSM/CBR/LW_SOILDATAREPO/work/SMIPSRegrid/SMIPS.nc'  # path to aggregated smips file
    ACCESS_G_AGG = '/OSM/CBR/LW_SATSOILMOIST/source/BOM-ACCESS-G/ACCESS_G_12z/ACCESS-G.nc'  # path to aggregated access-g file

else:  # Windows
    ACCESS_G_PATH = '//osm-12-cdc.it.csiro.au/OSM_CBR_LW_SATSOILMOIST_source/BOM-ACCESS-G/ACCESS_G_12z/'
    SMIPS_PATH = '//osm-12-cdc.it.csiro.au/OSM_CBR_LW_SATSOILMOIST_processed/SMIPSv0.5/thredds/public/SMIPS/'
    #SMIPS_PATH = '\\\\osm-12-cdc.it.csiro.au\\OSM_CBR_LW_SATSOILMOIST_processed\\SMIPSv0.5\\thredds\\public\\SMIPS\\'


SMIPS_CONTAINER = 'SMIPSv0.5.nc'  # name of container in SMIPS_PATH
TEST_PATH = 'temp/'  # local directory for temporary file saving - needs cleaning out

ACCESS_HOUR = '1200'  # time the access-g forecasts are taken at
ACCESS_HR = ACCESS_HOUR[:2]

SMIPS_STARTDATE = datetime.date(2015, 11, 20)  # smips data is available from this date
ACCESS_STARTDATE = datetime.date(2016, 3, 16)  # access-g data is available from this date

yesterday = datetime.date.today() - datetime.timedelta(days=1)

# combine with ACCESS_G_PATH for a specific file path
def access_g_filename(str_date):
    return str_date[:4] + '/ACCESS_G_accum_prcp_fc_' + str_date + ACCESS_HR + '.nc'

# combine with SMIPS_DEST_PATH for a specific file path
def smips_filename(str_date):
    return str_date[:4] + '/SMIPS_blnd_prcp_regrid_'+ str_date +'.nc'