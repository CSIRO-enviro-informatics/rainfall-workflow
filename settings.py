import platform
import datetime

if 'Linux' in platform.platform():
    ACCESS_G_PATH = '/OSM/CBR/LW_SATSOILMOIST/source/BOM-ACCESS-G/ACCESS_G_12z/'
    SMIPS_PATH = '/OSM/CBR/LW_SATSOILMOIST/processed/SMIPSv0.5/thredds/public/SMIPS/'

else:
    ACCESS_G_PATH = '//osm-12-cdc.it.csiro.au/OSM_CBR_LW_SATSOILMOIST_source/BOM-ACCESS-G/ACCESS_G_12z/'
    SMIPS_PATH = '//osm-12-cdc.it.csiro.au/OSM_CBR_LW_SATSOILMOIST_processed/SMIPSv0.5/thredds/public/SMIPS/'
    #SMIPS_PATH = '\\\\osm-12-cdc.it.csiro.au\\OSM_CBR_LW_SATSOILMOIST_processed\\SMIPSv0.5\\thredds\\public\\SMIPS\\'


SMIPS_CONTAINER = 'SMIPSv0.5.nc'
TEST_PATH = 'test/'

ACCESS_HOUR = '1200'
ACCESS_HR = ACCESS_HOUR[:2]

OUTCUBEPATH = TEST_PATH
EFFECTIVESTARTDATE = datetime.datetime(2015, 11, 20)

def access_g_filename(str_date):
    return str_date[:4] + '/ACCESS_G_accum_prcp_fc_' + str_date + ACCESS_HR + '.nc'

def smips_filename(str_date):
    return 'test/' + str_date[:4] + '/SMIPS_blnd_prcp_regrid_'+ str_date +'.nc'