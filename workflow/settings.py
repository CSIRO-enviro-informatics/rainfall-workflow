"""\ Variables and functions mostly regarding file locations and names."""
import glob
import pathlib
import sys
import os
from os import path, getenv
import platform
import datetime
from .dates import date2str
from .util import in_docker, in_kubernetes

module = sys.modules[__name__]
defaults = module.defaults = {
    'FORECAST_VALUE_PRECISION': 'f4',  # f8 for double, f4 for float
    'SMIPS_PRECIP_VALUE_PRECISION': 'f4',
    'TEMP_PATH': 'temp'+path.sep,  # local directory for temporary file saving - needs cleaning out
    'SMIPS_CONTAINER': 'SMIPSv0.5.nc',  # name of container in SMIPS_PATH'
    'ACCESS_HOUR': '1200',  # time the access-g forecasts are taken at
    'ACCESS_HR': '12',
    'ACCESS_FCHOURS': '240',
    'SMIPS_STARTDATE': datetime.date(2015, 11, 20),  # smips data is available from this date
    'ACCESS_STARTDATE': datetime.date(2016, 3, 16),  # access-g data is available from this date
    'ACCESS_G_NUM_LEAD_DAYS': 10,
    # Hours after ACCESS_G 12z time that we need to wait until its available to us
    # Eg, 12z + 10 (ACCESS_G_UPDATE_HOUR) = 22z. 22 + 10(utc offset) = 8am local time.
    # or: 12z + 11 (ACCESS_G_UPDATE_HOUR) = 23z. 23 + 10(utc offset) = 9amm local time.
    'ACCESS_G_UPDATE_HOUR': 11,
    'FORECAST_DATE': datetime.date(2020, 8, 1),  # date for forecast
    'PARAMS_DATE': datetime.date(2021, 1, 31),
    'MP_NUM_PROCESSES': "2",  # use 2-8 processes per node in multiprocessing mode
    # Set MP_NUM_PROCESSES to 1 on MPI-based runs!
    'SMIPS_PRECIP_SOURCE': "MLPrecip" + path.sep + "v0.6" + path.sep,  # options: "SMIPS", "blendedPrecipOutputs", "MLPrecip"
    "MLPRECIP_USE_HTTP": True,
    'MLPRECIP_URL': "http://lw-111-cdc.it.csiro.au:8082/MLPrecip/v0.6/"
    #'SMIPS_PRECIP_SOURCE': "SMIPS"  # options: "SMIPS", "blendedPrecipOutputs", "MLPrecip"
}
if in_docker() or in_kubernetes():
    defaults['ACCESS_G_APS2_PATH'] = '/mnt/ACCESS-G-APS2/ACCESS_G_12z/'  # access-g aps2 write path
    defaults['ACCESS_G_APS3_PATH'] = '/mnt/ACCESS-G-APS3/ACCESS_G_12z/'  # access-g aps3 write path
    defaults['ACCESS_G_REGRIDDED_APS2_PATH'] = '/mnt/ACCESS-G-APS2-3/ACCESS_G_12z/'  # access-g aps3 write path
    defaults['SMIPS_PATH'] = '/mnt/SMIPS/'  # path to source SMIPS container
    defaults['SMIPS_DEST_PATH'] = '/mnt/SMIPSRegrid/'  # smips write path
    defaults['SMIPS_REGRID_AGG_PATH'] = '/mnt/SMIPSRegrid/'  # path to aggregated smips file
    defaults['ACCESS_G_AGG'] = '/mnt/ACCESS-G-APS3/ACCESS-G.nc'  # path to aggregated access-g file
    defaults['ALTERNATE_ACCESS_G_AGG'] = False
    defaults['FC_PATH'] = '/mnt/RainfallForecasts/'
    defaults['MLPRECIP_PATH'] = '/mnt/'+defaults['SMIPS_PRECIP_SOURCE']
    defaults['BLENDED_PRECIP_PATH'] = '/mnt/blendedPrecipOutputs/'
elif 'Linux' in platform.platform():
    defaults['ACCESS_G_APS2_PATH'] = '/datasets/work/lw-soildatarepo/work/RainfallWorkflow/ACCESS-G-APS2/ACCESS_G_12z/'  # access-g aps2 write path
    defaults['ACCESS_G_APS3_PATH'] = '/datasets/work/lw-soildatarepo/work/RainfallWorkflow/ACCESS-G-APS3/ACCESS_G_12z/'  # access-g aps3 write path
    defaults['ACCESS_G_REGRIDDED_APS2_PATH'] = '/datasets/work/lw-soildatarepo/work/RainfallWorkflow/ACCESS-G-APS2-3/ACCESS_G_12z/'  # access-g aps3 write path
    defaults['SMIPS_PATH'] = '/datasets/work/lw-satsoilmoist/work/processed/thredds/ternlandscapes/public/SMIPS/'  # path to source SMIPS container
    defaults['SMIPS_DEST_PATH'] = '/datasets/work/lw-soildatarepo/work/SMIPSRegrid/'  # smips write path
    defaults['SMIPS_REGRID_AGG_PATH'] = '/datasets/work/lw-soildatarepo/work/SMIPSRegrid/'  # path to aggregated smips file
    defaults['ACCESS_G_AGG'] = '/datasets/work/lw-soildatarepo/work/RainfallWorkflow/ACCESS-G-APS3/ACCESS-G.nc'  # path to aggregated access-g file
    defaults['ALTERNATE_ACCESS_G_AGG'] = False
    defaults['FC_PATH'] = '/datasets/work/lw-satsoilmoist/work/processed/RainfallForecasts/'
    defaults['MLPRECIP_PATH'] = '/datasets/work/lw-satsoilmoist/work/processed/'+defaults['SMIPS_PRECIP_SOURCE']
    defaults['BLENDED_PRECIP_PATH'] = '/datasets/work/lw-satsoilmoist/work/processed/blendedPrecipOutputs/'
    #if os.path.exists("/home/som05d/ssd/scratch"):
        # defaults['SMIPS_REGRID_AGG_PATH'] = '/home/som05d/ssd/scratch/source/SMIPS.nc'  # path to aggregated smips file
        #defaults['ACCESS_G_AGG'] = '/home/som05d/ssd/scratch/source/ACCESS-G.nc'  # path to aggregated smips file
        # defaults['ALTERNATE_ACCESS_G_AGG'] = "/home/som05d/t1/ACCESS-G.nc"
        # defaults['FC_PATH'] = '/home/som05d/ssd/scratch/source/'

else:  # Windows
    defaults['ACCESS_G_APS2_PATH'] = '\\\\fs1-cbr.nexus.csiro.au\\{lw-soildatarepo}\\work\\RainfallWorkflow\\ACCESS-G-APS2\\ACCESS_G_12z\\'
    defaults['ACCESS_G_APS3_PATH'] = '\\\\fs1-cbr.nexus.csiro.au\\{lw-soildatarepo}\\work\\RainfallWorkflow\\ACCESS-G-APS3\\ACCESS_G_12z\\'
    defaults['ACCESS_G_REGRIDDED_APS2_PATH'] = '\\\\fs1-cbr.nexus.csiro.au\\{lw-soildatarepo}\\work\\RainfallWorkflow\\ACCESS-G-APS2-3\\ACCESS_G_12z\\'
    defaults['SMIPS_PATH'] = '\\\\osm-12-cdc.it.csiro.au\\OSM_CBR_LW_SATSOILMOIST_processed\\thredds\\ternlandscapes\\public\\SMIPS\\'
    defaults['SMIPS_DEST_PATH'] = '\\\\fs1-cbr.nexus.csiro.au\\{lw-soildatarepo}\\work\\SMIPSRegrid\\'
    defaults['SMIPS_REGRID_AGG_PATH'] = '\\\\fs1-cbr.nexus.csiro.au\\{lw-soildatarepo}\\work\\SMIPSRegrid\\'
    defaults['ACCESS_G_AGG'] = '\\\\fs1-cbr.nexus.csiro.au\\{lw-soildatarepo}\\work\\RainfallWorkflow\\ACCESS-G-APS3\\ACCESS-G.nc'
    defaults['FC_PATH'] = '\\\\fs1-cbr.nexus.csiro.au\\{lw-soildatarepo}\\work\\processed\\RainfallForecasts\\'
    defaults['MLPRECIP_PATH'] = '\\\\fs1-cbr.nexus.csiro.au\\{lw-soilsoilmoist}\\work\\processed\\'+defaults['SMIPS_PRECIP_SOURCE']
    defaults['BLENDED_PRECIP_PATH'] = '\\\\fs1-cbr.nexus.csiro.au\\{lw-soilsoilmoist}\\work\\processed\\blendedPrecipOutputs\\'

config = module.config = dict()

ACCESS_G_APS2_PATH = config['ACCESS_G_APS2_PATH'] = getenv("ACCESS_G_APS2_PATH", None)
ACCESS_G_APS3_PATH = config['ACCESS_G_APS3_PATH'] = getenv("ACCESS_G_APS3_PATH", None)
ACCESS_G_REGRIDDED_APS2_PATH = config['ACCESS_G_REGRIDDED_APS2_PATH'] = getenv("ACCESS_G_REGRIDDED_APS2_PATH", None)
SMIPS_PATH = config['SMIPS_PATH'] = getenv("SMIPS_PATH", None)
SMIPS_DEST_PATH = config['SMIPS_DEST_PATH'] = getenv("SMIPS_DEST_PATH", None)
SMIPS_REGRID_AGG_PATH = config['SMIPS_REGRID_AGG_PATH'] = getenv("SMIPS_REGRID_AGG_PATH", None)
ACCESS_G_AGG = config['ACCESS_G_AGG'] = getenv("ACCESS_G_AGG", None)
MLPRECIP_PATH = config['MLPRECIP_PATH'] = getenv("MLPRECIP_PATH", None)
MLPRECIP_URL = config['MLPRECIP_URL'] = getenv("MLPRECIP_URL", None)
BLENDED_PRECIP_PATH = config['BLENDED_PRECIP_PATH'] = getenv("BLENDED_PRECIP_PATH", None)
FC_PATH = config['FC_PATH'] = getenv("FC_PATH", None)
TEMP_PATH = config['TEMP_PATH'] = getenv("TEMP_PATH", None)
MP_NUM_PROCESSES = config['MP_NUM_PROCESSES'] = getenv("MP_NUM_PROCESSES", None)

# Add defaults into Config dict
for k, v in defaults.items():
    if k not in config or config[k] is None:
        if v is None:
            raise RuntimeError("No default value available for config key {}".format(k))
        config[k] = v

# Add config dict as attributes on settings module
for k, v in config.items():
    setattr(module, k, v)

TEST_PATH = config['TEST_PATH'] = config['TEMP_PATH']


def PARAMS_PATH():
    return config['FC_PATH'] + 'params' + path.sep

def PARAMS_GRIDS_PATH():
    return PARAMS_PATH() + 'grids' + path.sep

def PARAMS_AGG():
    return PARAMS_PATH() + 'PARAMS_aggregated.nc'

def SMIPS_REGRID_AGG():
    src = config['SMIPS_PRECIP_SOURCE']
    path = config['SMIPS_REGRID_AGG_PATH']
    if src == "SMIPS":
        return pathlib.Path(path + "SMIPS_Regrid.nc")
    elif src == "blendedPrecipOutputs":
        return pathlib.Path(path + "SMIPS_blendedPrecip_Regrid.nc")
    elif src.startswith("MLPrecip"):
        return pathlib.Path(path + "SMIPS_MLPrecip_Regrid.nc")
    else:
        raise RuntimeError("Bad SMIPS_PRECIP_SOURCE: {}".format(src))

def FORECAST_PATH():
    return config['FC_PATH'] + 'forecast' + path.sep

def FORECAST_GRID_PATH():
    return FORECAST_PATH() + 'grids' + path.sep

def FORECAST_SHUFFLE_PATH():
    return FORECAST_PATH() + 'shuffled' + path.sep

restrict_size = True  # Setting to drop off the first longitude and last two longitudes, and top and bottom latitudes

def update_setting(key, value):
    config = module.config
    setattr(module, key, value)
    config[key] = value
    setattr(module, 'config', config)
    return value


# combine with ACCESS_G_PATH for a specific file path
def access_g_filename(str_date):
    str_date = date_type_check(str_date)
    return str_date[:4] + path.sep + 'ACCESS_G_accum_prcp_fc_' + str_date + config['ACCESS_HR'] + '.nc'

def make_access_g_product_filename(str_date, fchours=240):
    str_date = date_type_check(str_date)
    fc_hours_str = "{:03d}".format(fchours)
    return 'IDY25006.APS3.group1.slv.' + str_date + config['ACCESS_HR'] + '.' + fc_hours_str + '.surface.nc4'


# combine with SMIPS_DEST_PATH for a specific file path
def smips_regrid_dest_filename(str_date):
    src = config['SMIPS_PRECIP_SOURCE']
    if src == "SMIPS":
        str_date = date_type_check(str_date)
        return str_date[:4] + path.sep + 'SMIPS_blnd_prcp_regrid_'+ str_date +'.nc'
    elif src == "blendedPrecipOutputs":
        return str_date[:4] + path.sep + 'SMIPS_blnd_prcp_regrid_'+ str_date +'.nc'
    elif src.startswith("MLPrecip"):
        return ml_precip_regrid_dest_filename(str_date)
    else:
        raise RuntimeError("Bad SMIPS_PRECIP_SOURCE: {}".format(src))


def ml_precip_regrid_dest_filename(str_date, date_at="end", ext="nc"):
    str_date = date_type_check(str_date)
    if date_at == "start":
        return str_date[:4] + path.sep + str_date + "_ML_Daily_Precip_est_1dd_regrid." + ext
    else:
        return str_date[:4] + path.sep + "ML_Daily_Precip_est_1dd_regrid_" + str_date + "." + ext

def ml_precip_src_path(str_date, date_at="end", ext="tif"):
    str_date = date_type_check(str_date)
    if module.MLPRECIP_USE_HTTP is True:
        if date_at == "start":
            return MLPRECIP_URL + str_date[:4] + '/' + str_date + "_ML_Daily_Precip_est_1dd." + ext
        else:
            return MLPRECIP_URL + str_date[:4] + '/' + "ML_Daily_Precip_est_1dd_" + str_date + "." + ext
    else:
        if date_at == "start":
            return MLPRECIP_PATH + str_date[:4] + path.sep + str_date + "_ML_Daily_Precip_est_1dd." + ext
        else:
            return MLPRECIP_PATH + str_date[:4] + path.sep + "ML_Daily_Precip_est_1dd_" + str_date + "." + ext

def smips_dest_mask():
    src = config['SMIPS_PRECIP_SOURCE']
    if src == "SMIPS":
        mask = 'SMIPS_blnd_prcp_regrid*.nc'
    elif src == "blendedPrecipOutputs":
        mask = 'SMIPS_blnd_prcp_regrid*.nc'
    elif src.startswith("MLPrecip"):
        mask = 'ML_Daily_Precip_est_1dd_regrid*.nc'
    else:
        raise RuntimeError("Bad SMIPS_PRECIP_SOURCE: {}".format(src))
    return mask

def find_smips_dest_files(path=None):
    if path is None:
        path = config['SMIPS_DEST_PATH']
    mask = smips_dest_mask()
    return [file for file in glob.glob(path + '*/' + mask)]



# combine with PARAMS_GRIDS_PATH for a specific file path
def params_filename(lat, lon):
    lat = str(lat)
    lon = str(lon)
    if lat.startswith("-"):
        lat = lat.replace("-", "s", 1)
    else:
        lat = "n" + lat
    if lon.startswith("-"):
        lon = lon.replace("-", "w", 1)
    else:
        lon = "e" + lon
    return lat + path.sep + lon + '.nc'


def forecast_filename(str_date, lat, lon):
    str_date = date_type_check(str_date)
    lat = str(lat)
    lon = str(lon)
    if lat.startswith("-"):
        lat = lat.replace("-", "s", 1)
    else:
        lat = "n" + lat
    if lon.startswith("-"):
        lon = lon.replace("-", "w", 1)
    else:
        lon = "e" + lon
    return 'forecast_'+ str_date + path.sep + str(lat) + path.sep + str(lon) + '.nc'

def forecast_dirname(str_date):
    str_date = date_type_check(str_date)
    return 'forecast_'+ str_date + path.sep


def shuffled_forecast_filename(str_date, lat, lon):
    str_date = date_type_check(str_date)
    lat = str(lat)
    lon = str(lon)
    if lat.startswith("-"):
        lat = lat.replace("-", "s", 1)
    else:
        lat = "n" + lat
    if lon.startswith("-"):
        lon = lon.replace("-", "w", 1)
    else:
        lon = "e" + lon
    return 'shuffledforecast_' + str_date + path.sep + lat + path.sep + lon + '.nc'


def shuffled_forecast_dirname(str_date):
    str_date = date_type_check(str_date)
    return 'shuffledforecast_' + str_date + path.sep


def forecast_agg(str_date):
    str_date = date_type_check(str_date)
    return FORECAST_PATH() + str_date + '_FORECAST_aggregated.nc'


def shuffled_forecast_agg(str_date):
    str_date = date_type_check(str_date)
    return FORECAST_PATH() + 'shuffled_' +  str_date + '_FORECAST_aggregated.nc'


def date_type_check(date):
    if type(date) is str:
        return date
    else:
        if type(date) is datetime.date or type(date) is datetime.datetime:
            return date2str(date)
        else:
            raise TypeError('Date is of the wrong type: ' + type(date))
