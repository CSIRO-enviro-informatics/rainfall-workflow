import os
import settings
import datetime
import parameter_cube, forecast_cube
import transform

test_coords = [-19.21875, 123.046875]
test_date = datetime.date(2019, 1, 1)

# CAUTION: test functions will delete the files they are overwriting


def delete_file(file):
    if os.path.exists(file):
        os.remove(file)
    else:
        print('File ' + file + ' does not exist to be deleted')


def test_parameter_file_creation():
    lat = test_coords[0]
    lon = test_coords[1]
    fname = settings.params_filename(lat, lon)
    delete_file(settings.PARAMS_GRIDS_PATH + fname)
    parameter_cube.generate_forecast_parameters(lat, lon)


def test_forecast_file_creation():
    lat = test_coords[0]
    lon = test_coords[1]
    mu, cov, tp = parameter_cube.read_parameters(lat, lon)
    fname = settings.forecast_filename(lat, lon)
    delete_file(settings.FORECAST_GRID_PATH + fname)
    transform.transform_forecast(lat, lon, test_date, mu, cov, tp)


def test_forecast_aggregation():
    # delete file
    fname = settings.FORECAST_AGG
    delete_file(fname)
    forecast_cube.aggregate_netcdf(test_date)


def test_parameter_aggregation():
    # delete file
    fname = settings.PARAMS_AGG
    delete_file(fname)
    parameter_cube.aggregate_netcdf()


if __name__ == '__main__':
    test_parameter_file_creation()
    test_parameter_aggregation()
    test_forecast_file_creation()
    test_forecast_aggregation()
