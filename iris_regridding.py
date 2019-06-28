	# 1. Grab the layer (2D array representing one time step) from the ACCESS-G container
	# 	○ One timestep at a time because this'll need to happen in the future when daily data is coming in.
	# 2. Resample it (in Python if you can)
	# 	○ Using Iris?
	# 3. Push it into a new NetCDF container (code in matt_resampling)
	# 4. Drill the container to get your timeseries for each pixel (grid centroid)
	# 5. Store the time series in fluxDB

import iris
import settings

smips_file = settings.SMIPS_PATH + settings.SMIPS_CONTAINER
smips_cubes = iris.load(smips_file) #, ['Blended_Precipitation', 'blended_precipitation'])
print(smips_cubes)
