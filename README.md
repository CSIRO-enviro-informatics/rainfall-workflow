# Rainfall workflow

Workflow which uses observed and forecast precipitation data, processes and transforms it, with the eventual goal of creating a soil moisture forecast API. 

## Use notes

- Need NCI login and membership in lb4 project (to get new ACCESS-G data). 
- Best on Unix systems - (if Windows, need to add equivalent network locations in settings.py and there have been problems with packages working). 
- Set up environment with environment.yml. 
- External Cython packages are used: pytrans, pybjp. Need to build and install these. 
- Generate HTML documentation by installing doxygen and running "doxygen Doxyfile". 


## Project file overview

workflow.py - Main file <br>
worflow_tests.py, test.py - Test files <br>
transform.py, bjpmodel.py,shuffle.py - Data processing files primarily using extrenal packages <br>
forecast_cube.py, parameter_cube.py, source_cube.py - Files managing netCDF4 files, or "cubes" - they contain similar functions, but have been split up for cleanliness. <br>
data_transfer.py, iris_regridding.py - Get ACCESS-G and SMIPS data (respectively), pre-process it, and save it to a network location. <br>
dates.py - Date functions package. 
settings.py - Stores file names and variables.

