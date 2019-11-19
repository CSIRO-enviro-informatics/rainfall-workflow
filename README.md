# Rainfall workflow

Workflow which uses observed and forecast precipitation data, processes and transforms it, with the eventual goal of creating a soil moisture forecast API. 

Use Workflow_Guide.ipynb to familiarise yourself with the main workflow steps. 

## Use notes

- Need NCI login and membership in lb4 project (to get new ACCESS-G data). Edit transfer_files.py with these credentials. 
- Best on Unix systems - (if Windows, need to add equivalent network locations in settings.py and there have been problems with packages working). 
- After cloning the repository, set up the conda environment.  
  `conda env create -f environment.yml`

- External Cython packages are used: pytrans, pybjp. <br>
  pytrans: https://bitbucket.csiro.au/projects/SF/repos/transformation/browse.  <br>
  pybjp: contact Andrew. <br> 
  After downloading these, edit setup.py as needed (eg. to reflect locations of C++ libraries), and build packages with: <br>
  `python setup.py build_ext --inplace` <br>
  Move the created .so or .pyd files to working directory to install. 

- Generate HTML documentation by installing [Doxygen](http://www.doxygen.nl/manual/install.html) and running: <br>
  `doxygen Doxyfile` 

## Project file overview

workflow.py - Main file <br>
worflow_tests.py, test.py - Test files <br>
transform.py, bjpmodel.py,shuffle.py - Data processing files primarily using extrenal packages <br>
forecast_cube.py, parameter_cube.py, source_cube.py - Files managing netCDF4 files, or "cubes" - they contain similar functions, but have been split up for cleanliness. <br>
data_transfer.py, iris_regridding.py - Get ACCESS-G and SMIPS data (respectively), pre-process it, and save it to a network location. <br>
dates.py - Date functions package. 
settings.py - Stores file names and variables.
