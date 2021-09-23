import os
import subprocess
import sys
from os import path, getuid
from pathlib import Path

from . import settings
from .util import grouper, distributor
FALSIES = ('f', 'F', 'FALSE', 'false', '0', 'off', 0, False)

MY_UID = getuid()

# hwloc opencl plugin is currently broken on latest Ubuntu with latest MESA_OPENCL_ICD version
os.putenv("HWLOC_PLUGINS_BLACKLIST", "hwloc_opencl")

USE_MPI = False
comm = None
world_size = None
rank = None
local_comm = None
local_size = None
local_rank = None

MPI_env_options = [
    "OMPI_COMM_WORLD_RANK",
    "PMIX_RANK",
    "MPI_LOCALRANKID",
    "PMI_RANK"
]
for e in MPI_env_options:
    r = os.getenv(e, None)
    if not r:
        continue
    try:
        USE_MPI = int(r)
    except ValueError:
        continue
    print("Found env var {}={}".format(e, USE_MPI), flush=True)
    print("Enabling MPI mode.", flush=True)
    break
else:
    print("MPI Env vars not found. Not enabling MPI mode.", flush=True)

if USE_MPI is not False:
    try:
        print("({}) Attempting to load mpi4py...".format(USE_MPI), flush=True)
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        world_size = comm.Get_size()
        rank = comm.Get_rank()
        assert rank == USE_MPI
        local_comm = comm.Split_type(MPI.COMM_TYPE_SHARED)
        local_rank = local_comm.Get_rank()
        local_size = local_comm.Get_size()
        print("Activated MPI with RANK={} of {} / LOCAL_RANK={} of {}".format(rank, world_size, local_rank, local_size), flush=True)
    except Exception as e:
        print("Cannot use MPI mode. Exiting.", flush=True)
        raise
    USE_MPI = True

def task_test():
    print("Attempting Cython Module import test...")
    import pytrans
    import pybjp
    print("Attempting settings lookup test...")
    print("ACCESS-G Aggregation: {}".format(settings.ACCESS_G_AGG))
    print("SMIPS Daily Aggregation: {}".format(settings.SMIPS_REGRID_AGG()))
    print("Params Grids Path: {}".format(settings.PARAMS_GRIDS_PATH()))
    print("Seems like it worked?")

def task_test_mpi():
    if not USE_MPI:
        raise RuntimeError("MPI Mode not detected.")
    else:
        print("TESTING MPI Scatter...", flush=True)
        data_bits = []
        if rank == 0:
            source_data = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
            data_bits = list(distributor(source_data, world_size))
        data_bits = comm.scatter(data_bits, root=0)
        print("{} = {}".format(rank, str(data_bits)), flush=True)

        print("TESTING MPI Gather...", flush=True)
        done = True
        done = comm.gather(done, root=0)
        if rank == 0:
            print("{} = {}".format(rank, len(done)))
        else:
            print("{} = {}".format(rank, str(done)))


def make_local_copy(in_file: Path):
    local_path = Path(os.getenv("LOCALDIR", ".")).absolute()
    local_name = in_file.name
    print("making local copy of {} to {}".format(local_name, local_path))
    args = [
        "rsync",
        "-a",
        "-u",
        "-I",
        str(in_file.absolute()),
        str(local_path)+path.sep
    ]
    subprocess.run(args, shell=False, check=True)
    new_name = local_path / local_name
    if not new_name.exists():
        raise RuntimeError("Cannot copy {} to {}".format(in_file, new_name))
    return new_name


def task_check_bad_smips():
    from . import main
    return main.check_for_bad_smips()


def task_forecast():
    from . import main
    from . import dates
    import time
    at_date = os.getenv("WORKFLOW_FORECAST_DATE", "")
    if at_date:
        at_date = dates.datetime_from_iso(at_date).date()
    else:
        at_date = None
    t1 = time.perf_counter()
    did_date = main.create_forecast_files(at_date, multi_process=True)
    t2 = time.perf_counter()
    print("Create Forecast took {} seconds".format(t2-t1), flush=True)
    did_date = at_date
    ret = main.create_shuffled_forecasts(did_date, multi_process=True)
    t3 = time.perf_counter()
    print("Shuffle Forecast took {} seconds".format(t3-t2), flush=True)
    print("Total elapsed time: {} seconds".format(t3-t1), flush=True)
    return ret


def task_params():
    copylocal = os.getenv("WORKFLOW_COPY_LOCAL", "false") not in FALSIES
    ACCESS_G_AGG = Path(settings.ACCESS_G_AGG)
    if not ACCESS_G_AGG.exists():
        raise FileNotFoundError(ACCESS_G_AGG)
    SMIPS_AGG = settings.SMIPS_REGRID_AGG()
    if not SMIPS_AGG.exists():
        raise FileNotFoundError(SMIPS_AGG)

    local_files = []

    if copylocal:
        # We want to ensure only one instance of the process does copylocal on a single node
        if not USE_MPI or (USE_MPI and local_rank is not None and local_rank == 0):
            new_file = make_local_copy(SMIPS_AGG)
            SMIPS_AGG = settings.update_setting('SMIPS_AGG', new_file)
            local_files.append(new_file)
            new_file = make_local_copy(ACCESS_G_AGG)
            ACCESS_G_AGG = settings.update_setting('ACCESS_G_AGG', new_file)
            local_files.append(new_file)
            barrier = [SMIPS_AGG, ACCESS_G_AGG]
        else:
            print("Waiting for other local process to copy files needed to this node...", flush=True)
            barrier = ["", ""]
        if USE_MPI and local_comm is not None:
            barrier = local_comm.bcast(barrier, root=0)
            if local_rank != 0:
                SMIPS_AGG = settings.update_setting('SMIPS_AGG', barrier[0])
                ACCESS_G_AGG = settings.update_setting('ACCESS_G_AGG', barrier[1])
    try:
        from . import main
        use_mpi = {'rank': rank, 'world_size': world_size, 'comm': comm} if bool(
            USE_MPI and comm is not None) else False
        main.create_parameter_files(multi_process=True, restrict_nsw=False, use_mpi=use_mpi)
    finally:
        for lf in local_files:
            os.unlink(str(lf))

def task_get_latest_accessg():
    from . import data_transfer, source_cube
    accessg_dates = data_transfer.get_latest_accessg_files()
    source_cube.aggregate_netcdf(accessg=True, update_only=True)
    #source_cube.aggregate_netcdf(accessg=True, just_dates=accessg_dates)

def task_get_latest_smips():
    import datetime
    from . import iris_regridding, source_cube
    #smips_dates = iris_regridding.run_smips_regridding()
    #smips_dates = iris_regridding.run_smips_regridding(update_only=True, start_date=datetime.date(2020,8,31), end_date=datetime.date(2020,8,31))
    smips_dates = iris_regridding.run_smips_regridding(update_only=True, end_date=datetime.date(2021, 2, 21))
    #source_cube.aggregate_netcdf(smips=True, just_dates=smips_dates)
    source_cube.aggregate_netcdf(smips=True, update_only=False)

def task_split_access_g():
    from . import main
    main.split_accum_access_g()

def ep():
    task = os.getenv("WORKFLOW_TASK", "test").lower()
    module = sys.modules[__name__]
    print("Workflow script entrypoint reached, UID={}".format(MY_UID))
    print("Doing Workflow Task: {}".format(task))
    task_fn_name = "task_"+task
    fn = getattr(module, task_fn_name, None)
    if fn and callable(fn):
        fn()
    else:
        raise RuntimeError("Cannot execute unknown task {}".format(task))

if __name__ == "__main__":
    ep()
