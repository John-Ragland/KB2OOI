import xarray as xr
import bighorn
import numpy as np
from matplotlib import pyplot as plt
import matplotlib
from matplotlib import colors as mcolors
from scipy import signal
from tqdm import tqdm
import multiprocessing as mp
import kaooi
from matplotlib import dates as mdates
import matplotlib.lines as lines
from dask.distributed import Client, LocalCluster
import pandas as pd
import envy
import geopy.distance as geo
from matplotlib import gridspec
from kaooi.coordinates import coords, depths
import hvplot.xarray 
#hydrophones.remove('LJ03A')
#hydrophones.remove('HYSB1')
from scipy.interpolate import interp1d
from datetime import datetime
from dotenv import load_dotenv
import os

if __name__ == '__main__':
    # load .env file
    env_path = '../.env'
    _ = load_dotenv(env_path)

    # spin up local dask cluster
    cluster = LocalCluster(
        n_workers=8,           # Number of workers
        processes=True,        # Use processes instead of threads
        threads_per_worker=4,  # Threads per worker
        memory_limit='50GB',    # Memory limit per worker
        dashboard_address=':8787'  # Dashboard address and port
    )

    # Connect to the cluster
    client = Client(cluster)

    # Print the dashboard URL
    print(f"Dask dashboard available at: {client.dashboard_link}")

    print('openning datasets...')
    bb = kaooi.open_ooi_bb(compute=True)
    bb_proc = kaooi.process_data(bb, sampling_rate=500)

    lf = kaooi.open_ooi_lf(compute=True)
    lf_proc = kaooi.process_data(lf, sampling_rate=200)

    hydrophones = [
        'AXBA1',
        'AXCC1',
        'AXEC2',
        'HYS14',
        'LJ01C',
        'PC01A',
        'PC03A',
        'LJ01A',
        'LJ01D',
        'HYSB1'
    ]

    # estimate arrival times for stacking
    print('estimating absolute arrival times...')
    T0s = {}

    for node in hydrophones:
        ssp = envy.get_ssp_slice(coords['KB'], coords[node], 50, fillna=True).load()
        kb_range = geo.great_circle(coords['KB'], coords[node]).km
        integrated_minimum = ssp.min('depth').mean('range')

        # time of last arrival (time spread is only at maximum 10 s)
        T0s[node] =  float(kb_range*1000 / integrated_minimum)

    bb_stack = {}
    for node in bb_proc.keys():
        bb_stack[node] = bb_proc[node].sel({'longtime':slice(T0s[node], T0s[node]+20*60)}).mean('longtime')
    bb_stack = xr.Dataset(bb_stack)

    lf_stack = {}
    for node in lf_proc.keys():
        lf_stack[node] = lf_proc[node].sel({'longtime':slice(T0s[node], T0s[node]+20*60)}).mean('longtime')
    lf_stack = xr.Dataset(lf_stack)

    # check that analysis directory exists
    if not os.path.exists(f'{os.environ["data_directory"]}analysis'):
        os.makedirs(f'{os.environ["data_directory"]}analysis')

    # save to disk
    print('computing arrivals and saving to disk...')
    fn = f'{os.environ["data_directory"]}analysis/bb_stack.nc'
    np.abs(bb_stack).to_netcdf(fn)

    fn = f'{os.environ["data_directory"]}analysis/lf_stack.nc'
    np.abs(lf_stack).to_netcdf(fn)