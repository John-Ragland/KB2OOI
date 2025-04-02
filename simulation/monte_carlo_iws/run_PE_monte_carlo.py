# create mixed iw perturbation
import os
import envy
from kaooi.coordinates import coords, depths
import iwGM
import numpy as np
import xarray as xr
import bighorn
from geopy.distance import geodesic
import sys
import argparse
import pathlib
from dotenv import load_dotenv

if __name__ == '__main__':

    # Set up argument parser
    parser = argparse.ArgumentParser(description='Monte Carlo simulation script')
    parser.add_argument('node', type=str, help='Node identifier (e.g. AXCC1)')
    parser.add_argument('dciw_filepath', type=str, 
                    help='Path to dciw realization file (e.g. /path/to/dciw_001.nc)')

    args = parser.parse_args()

    # load .env file
    current_file_path = pathlib.Path(__file__).resolve()
    env_path = f'{current_file_path.parent.parent.parent}/.env'
    load_dotenv(env_path)

    # Replace hardcoded values with command line arguments
    node = args.node
    dciw_filepath = args.dciw_filepath

    realization = int(dciw_filepath[-6:-3])

    fnr = f'{os.environ['data_directory']}mc_iws/{node}_{realization:02}_Gfz_real.nc'
    fni = f'{os.environ['data_directory']}mc_iws/{node}_{realization:02}_Gfz_imag.nc'

    # check if simulation has already been run:
    if os.path.exists(fnr) and os.path.exists(fni):
        print(f'simulation file already exists for {node}, skipping...')
        sys.exit()

    print('loading environment')
    ssp = envy.get_ssp_slice(
        coords['KB'],
        coords[node],
        num_range_points=3000,
        fillna=True,
        climate=True,
    ).load()

    bathy = envy.get_bathymetry_slice(
        coords['KB'],
        coords[node],
        num_range_points=3000,
    ).load()

    # load iw perturbations
    dciw = xr.open_dataarray(dciw_filepath)

    # combine climate and iw perturbations
    ssp_dciw = ssp.interp({'range':dciw.range}) + dciw.interp({'depth':ssp.depth}, kwargs={'bounds_error':False, 'fill_value':'extrapolate'})

    # flat earth transform sound speed and bathymetry
    print('computing flat earth transform...')
    ssp_dciw_f = envy.flat_earth_c(ssp_dciw, verbose=True)
    ssp_f = envy.flat_earth_c(ssp, verbose=True)
    bathy_f = envy.flat_earth_bathy(bathy)

    # Bottom properties
    cb = xr.DataArray(
        np.ones((1,ssp_dciw_f.sizes['range'])),
        dims=['depth','range'],
        coords={
            'range':ssp_dciw_f.range,
            'depth':np.array([6000])}
    )

    rhob = xr.DataArray(
        np.ones((1,ssp_dciw_f.sizes['range'])),
        dims=['depth','range'],
        coords={
            'range':ssp_dciw_f.range,
            'depth':np.array([6000])}
    )

    attn = xr.DataArray(
        np.ones((1,ssp_dciw_f.sizes['range'])),
        dims=['depth','range'],
        coords = {
            'depth':np.array([6000]),
            'range':ssp_dciw_f.range
        }
    )

    cb.loc[:,:30] = bighorn.bottom_props['med_silt']['soundSpeed']
    cb.loc[:,30:] = bighorn.bottom_props['clay']['soundSpeed']
    if node in ['AXBA1', 'AXCC1', 'AXEC2', 'PC03A']:
        cb.loc[:,3555:] = bighorn.bottom_props['rock']['soundSpeed']

    rhob.loc[:,:30] = bighorn.bottom_props['med_silt']['density']
    rhob.loc[:,30:] = bighorn.bottom_props['clay']['density']
    if node in ['AXBA1', 'AXCC1', 'AXEC2', 'PC03A']:
        rhob.loc[:,3555:] = bighorn.bottom_props['rock']['density']

    attn.loc[:,:30] = bighorn.bottom_props['med_silt']['soundAttenuation']
    attn.loc[:,30:] = bighorn.bottom_props['clay']['soundAttenuation']
    if node in ['AXBA1', 'AXCC1', 'AXEC2', 'PC03A']:
        attn.loc[:,3555:] = bighorn.bottom_props['rock']['soundAttenuation']
                
    input_params = {
        'title':node,
        'freq':75,
        'zs':depths['KB']+10,
        'zr':0,
        'rmax':bathy_f.range[-1]*1000,
        'dr':10,
        'ndr':14,
        'zmax':6200,
        'dz':2,
        'ndz':5,
        'zmplt':6200,
        'c0':1500,
        'np':8,
        'ns':1,
        'rs':10000,
        'bathymetry':bathy_f,
        'soundspeed':ssp_dciw_f,
        'cb':cb,
        'rhob':rhob,
        'attn':attn,
    }

    env_dciw = envy.EnvironmentRAM(**input_params)

    print('running ram...')
    # run RAM
    gf_iw = bighorn.run_ram(env_dciw, Fs=300, T0 = 10, bw = (37.5, 112.5), zdec=1, rdec = -1)

    # save output
    gf_iw.real.to_netcdf(fnr)
    gf_iw.imag.to_netcdf(fni)

    print(f'{node} complete.')