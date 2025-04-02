# create mixed iw perturbation
import os
import envy
from kaooi.coordinates import coords, depths
import numpy as np
import xarray as xr
import bighorn
import pathlib
from dotenv import load_dotenv
import sys
import argparse  # Add import for argument parsing

if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Run monthly arrivals simulation')
    parser.add_argument('--node', type=str, required=True, help='Node name (e.g., AXBA1, AXCC1)')
    parser.add_argument('--month', type=int, required=True, choices=range(1, 13), help='Month (1-12)')
    args = parser.parse_args()
    
    # Get command line arguments
    node = args.node
    month = args.month
    
    # load .env file
    current_file_path = pathlib.Path(__file__).resolve()
    env_path = f'{current_file_path.parent.parent.parent}/.env'
    load_dotenv(env_path)

    file_dir = f'{os.environ["data_directory"]}monthly_arrivals/'
    env_file_dir = f'{file_dir}env_files/'

    # check that file_dir and env_file_dir exist, if not create
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    if not os.path.exists(env_file_dir):
        os.makedirs(env_file_dir)

    # Validate node exists in coords dictionary
    if node not in coords:
        print(f"Error: Node '{node}' not found in coordinates dictionary")
        sys.exit(1)

    print(f'running month {month}...')
    fnr_cl = f'{file_dir}climate_Gtz_{node}_{month:02}_real.nc'
    fni_cl = f'{file_dir}climate_Gtz_{node}_{month:02}_imag.nc'
    fnr_iw = f'{file_dir}iw_climate_Gtz_{node}_{month:02}_iw_real.nc'
    fni_iw = f'{file_dir}iw_climate_Gtz_{node}_{month:02}_iw_imag.nc'

    # check if simulation has already been run:
    if os.path.exists(fnr_cl) and os.path.exists(fnr_iw) and os.path.exists(fni_cl) and os.path.exists(fni_iw):
        print(f'simulation files already exists for {node}, skipping...')
        sys.exit()

    ssp = envy.get_ssp_slice(
        coords['KB'],
        coords[node],
        num_range_points=5000,
        fillna=True,
        climate=False,
    ).load()

    # interpolate to 2m depth resolution
    ssp = ssp.interp({'depth':np.hstack((np.arange(0,6000,2), 10000))})

    # choose specific month
    ssp = ssp.isel({'time':month-1})

    bathy = envy.get_bathymetry_slice(
        coords['KB'],
        coords[node],
        num_range_points=5000,
    ).load()

    dciw_fn = f'{os.environ['data_directory']}/iws/realizations/dciw_001.nc'
    dciw = xr.open_dataarray(dciw_fn)

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

    rhob.loc[:,:30] = bighorn.bottom_props['med_silt']['density']
    rhob.loc[:,30:] = bighorn.bottom_props['clay']['density']

    attn.loc[:,:30] = bighorn.bottom_props['med_silt']['soundAttenuation']
    attn.loc[:,30:] = bighorn.bottom_props['clay']['soundAttenuation']

    # add axial seamount properties for axial nodes
    if node in ['AXBA1', 'AXCC1', 'AXEC2', 'PC03A']:
        cb.loc[:,3555:] = bighorn.bottom_props['rock']['soundSpeed']
        rhob.loc[:,3555:] = bighorn.bottom_props['rock']['density']
        attn.loc[:,3555:] = bighorn.bottom_props['rock']['soundAttenuation']

    # add cascadia slope model for continental shelf nodes
    if node in ['HYS14','LJ01C']:
        cb.loc[:,3000:] = bighorn.bottom_props['cascadia-slope']['soundSpeed']
        rhob.loc[:,3000:] = bighorn.bottom_props['cascadia-slope']['density']
        attn.loc[:,3000:] = bighorn.bottom_props['cascadia-slope']['soundAttenuation']
        
    input_params_dciw = {
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

    input_params= {
        'title':node,
        'freq':75,
        'zs':depths['KB']+10,
        'zr':0,
        'rmax':bathy.range[-1]*1000,
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
        'soundspeed':ssp_f,
        'cb':cb.interp({'range':ssp_f.range}, kwargs = {'bounds_error':False, 'fill_value':'extrapolate'}),
        'rhob':rhob.interp({'range':ssp_f.range}, kwargs = {'bounds_error':False, 'fill_value':'extrapolate'}),
        'attn':attn.interp({'range':ssp_f.range}, kwargs = {'bounds_error':False, 'fill_value':'extrapolate'})
    }

    env = envy.EnvironmentRAM(**input_params)
    env_dciw = envy.EnvironmentRAM(**input_params_dciw)

    # run RAM
    gf_cl = bighorn.run_ram(env, Fs=300, T0 = 10, bw = (37.5, 112.5), zdec=1, rdec = -1)
    # save output
    gf_cl.real.to_netcdf(fnr_cl)
    gf_cl.imag.to_netcdf(fni_cl)

    gf_iw = bighorn.run_ram(env_dciw, Fs=300, T0 = 10, bw = (37.5, 112.5), zdec=1, rdec = -1)

    # save output
    gf_iw.real.to_netcdf(fnr_iw)
    gf_iw.imag.to_netcdf(fni_iw)

    # inverse flat-earth transform depth coordinates
    # depths_climate_ife,_ = envy.eflatinv(gf_cl.depth.values, bathy.lat[-1].values)
    # depths_iw_ife,_ = envy.eflatinv(gf_iw.depth.values, bathy.lat[-1].values)

    #gf_cl_ife = gf_cl.assign_coords({'depth':depths_climate_ife})
    #gf_iw_ife = gf_iw.assign_coords({'depth':depths_iw_ife})




