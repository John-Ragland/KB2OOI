# create mixed iw perturbation
import os
import envy
from kaooi.coordinates import coords, depths
import numpy as np
import xarray as xr
import bighorn
from dotenv import load_dotenv
import pathlib

hydrophones = [
    "AXCC1",
    "AXEC2",
    "AXBA1",
    "LJ01A",
    "LJ01C",
    "HYS14",
]

# check __main__
if __name__ == '__main__':
    
    # load .env file
    current_file_path = pathlib.Path(__file__).resolve()
    env_path = f'{current_file_path.parent.parent.parent}/.env'
    load_dotenv(env_path)
    
    # check that directory exists
    file_dir = f'{os.environ['data_directory']}/timefront/'
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)

    # check that env_files directory exists
    if not os.path.exists(f'{file_dir}env_files/'):
        os.makedirs(f'{file_dir}env_files/')
    
    for node in hydrophones:
        print(f'running PE for {node}...')

        fnr_iw = f'{file_dir}climate_{node}_Gfz_real.nc'
        fni_iw = f'{file_dir}climate_{node}_Gfz_imag.nc'
        
        fnr_cl = f'{file_dir}iw_climate_{node}_Gfz_real.nc'
        fni_cl = f'{file_dir}iw_climate_{node}_Gfz_imag.nc'

        # check if simulation has already been run:
        if os.path.exists(fnr_iw) and os.path.exists(fnr_iw) and os.path.exists(fni_cl) and os.path.exists(fni_cl):
            print(f'simulation files already exists for {node}, skipping...')
            continue
        
        ssp = envy.get_ssp_slice(
            coords['KB'],
            coords[node],
            num_range_points=5000,
            fillna=True,
            climate=True,
        ).load()

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
        ssp_dciw_f = envy.flat_earth_c(ssp_dciw, verbose=True, n_cpus=90, chunk_size = 360)
        ssp_f = envy.flat_earth_c(ssp, verbose=True)
        bathy_f = envy.flat_earth_bathy(bathy)

        # Bottom properties
        cb = xr.DataArray(
            np.ones((1,ssp_dciw_f.sizes['range'])),
            dims=['depth','range'],
            coords={
                'range':ssp_dciw.range,
                'depth':np.array([6000])}
        )

        rhob = xr.DataArray(
            np.ones((1,ssp_dciw_f.sizes['range'])),
            dims=['depth','range'],
            coords={
                'range':ssp_dciw.range,
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
        # use clay for ocean basin, med_silt near KB and rock for Axial Seamount
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

        # add cascadia slope model for continental slope nodes
        if node in ['HYS14','LJ01C']:
            cb.loc[:,3000:] = bighorn.bottom_props['cascadia-slope']['soundSpeed']
            rhob.loc[:,3000:] = bighorn.bottom_props['cascadia-slope']['density']
            attn.loc[:,3000:] = bighorn.bottom_props['cascadia-slope']['soundAttenuation']

        input_params_dciw = {
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

        # save environment file info for climate (no iw perturbations)
        with open(f"{file_dir}env_files/climate_{node}_env.txt", 'w') as f:
            f.write(env.__repr__())
        # save environment file info for iw perturbations
        with open(f"{file_dir}env_files/iw_climate_{node}_env.txt", 'w') as f:
            f.write(env_dciw.__repr__())
        
        ## saving output in earth flattened depth coordinates
        # run RAM for climate profile
        gf_cl = bighorn.run_ram(env, Fs=300, T0 = 10, bw = (37.5, 112.5), zdec=1, rdec = -1)

        # save output
        gf_cl.real.to_netcdf(fnr_cl)
        gf_cl.imag.to_netcdf(fni_cl)

        # run RAM for iw profile
        gf_iw = bighorn.run_ram(env_dciw, Fs=300, T0 = 10, bw = (37.5, 112.5), zdec=1, rdec = -1)

        # save output
        gf_iw.real.to_netcdf(fnr_iw)
        gf_iw.imag.to_netcdf(fni_iw)

        # inverse flat-earth transform depth coordinates
        #depths_climate_ife,_ = envy.eflatinv(gf_cl.depth.values, bathy.lat[-1].values)
        #depths_iw_ife,_ = envy.eflatinv(gf_iw.depth.values, bathy.lat[-1].values)
        #gf_cl_ife = gf_cl.assign_coords({'depth':depths_climate_ife})
        #gf_iw_ife = gf_iw.assign_coords({'depth':depths_iw_ife})



        print(f'{node} complete.')