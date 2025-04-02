"""
get_100km_ocean_sections.py - get 100km average sections of the ocean
    along the tracts between KB and OOI Oregon Shelf hydrophone in
    order to compute internal wave perturbations. These results are used
    as the input to iwGM/scripts/KB_OOI_IW.m
"""

if __name__ == '__main__':
    from kaooi.coordinates import coords
    import xarray as xr
    import envy
    import gsw
    from tqdm import tqdm
    import numpy as np
    import pathlib
    import os
    from dotenv import load_dotenv

    # load .env file
    current_file_path = pathlib.Path(__file__).resolve()
    env_path = f'{current_file_path.parent.parent.parent}/.env'
    load_dotenv(env_path)

    hydrophone = 'LJ01D'

    T, S, C = envy.get_TSC_slice(coords['KB'], coords[hydrophone], 1000, fillna=True)
    tsc = xr.Dataset({'T':T, 'S':S, 'C':C})

    sections = []
    for k in range(int((C.range[-2] - 100)/50) + 1):
        
        tsc_sec = tsc.sel({'range':slice(50*k, 50*(k+1)+50)})
        mean_lat = tsc_sec.lat.mean()
        mean_range = tsc_sec.range.mean()
        tsc_sec = tsc_sec.mean('range').assign_coords({'lat':mean_lat})
        
        N2, pmid = gsw.Nsquared(tsc_sec.S, tsc_sec.T, tsc.depth, lat=tsc_sec.lat, )
        N2 = xr.DataArray(N2, dims=['depth'], coords={'depth':tsc_sec.depth[:-1]})
        pmid = xr.DataArray(pmid, dims=['depth'], coords={'depth':tsc_sec.depth[:-1]})
        section = tsc_sec.merge({'N2':N2, 'pmid':pmid})
        section = section.expand_dims({'range':[float(mean_range)]})#, create_index_for_new_dim=False).assign_coords({'range':float(mean_range)})

        sections.append(section)
    sections = xr.concat(sections, dim='range')

    sections_interp = sections.interp({'depth':np.arange(0,5000)}, method='linear')

    # check that directory exists
    file_dir = f'{os.environ['data_directory']}/iws/'
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)

    sections_interp.to_netcdf(f'{file_dir}/KB_2_{hydrophone}.nc')