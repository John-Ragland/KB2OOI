import os
import fsspec
from multiprocessing import Pool
from functools import partial
from tqdm import tqdm
from dotenv import load_dotenv
import pathlib
import numpy as np
import xarray as xr
from tqdm import tqdm
import h5py

def open_iw_mat(fn, start_idx=0):
    '''
    open_iw_mat - open a .mat file output from iwGM

    Parameters
    ----------
    fn : str
        path to the .mat file output from iwGM
    start_idx : int
        index within mat file to start reading

    Returns
    -------
    dciws : list
        list of xr.DataArray objects containing iw perturbation realizations
    '''

    fo = h5py.File(fn)

    dciws = []
    print('loading sections into memory...')
    for k in tqdm(range(start_idx, fo['sectioniw']['dciw'].shape[0])):
        
        dciw = fo[fo['sectioniw']['dciw'][k][0]][:]
        xiw = fo[fo['sectioniw']['xiw'][k][0]][:]
        ziw = fo[fo['sectioniw']['ziw'][k][0]][:]
        
        dciws.append(
            xr.DataArray(
                dciw,
                dims=['range', 'depth'],
                coords={'depth':ziw.flatten(), 'range':xiw.flatten()/1000}
            )
        )
      
    return dciws

def merge_iw(fn : str, start_idx : int = 0, verbose : bool=True):
    '''
    merge_iw - take output of iwGM, which is saved as a .mat file, and merge them into a single 
    changing latitude ssp perturbation realization

    Parameters
    ----------
    fn : str
        path to the .mat file output from iwGM
    start_idx : int
        index within mat file to start reading. Default is 0
    verbose : bool
        whether to print out progress

    Returns
    -------
    merged_iw : xr.DataArray
        merged iw perturbation realization
    '''

    dciws = open_iw_mat(fn, start_idx)

    dr = float(dciws[0].range[1] - dciws[0].range[0])

    def get_factors(x):
        factors = []
        for i in range(1, int(x + 1)):
            if x % i == 0:
                factors.append(i)
        return factors

    factors = np.array(get_factors(50e3))

    # get dr that is integer divisible by (100/2)km
    # largest integer factor of 100km that is larger that dr
    dr_bin = int(np.max((factors - (dr*1000))[(factors - (dr*1000)) < 0]) + dr*1000)/1000

    ro_idx = np.arange(0, len(dciws))

    ro = ro_idx*50

    rs = []
    for k in range(len(ro)):
        rs.append(np.arange(ro[k], ro[k] + 100, dr_bin))

        dciws_interp = []

    # interpolate and reassign coordinates to sections
    print('interpolating and reassigning coordinates...')
    for k, dciw in enumerate(tqdm(dciws)):
        # interpolate to change in range of dr_bin
        dciws_interp.append(dciw.interp({'range':np.arange(0,100,dr_bin)}))

        # make sure range is first dimension
        dciws_interp[k] = dciws_interp[k].transpose('range', 'depth')
        # add cosine taper
        cos_tap = np.expand_dims(np.cos(np.linspace(-np.pi/2, np.pi/2, dciws_interp[k].sizes['range']))**2, 1)
        if k == 0:
            cos_tap[:int(dciws_interp[k].sizes['range']/2)] = 1
        elif k == (len(dciws)-1):
            cos_tap[int(dciws_interp[k].sizes['range']/2):] = 1
        dciws_interp[k] = dciws_interp[k] * cos_tap
        # reassign range relative coordinates
        dciws_interp[k] = dciws_interp[k].assign_coords({'range':np.arange(0,100,dr_bin) + 50*k})

    # merge sections
    print('merging sections...')
    dciws_mixed = dciws_interp[0]
    for k in tqdm(range(1,len(dciws_interp))):
        #dciws_mixed = dciws_interp[k] + dciws_mixed

        a,b = xr.align(dciws_mixed, dciws_interp[k], fill_value=0, join='outer')
        dciws_mixed = a+b

    return dciws_mixed

def process_file(fn):
    """Process a single file and save the result"""
    try:
        file_base = fn[-12:-4]
        print(f'merging dciw realization {file_base}...')
        
        fno = f'{os.environ['data_directory']}iws/realizations/{file_base}.nc'
        
        # Skip if file exists
        if os.path.exists(fno):
            print(f'file already exists for {file_base}, skipping...')
            return None
            
        dciw = merge_iw(fn)
        dciw.to_netcdf(fno)
        return file_base
        
    except Exception as e:
        print(f"Error processing {fn}: {str(e)}")
        return None

def main():
    fs = fsspec.filesystem('')
    fns = fs.glob(f'{os.environ['data_directory']}iws/realizations/sections/*.mat')
    
    # Use number of CPUs for parallel processing
    with Pool(processes=5) as pool:
        results = list(tqdm(pool.imap(process_file, fns), total=len(fns)))
    
    # Filter out None results and print summary
    completed = [r for r in results if r is not None]
    print(f"Processed {len(completed)} files successfully")

if __name__ == '__main__':

    # load .env file
    current_file_path = pathlib.Path(__file__).resolve()
    env_path = f'{current_file_path.parent.parent.parent}/.env'
    load_dotenv(env_path)

    main()
