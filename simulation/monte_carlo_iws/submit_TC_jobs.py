import fsspec
from dotenv import load_dotenv, find_dotenv
import os
from textwrap import dedent
import time
import subprocess

if __name__ == '__main__':
    # load .env file
    load_dotenv(find_dotenv())

    fs = fsspec.filesystem('')
    dciw_filebase = f'{os.environ["data_directory"]}iws/time/'

    dciw_fns = fs.glob(f'{dciw_filebase}*.nc')

    for k in range(len(dciw_fns)):
        slurm_script = dedent(
            f"""
            #!/bin/bash
            #SBATCH --account=coenv
            #SBATCH --cpus-per-task=30 #number of CPUs
            #SBATCH --mem=80GB #RAM
            #SBATCH --partition=ckpt
            #SBATCH --time=3:00:00 #time limit 8 hours
            #SBATCH -J PEckpt
            #SBATCH --output=logs/mc/pe_tc_{k:02}.out
            #SBATCH --error=logs/mc/pe_tc_{k:02}.err

            python simulation/monte_carlo_iws/run_PE_time_coherence.py LJ01C {dciw_fns[k]}
            """).strip()


        # Write temporary script file
        script_file = f"tmp_submit_{time.time()}.sh"
        with open(script_file, 'w') as f:
            f.write(slurm_script)
        
        # Submit job
        subprocess.run(f"sbatch {script_file}", shell=True)
        
        # Clean up
        subprocess.run(f"rm {script_file}", shell=True)
        
        print('submitted job for tc idx', k)
