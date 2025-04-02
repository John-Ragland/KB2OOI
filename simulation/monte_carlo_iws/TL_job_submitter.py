import fsspec
from dotenv import load_dotenv
import pathlib
import os
import subprocess
import time
from textwrap import dedent
import sys
# load .env file
current_file_path = pathlib.Path(__file__).resolve()
env_path = f'{current_file_path.parent.parent.parent}/.env'
load_dotenv(env_path)
log_path = f'{current_file_path.parent.parent.parent}/logs/tl/'


fs = fsspec.filesystem('')
fns = fs.glob(f'{os.environ["data_directory"]}iws/realizations/*.nc')

def submit_job(filepath):
    """Submit a TL calculation job to SLURM"""
    # Create a shortened log name from the filepath
    log_str = os.path.basename(filepath).replace('.nc', '')
    
    # Create SLURM script
    slurm_script = dedent(f"""
        #!/bin/bash
        #SBATCH --account=coenv
        #SBATCH --cpus-per-task=1
        #SBATCH --mem=50GB
        #SBATCH --partition=ckpt
        #SBATCH --time=36:00:00
        #SBATCH -J TL
        #SBATCH --output={log_path}{log_str}.out
        #SBATCH --error={log_path}{log_str}.err

        python {current_file_path.parent}/TL_iw_range.py LJ01C {filepath}
    """).strip()
    
    # Write temporary script file
    script_file = f"tmp_tl_submit_{time.time()}.sh"
    with open(script_file, 'w') as f:
        f.write(slurm_script)
    
    # Submit job
    subprocess.run(f"sbatch {script_file}", shell=True, check=True)
    
    # Clean up
    subprocess.run(f"rm {script_file}", shell=True, check=True)

# Create logs directory
subprocess.run("mkdir -p logs/tl", shell=True)

# Submit all jobs at once
print(f"Submitting {len(fns)} jobs...")
for i, fn in enumerate(fns):
    submit_job(fn)
    print(f"Submitted job {i+1}/{len(fns)}: {os.path.basename(fn)}")

print("All jobs submitted!")

