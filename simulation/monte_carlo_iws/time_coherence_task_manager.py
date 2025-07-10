"""
distribute PE runs to different SLURM jobs / partitions
"""

import fsspec
import subprocess
import time
from typing import List, Dict
from textwrap import dedent
import os
import pathlib
from dotenv import load_dotenv



def get_slurm_jobs() -> Dict[str, int]:
    """Get count of running/pending jobs per partition with specific names"""
    jobs = {
        'cpu-g2': 0,
        'ckpt-g2': 0
    }
    
    try:
        # Get partition and job name
        cmd = "squeue -u $USER -h -o '%P %j'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        
        if result.stdout.strip():
            for line in result.stdout.strip().split('\n'):
                partition, jobname = line.split()
                if partition == 'cpu-g2' and jobname == 'PE':
                    jobs['cpu-g2'] += 1
                elif partition == 'ckpt-g2' and jobname == 'PEckpt':
                    jobs['ckpt-g2'] += 1
    except subprocess.CalledProcessError as e:
        print(f"Error running squeue command: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        
    return jobs

def submit_job(py_input: str, partition: str) -> bool:
    """
    Submit a single SLURM job
    
    Returns
    -------
    bool
        True if job submitted successfully, False otherwise
    """

    try:
        # Validate inputs
        if not py_input or not partition:
            raise ValueError("py_input and partition must not be empty")
            
        if partition not in ['cpu-g2', 'ckpt-g2']:
            raise ValueError(f"Invalid partition: {partition}")
        log_str = py_input.replace(' ', '').replace(os.environ['data_directory'],'').replace('/','')
        if partition == 'cpu-g2':
            slurm_script = dedent(f"""
                #!/bin/bash
                #SBATCH --account=coenv
                #SBATCH --cpus-per-task=20 #number of CPUs
                #SBATCH --mem=80GB #RAM
                #SBATCH --partition={partition}
                #SBATCH --time=36:00:00 #time limit 1 hour
                #SBATCH -J PE
                #SBATCH --output=logs/mc/pe_mc_{log_str}.out
                #SBATCH --error=logs/mc/pe_mc_{log_str}.err

                python simulation/monte_carlo_iws/run_PE_time_coherence.py {py_input}
            """).strip()
        elif partition == 'ckpt-g2':
            slurm_script = dedent(f"""
                #!/bin/bash
                #SBATCH --account=coenv
                #SBATCH --cpus-per-task=20 #number of CPUs
                #SBATCH --mem=80GB #RAM
                #SBATCH --partition={partition}
                #SBATCH --time=3:00:00 #time limit 8 hours
                #SBATCH -J PEckpt
                #SBATCH --output=logs/mc/pe_mc_{log_str}.out
                #SBATCH --error=logs/mc/pe_mc_{log_str}.err

                python simulation/monte_carlo_iws/run_PE_time_coherence.py {py_input}
            """).strip()
        
        # Write temporary script file
        script_file = f"tmp_submit_{time.time()}.sh"
        with open(script_file, 'w') as f:
            f.write(slurm_script)
        
        # Submit job
        subprocess.run(f"sbatch {script_file}", shell=True)
        
        # Clean up
        subprocess.run(f"rm {script_file}", shell=True)
        
        return True
        
    except Exception as e:
        print(f"Error submitting job: {e}")
        return False

def main(py_inputs: List[str]):
    # Create logs directory
    subprocess.run("mkdir -p logs", shell=True)
    
    # Track which inputs have been submitted
    submitted = set()
    
    while len(submitted) < len(py_inputs):
        current_jobs = get_slurm_jobs()
        
        # Check cpu-g2 partition (maintain 1 job)
        if current_jobs['cpu-g2'] < 0:
            for input_str in py_inputs:
                if input_str not in submitted:
                    submit_job(input_str, 'cpu-g2')
                    submitted.add(input_str)
                    break
        
        # Check ckpt-g2 partition (maintain 5 jobs)
        while current_jobs['ckpt-g2'] < 50:
            submitted_new = False
            for input_str in py_inputs:
                if input_str not in submitted:
                    submit_job(input_str, 'ckpt-g2')
                    submitted.add(input_str)
                    current_jobs['ckpt-g2'] += 1
                    submitted_new = True
                    break
            if not submitted_new:
                break
                
        time.sleep(30)  # Wait before next check

if __name__ == "__main__":


    # load .env file
    current_file_path = pathlib.Path(__file__).resolve()
    env_path = f'{current_file_path.parent.parent.parent}/.env'
    load_dotenv(env_path)

    fs = fsspec.filesystem('')
    file_base = f'{os.environ["data_directory"]}time_coherence_iws/realizations/'
    fns = fs.glob(f'{file_base}*.nc')

    nodes = ['AXCC1','AXEC2','AXBA1','HYS14','LJ01C','PC01A','PC03A', 'LJ01A', 'LJ01D']

    py_inputs = []
    for node in nodes:
        for fn in fns:
            py_inputs.append(f'{node} {fn}')

    # remove entries that already have the sim files (meaning they've already been run)
    py_inputs = [py_input for py_input in py_inputs 
             if not (os.path.exists(f'{os.environ["data_directory"]}time_coherence_iws/{py_input[:5]}_{int(py_input[6:][-6:-3]):02}_Gfz_real.nc')
                    and os.path.exists(f'{os.environ["data_directory"]}time_coherence_iws/{py_input[:5]}_{int(py_input[6:][-6:-3]):02}_Gfz_imag.nc'))]
            
    # Use the existing py_inputs from your code
    main(py_inputs)
