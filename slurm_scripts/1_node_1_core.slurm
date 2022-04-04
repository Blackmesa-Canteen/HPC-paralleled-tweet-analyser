#!/bin/bash
#SBATCH --job-name="ass1_1_node_1_core"
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --output=R-%x.%j.out
#SBATCH --error=R-%x.%j.err

module purge

module load foss/2020b
module load python/3.8.6


pip3 install ijson --user
pip3 install mpi4py --user
pip3 install PyYAML --user

mpiexec python3 ../src/main.py
