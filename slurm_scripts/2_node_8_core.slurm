#!/bin/bash
#SBATCH --job-name="ass1_2_node_8_core"
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --output=R-%x.%j.out

module purge

module load foss/2020b
module load python/3.8.6
pip3 install ijson --user
pip3 install mpi4py --user
pip3 install PyYAML --user

mpiexec python3 ../src/main.py
