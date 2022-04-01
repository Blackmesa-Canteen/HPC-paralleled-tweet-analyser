#!/bin/bash
#SBATCH --job-name="ass1_1_node_8_core"
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8

module purge

module load foss/2020b

pip3 install ijson --user
pip3 install mpi4py --user
pip3 install PyYAML --user

mpiexec -np 16 python3 ../src/main.py
