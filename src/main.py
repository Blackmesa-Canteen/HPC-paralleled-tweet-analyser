# Here we will write main script with
from cmath import polar
from dataclasses import dataclass
import os
import random
import re
import sys

# make single script runnable!!!
import time
from decimal import Decimal

import datetime
from math import ceil
from tkinter import N



# make single script runnable!!!
sys.path.append(os.path.dirname(sys.path[0]))
print('[debug] running root path:', os.path.dirname(sys.path[0]))

from mpi4py import MPI

from src.config.config_handler import ConfigHandler
from src.handler.result_gather_handler import ResultGatherHandler
from src.handler.test_send_handler import TestSendHandler
from src.util.grid_json_parser import GridJsonParser
from src.util.lang_tag_json_parser import LangTagJsonParser
from src.util.twitter_json_parser import TwitterJsonParser
from src.handler.thread_pool_handler import ThreadPoolHandler
from src.handler.lang_calc_handler import LangCalcHandler



if __name__ == '__main__':

    comm = MPI.COMM_WORLD                                                      
    rank = comm.Get_rank()                                                     
    size = comm.Get_size()                                                     

    if rank == 0:
        send_data = (0, 99999, 199999, 299999, 399999)
        
    else:                                                                      
        send_data = None

    # Per-process except rank 0
    recv_data = comm.scatter(send_data, root=0)                                       
    pool = ThreadPoolHandler(recv_data, test_mode=True, test_queue_num=100000, process_step=100000)
    pool.launch('lang_calc')   
    result = pool.collect_result()    
    send_data = result
    recv_data = comm.gather(send_data, root=0)

    if rank == 0:
        print(len(recv_data))
        grid_parser = GridJsonParser()
        table = LangCalcHandler.table_union(recv_data, grid_parser)
        LangCalcHandler.view(table)