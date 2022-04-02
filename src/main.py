# Here we will write main script with
from cmath import polar
from curses import noecho
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
from src.util.math_util import *



if __name__ == '__main__':

    comm = MPI.COMM_WORLD                                                      
    rank = comm.Get_rank()                                                     
    size = comm.Get_size()      

    PROCESS_NUM = size
    '''
    Master process node 0 packs and sends the total row number to each process
    '''
    if rank == 0:
        starttime = datetime.datetime.now()
        twitter_json_parser =  TwitterJsonParser()
        total_rows = twitter_json_parser.get_total_rows()
        send_data = get_interval(total_rows, size)
    else:                                                                      
        send_data = None

    '''
    Worker process read lines from specific start index and perform calculatation 

    '''
    recv_data = comm.scatter(send_data, root=0)
    start_index = recv_data[0]
    total_rows_per_process = recv_data[1]
    print("[INFO] Start index: ", start_index, "Step: ", total_rows_per_process, " Rank: ", rank)
    pool = ThreadPoolHandler(start_index=start_index, total_rows_per_process=total_rows_per_process, 
                                    test_thread_step=500, test_mode=False)
    pool.launch('lang_calc')   
    result = pool.collect_result()    
    send_data = result
    recv_data = comm.gather(send_data, root=0)
    '''
    Master process rececive the result from each process
    '''
    if rank == 0:

        grid_parser = GridJsonParser()
        table = LangCalcHandler.table_union(recv_data, grid_parser) 
        for key in table.keys():
                table[key][1] = len(table[key][1])
                # add [:10] to get top 10
                table[key][2] = list(sorted(table[key][2].items(), key=lambda x: x[1], reverse=True))
        LangCalcHandler.view(table)
        endtime = datetime.datetime.now()

        print(" Time Total: ", endtime - starttime)





