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
from tkinter.messagebox import NO
from tracemalloc import start

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
    '''
    TODO: 命令行解析进程数
    '''
    PROCESS_NUM = 3

    if rank == 0:

        twitter_json_parser =  TwitterJsonParser()
        total_rows = twitter_json_parser.get_total_rows()
        send_data = get_interval(total_rows, PROCESS_NUM)
        print("[Rank: {0}] Total rows: {1}".format(rank, total_rows))

    else:                                                                      
        send_data = None

    # Per-process except rank 0
    recv_data = comm.scatter(send_data, root=0)

    start_index = recv_data[0]
    total_rows_per_process = recv_data[1]


    start_index = 0
    total_rows_per_process = 12345
    print("process {} recv data {}...".format(rank, recv_data))


    pool = ThreadPoolHandler(start_index=start_index, total_rows_per_process=total_rows_per_process, 
                                    test_thread_step=500, test_mode=True)

    pool.launch('lang_calc')   
    result = pool.collect_result()    
    send_data = result
    recv_data = comm.gather(send_data, root=0)

    if rank == 0:

        grid_parser = GridJsonParser()
        table = LangCalcHandler.table_union(recv_data, grid_parser) 

        for key in table.keys():
                table[key][1] = len(table[key][1])
                # add [:10] to get top 10
                table[key][2] = list(sorted(table[key][2].items(), key=lambda x: x[1], reverse=True))
        LangCalcHandler.view(table)


