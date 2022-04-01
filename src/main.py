# Here we will write main script with
from cmath import polar
import os
import random
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



# The world
comm = MPI.COMM_WORLD

# Total Processes
total_processes = comm.Get_size()

# Current Process rank
current_process_rank = comm.Get_rank()


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# demo for multi process calc running
# mpiexec -np 8 python /home/xiaotian/Desktop/projects/comp90024_ass_1/COMP90024_Assignment_1/src/main.py
def multi_process_calc():
    thread_demo = ResultGatherHandler(comm)
    if current_process_rank == 0:

        thread_demo.start()

        # fake calculation
        time.sleep(random.randint(1, 3))

        # busy waiting to wait for others
        while not thread_demo.is_finished_receiving():
            pass

        # now we received all res,
        res = thread_demo.get_received_data()

        # insert master's fake result
        res[0] = 0
        print("[debug] received data:", res)

    else:
        # fake calculation
        time.sleep(random.randint(1, 8))

        # send result
        TestSendHandler(comm).start()


# Press the green button in the gutter to run the script.

if __name__ == '__main__':
    # parser = TwitterJsonParser()
    # print(parser.get_total_rows())



    pool = ThreadPoolHandler(test_mode=True, test_queue_num=1000000)
    pool.launch('lang_calc')
    # print(pool.collect_result())
    # print(pool.info())

    final_table = pool.collect_result()


    sum_record = 0
    # simple visualise
    for key in final_table.keys():
        record = final_table[key]
        lang_vs_num = record[2]
        lang_types_num = record[1]
        total_tw = record[0]

        sum_record += total_tw
        assert (len(record[2]) == lang_types_num)
        sum = 0
        for item in lang_vs_num:
            sum += item[1]
        assert (sum == total_tw)
        print(key, ": ", final_table[key])
    print(" Toal records: ", sum_record)
    print(pool.info())