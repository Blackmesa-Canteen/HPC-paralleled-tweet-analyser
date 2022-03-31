# Here we will write main script with

import os
import random
import re

import sys

# make single script runnable!!!
import time


# from pip import main



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
from src.util.utils import Utils


GAP = 0.15

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

    # init parser
    config_handler = ConfigHandler()
    lang_tag_parser = LangTagJsonParser()
    grid_parser = GridJsonParser()
    twitter_json_parser = TwitterJsonParser()

    # init data
    twitter_json_parser.parse_valid_coordinate_lang_maps_in_range(start_index=0, step=500000000)
    # records num that each thread would process
    step = config_handler.get_step()
    # Main queue for one process
    main_records_queue = twitter_json_parser.get_twitter_queue()
    # how many records for one process
    total_row = config_handler.get_upper_bound_rows_per_iteration()
    # threads number in thread pool
    thread_nums = int( total_row / step)

    # # how many job in the threadpool job queue
    # job_nums = thread_nums

    # # run threadpool
    # pool = ThreadPoolHandler(thread_nums, job_nums)
    # args = (main_records_queue, step)
    # pool.start('lang_calc', args)
    # pool.stop()

    # # collect result and view
    # table_list = pool.collect_result()
    # table_sum = LangCalcHandler.table_union(table_list, grid_parser)
    # Utils.visualise(table_sum)



    '''
    以下程序用来测试
    '''
    
    thread_nums = 1000   # 线程池最大线程，提交的任务大于此数量则不会生成新的线程
    job_nums = 10        # 线程池任务数，代表你想用几个线程去完成这个工作，若任务数为2，则生成两个线程，每个线程的消费数量为total_row / job_num
    max_job = 99999      # 线程池最大任务数，默认无限大，无关紧要

    # 进程池实例
    pool = ThreadPoolHandler(thread_nums, job_nums, max_job)   
    # 生成测试数据
    total_row = 10000
    q = Utils.sample_generator(total_row)

    # packing thread function argument
    args = (q, total_row / job_nums, grid_parser, lang_tag_parser)
    pool.run_task('lang_calc', args)
    pool.stop()
    
    # Collect result from multiple threads
    table_list = pool.collect_result()
    final_table = Utils.table_union(table_list, grid_parser)


    # simple visualise
    for key in final_table.keys():
        print(key, ": ", final_table[key])