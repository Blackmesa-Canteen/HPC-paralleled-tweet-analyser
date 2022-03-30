# Here we will write main script with

import os
import random
from reprlib import aRepr
from socket import timeout
import sys

# make single script runnable!!!
import time
from turtle import xcor
from unicodedata import decimal


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

    # print_hi('PyCharm')
    # config_handler = ConfigHandler()
    # print(config_handler.get_grid_path(), " ", config_handler.get_grid_columns())
    # print('upper-bound-rows', config_handler.get_upper_bound_rows_per_iteration())

    # lang_tag_parser = LangTagJsonParser()
    # print(lang_tag_parser.get_tag_lang_map())

    # grid_parser = GridJsonParser()
    # print(grid_parser.get_all_grids())
    # print(grid_parser.get_grid_by_name('A1'))
    # print(grid_parser.get_grid_by_name('B2'))
    # print(grid_parser.get_grid_by_name('B3'))
    # print(grid_parser.get_grid_by_name('B4'))

    # print('----------------------------------------')

    # print(grid_parser.get_grid_by_name('A2'))
    # print(grid_parser.get_grid_by_name('B2'))
    # print(grid_parser.get_grid_by_name('C2'))
    # print(grid_parser.get_grid_by_name('D2'))

    # twitter_json_parser = TwitterJsonParser()
    # print(twitter_json_parser.get_total_rows())

    # res_queue = twitter_json_parser.parse_valid_coordinate_lang_maps_in_range(start_index=0, step=500000000)

    # while not res_queue.empty():
    #     print(res_queue.get())



    # init per-process 
    config_handler = ConfigHandler()
    lang_tag_parser = LangTagJsonParser()
    grid_parser = GridJsonParser()
    twitter_json_parser = TwitterJsonParser()


    twitter_json_parser.parse_valid_coordinate_lang_maps_in_range(start_index=0, step=500000000)
    step = config_handler.get_step()
    main_queue = twitter_json_parser.get_twitter_queue()
    print(main_queue.empty())
    total_row = config_handler.get_upper_bound_rows_per_iteration()
    thread_nums = int( total_row / step)

    grid_info = grid_parser.get_all_grids()
    job_nums = thread_nums

    # define thread task (func, args=(queue, step))
    # 执行线程的主要逻辑
    
    init_table = {}
    # print(grid_info)

    for key in grid_info.keys():
        init_table[key] = [None]*3
        init_table[key][2] = {}


    '''
    测试init table
    '''

    print(grid_info)

    lang_calc = LangCalcHandler(1, init_table)
    # init_table['A1'][0] = 1000
    # init_table['A1'][2]['English'] = 1

    # init_table['B1'][0] = 2000
    # print(init_table)

    # print(len(init_table.keys()))

    from decimal import Decimal
    def which_grid(grid_info, pos):

        GAP = Decimal('0.15')
        achor1 = grid_info['A1'][0]
        achor2 = grid_info['A1'][1]
        x_0, y_0 = achor1
        x_1, y_1 = achor2

        x, y = pos
        x_step = -GAP
        y_step = GAP

        x_count = 0
        y_count = 0

        while True:
            if x <= x_1 and x >= x_0:
                break
            x += x_step
            x_count += 1

        while True:
            if y > y_1 and y <= y_0:
                break
            y += y_step
            y_count += 1
        
        index = (x_count) * 4 + y_count

        for key in grid_info.keys():
            if index == 0:
                print(str(key))
                return str(key)
            index -= 1

        return 'OUT OF RANGE'
    


    
    def lang_calc(thread_id, args):

        print("[INFO] Thread ", thread_id, " start job {0}".format(args))
        main_queue, step = args
        lang_calc_handler = LangCalcHandler(thread_id)

        print(main_queue.empty())
        while step:
            if main_queue.empty():
                break
            else:
                message = main_queue.get()
                lang_calc_handler.handle(1)

        return lang_calc_handler.result()


    # Scheduler
    # pool = ThreadPoolHandler(2)

    # print(thread_nums, step, total_row)

    # for i in range(0, 2):
    #     pool.submit(lang_calc, (main_queue, step))

    # result = pool.collect_result()
    # pool.stop()
    # # time.sleep(2)
    # # print(pool)
    # # collect结果和queue的超时时间需要精确把控
    # print("Result: ", result)
    

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
