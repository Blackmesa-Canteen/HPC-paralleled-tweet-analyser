# Here we will write main script with
from email import message
import os
import random
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



    step = config_handler.get_step()
    main_queue = twitter_json_parser.get_twitter_queue()

    total_row = config_handler.get_upper_bound_rows_per_iteration()

    thread_nums = int( total_row / step)


    job_nums = thread_nums

    # define thread task (func, args=(queue, step))
    def lang_calc(thread_id, args):

        main_queue, step = args

        lang_calc_handler = LangCalcHandler(thread_id)

        # while step:
        #     message = main_queue.get()
        #     print("[INFO] message: ", message)
        #     lang_calc_handler.handle(message)
        #     step -= 1
        for i in range(2):
            lang_calc_handler.handle(thread_id)

        print("[INFO] Thread ", thread_id, " finish job {0}".format(args))
        
        return lang_calc_handler.result()


    # Scheduler
    pool = ThreadPoolHandler(thread_nums)

    print(thread_nums, step, total_row)

    for i in range(0, 100):
        pool.submit(lang_calc, (main_queue, step))

    

    result = pool.collect_result()

    pool.stop()
    time.sleep(2)


    print(pool)
    print(result)
    

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
