# author: YuanZhi Shang


from concurrent.futures import ThreadPoolExecutor
import datetime
from math import ceil
from src.config.config_handler import ConfigHandler
from src.handler.lang_calc_handler import LangCalcHandler
from src.util.grid_json_parser import GridJsonParser
from src.util.lang_tag_json_parser import LangTagJsonParser
from src.util.singleton_decorator import singleton
from src.util.twitter_json_parser import TwitterJsonParser
from mpi4py import MPI

comm = MPI.COMM_WORLD    

# A stop signal
STOP = (404,404)

# @singleton
class ThreadPoolHandler(object):
  
    def __init__(self, start_index=0, total_rows_per_process=100000, 
                    test_thread_step=500, test_mode=False):

        self._config_handler = ConfigHandler()
        self._lang_tag_parser = LangTagJsonParser()
        self._grid_parser = GridJsonParser()
        self._twitter_json_parser = TwitterJsonParser()

        if test_mode:
            self._main_queue = self._twitter_json_parser.test_queue_generator(total_rows_per_process)
            self._thread_step = test_thread_step
        else:
            starttime = datetime.datetime.now()
            # self._main_queue = self._twitter_json_parser.parse_valid_coordinate_lang_maps_in_range(start_index=start_index, step=total_rows_per_process)\
            self._main_queue = self._twitter_json_parser.parse_valid_coordinate_lang_maps_in_range_v2(start_index=start_index, step=total_rows_per_process)
            endtime = datetime.datetime.now()
            print("[INFO] Parsing Time cost: ", endtime - starttime, " rank: ", comm.Get_rank())
            self._thread_step = self._config_handler.get_step()

        upper_bound = self._config_handler.get_upper_bound_rows_per_iteration()
        if total_rows_per_process > upper_bound:
            self._total_rows_per_process = upper_bound
        else:
            self._total_rows_per_process = total_rows_per_process

        self._thread_nums = ceil(self._total_rows_per_process / self._thread_step)
        self._thread_nums = 2000 if self._thread_nums > 2000 else self._thread_nums
        self._thread_pool = ThreadPoolExecutor(self._thread_nums)
        self._job_num = self._thread_nums
        self._result = []

    def launch(self, task):
        func = self._get_func(task)
        args = (self._main_queue, self._thread_step, 
                    self._grid_parser, self._lang_tag_parser)
        for i in range(self._job_num):
            future = self._thread_pool.submit(func, args)
            future.add_done_callback(self._call_back)
        
        self._thread_pool.shutdown(wait=True)

    def collect_result(self):
        result = LangCalcHandler.table_union(self._result, self._grid_parser)
        return result

    def _get_func(self, task):
        if task not in dir(LangCalcHandler):
            raise AttributeError(task + " does not exist")
        func = getattr(LangCalcHandler, task)
        return func

    def _call_back(self,future):
        self._result.append(future.result())

    def info(self):
        return " \n ThreadNum: " + str(self._thread_nums) + " \n threadstep: " + str(self._thread_step)
    