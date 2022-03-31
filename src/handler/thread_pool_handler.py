# # TODO 线程池管理，可能能用上

# from tkinter import N
from ctypes import util
from email import utils
from shutil import get_archive_formats
from src.util.singleton_decorator import singleton
# from src.config.twitter_type_enum import ThreadPoolSign
import queue
import threading
import time


# A stop signal
STOP = (404,404)
# Test job function
'''
以下函数用来测试并发程度，可忽略
'''
def func(thread_name, jobid):
    for i in range(0,3):
        print("[INFO] Thread: ", thread_name, " is doing job {0}".format(jobid))
        # time.sleep(0.1)
    print("[INFO] Thread ", thread_name, " finish job {0}".format(jobid))
    return jobid

global_queue = queue.Queue()
global_list = []

def func2(thread_name, params):
    for i in range(0,3):
        print("[INFO] Thread: ", thread_name, " is doing job {0}".format(params))
        # time.sleep(1)
        # global_queue.put(thread_name)
        global_list.append(thread_name)
    
    print("[INFO] Thread ", thread_name, " finish job {0}".format(params))
    # print("LIST: ", global_list)
    return thread_name

# @singleton
class ThreadPoolHandler(object):
  
    def __init__(self, thread_num, job_num, max_jobs=1000):
        # object.__init__(self)
        # Maybe useful
        self._max_threads = thread_num
        # self._max_jobs = max_jobs
        self._cancel_flag = False

        # Main data structure 
        self._queue = queue.Queue(max_jobs)
        self._running_threads = []
        self._free_threads = []

        # list append thread safe
        self._collect = []

        # TODO
        self._job_num = 100

    def submit(self, func, args):
        # How jobs are defined
        load = (func, args)
        if self._cancel_flag == True:
            print("[INFO] ThreadPool is down")
            return
        # Start new thread
        if len(self._running_threads) < self._max_threads and len(self._free_threads) == 0:
            thread = threading.Thread(target=self.run)
            thread.start()
        # Or use exist thread
        self._queue.put(load)
 
    # All threads would call this
    def run(self):
        thread_id = threading.currentThread().getName()
        # Add to busy list
        self._running_threads.append(thread_id)
        task = self._queue.get()
        # Work until handler say stop
        while task != STOP:
            func, params = task
            print("[INFO] Thread: {0} get a job: {1}".format(thread_id, params))

            try:
                result = func(thread_id, params)
                print("Result here: ", result)
                self._collect.append(result)
            except Exception as e:
                print("[WARNING]Thread: {0} throw {1}".format(thread_id, e))
            # Finish one job (wether success or not)
            self._free_threads.append(thread_id)
            # All threads will block here if no job
            task = self._queue.get()
            print("[INFO] Queue: get a job: {0}, thread: {1} run again".format(params, thread_id))
            self._free_threads.remove(thread_id)
        
        self._running_threads.remove(thread_id)
        print("[INFO] Current thread: {0} is done".format(thread_id))

    # gently stop thread pool
    def stop(self):
        self._cancel_flag = True
        for i in range(0, len(self._running_threads)):
            self._queue.put(STOP)

        while not (len(self._running_threads) == 0 and len(self._free_threads) == 0):
            pass 

    def check_free_thread(self):
        return self._free_threads

    def check_running_thread(self):
        return self._running_threads

    def check_state(self):
        return "RUNNING" if self._cancel_flag == False else "DOWN"

    def collect_result(self):
        return self._collect

    def __str__(self):        
        return "[INFO]\nCurrent running thread: " + str(self._running_threads) + " \n" + "Current free thread: " + str(self._free_threads) + " \n" + "Current status: " + self.check_state()

    # 线程池启动逻辑
    def start(self, task, args):
        func = self._get_func(task)     # TODO 
        for i in range(self._job_num):
            self.submit(func, args)
    
    def _get_func(self, task):
        if task not in dir(utils):
            raise AttributeError(task + " does not exist")
        func = getattr(utils, task)
        return func

# if __name__ == '__main__':
#     pool = ThreadPoolHandler(20)
#     for i in range(100):
#         pool.submit(func2, i)
#     time.sleep(4)
#     pool.stop()
#     # print(pool.collect())
#     # print(len(pool.collect()))
#     # print(global_list)



