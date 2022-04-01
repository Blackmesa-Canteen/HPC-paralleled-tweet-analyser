# author: YuanZhi Shang


from src.util.singleton_decorator import singleton
from src.util.utils import Utils

import logging
import queue
import threading
import time


# A stop signal
STOP = (404,404)

# @singleton
class ThreadPoolHandler(object):
  
    def __init__(self, thread_num, max_threads=1000):
        # object.__init__(self)
        # Maybe useful
        self._max_threads = max_threads
        # self._max_jobs = max_jobs
        self._thread_num = thread_num

        self._cancel_flag = False

        # Main data structure 
        self._queue = queue.Queue()
        # self._queue = queue.Queue(max_jobs)
        self._running_threads = []
        self._free_threads = []

        # list append thread safe
        self._collect = []



    def submit(self, func, args):
        # How jobs are defined
        task = (func, args)
        if self._cancel_flag == True:
            print("[WARNING] ThreadPool is down")
            return
        # Start new thread
        # if len(self._running_threads) < self._max_threads and len(self._free_threads) == 0:
        if len(self._running_threads) < self._max_threads:
            thread = threading.Thread(target=self.run)
            print("[INFO] Create thread: ", thread.getName())
            thread.start()
        # Or use exist thread
        self._queue.put(task)
 
    # All threads would call this
    def run(self):
        thread_id = threading.currentThread().getName()
        # Add to busy list
        self._running_threads.append(thread_id)
        task = self._queue.get()
        # Work until handler say stop
        while task != STOP:
            func, args = task
            # print("[INFO] Thread: {0} get a job: {1}".format(thread_id, params))

            try:
                result = func(thread_id, args)
                self._collect.append(result)
            except Exception as e:
                # print("[WARNING]Thread: {0} throw {1}".format(thread_id, e))
                logging.exception(e)
            # Finish one job (wether success or not)
            self._free_threads.append(thread_id)
            # All threads will block here if no job
            task = self._queue.get()
            # print("[INFO] Queue: get a job: {0}, thread: {1} run again".format(params, thread_id))
            self._free_threads.remove(thread_id)
        
        self._running_threads.remove(thread_id)
        # print("[INFO] Current thread: {0} is done".format(thread_id))

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

    # Main execution logic
    def run_task(self, task, args):
        func = self._get_func(task)
        for i in range(self._thread_num):
            self.submit(func, args)
    
    def _get_func(self, task):
        if task not in dir(Utils):
            raise AttributeError(task + " does not exist")
        func = getattr(Utils, task)
        return func


# Test job function
'''
Test function for thread pool
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
