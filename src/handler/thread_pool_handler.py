# # TODO 线程池管理，可能能用上

# from src.util.singleton_decorator import singleton
# from src.config.twitter_type_enum import ThreadPoolSign
import queue
import threading
import time


STOP = (404,404)
# Test job function 
def func(thread_name, jobid):
    for i in range(0,3):
        print("[INFO] Thread: ", thread_name, " is doing job {0}".format(jobid))
        # time.sleep(0.1)
    print("[INFO] Thread ", thread_name, " finish job {0}".format(jobid))
    return jobid

class ThreadPoolHandler:

    def __init__(self, max_threads, max_jobs=1000):
        # Maybe useful
        self._max_threads = max_threads
        self._max_jobs = max_jobs
        self._cancel_flag = False

        # Main data structure 
        self._queue = queue.Queue(maxsize=max_jobs)
        self._running_threads = []
        self._free_threads = []

        # list append thread safe
        self._collect = []

    def submit(self, job, args):
        # How jobs are defined
        load = (job, args)
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
            func, jobid = task
            print("[INFO] Thread: {0} get a job: {1}".format(thread_id, jobid))

            try:
                result = func(thread_id, jobid)
                self._collect.append(result)
            except Exception as e:
                print("[WARNING]Thread: {0} throw {1}".format(thread_id, e))
            # Finish one job (wether success or not)
            self._free_threads.append(thread_id)
            # All threads will block here if no job
            task = self._queue.get()
            print("[INFO] Queue: get a job: {0}, thread: {1} run again".format(jobid, thread_id))
            self._free_threads.remove(thread_id)
        
        self._running_threads.remove(thread_id)
        print("[INFO] Current thread: {0} is done".format(thread_id))

    def stop(self):
        self._cancel_flag = True
        for i in range(0, len(self._running_threads)):
            self._queue.put(STOP)

    def collect(self):
        return self._collect

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

if __name__ == '__main__':
    pool = ThreadPoolHandler(20)
    for i in range(1000):
        pool.submit(func, i)
    time.sleep(4)
    pool.stop()
    print(pool.collect())
    print(len(pool.collect()))

