# -*- coding:utf-8 -*-
# @FileName  :time_util.py
# @Time      :2022/3/29 4:03 PM
# @Author    : https://blog.csdn.net/weixin_39765588/article/details/110168966
"""
__title__ = 'timer class'

"""

import time


class Timer:

    # call this public attribute to get elapsed time
    elapsed = 0.0

    def __init__(self, func=time.perf_counter):

        self.elapsed = 0.0

        self._func = func

        self._start = None

    # call it when start a timing session
    def start(self):

        if self._start is not None:
            raise RuntimeError('Already started')

        self._start = self._func()

    # call it when stop a timing session
    def stop(self):

        if self._start is None:
            raise RuntimeError('Not started')

        end = self._func()

        self.elapsed += end - self._start

        self._start = None

    # reset elapsed time
    def reset(self):
        self.elapsed = 0.0

    @property
    def running(self):
        return self._start is not None

    def __enter__(self):

        self.start()

        return self

    def __exit__(self, *args):

        self.stop()


if __name__ == '__main__':
    timer = Timer()
    timer.start()
    time.sleep(2)
    timer.stop()

    print(timer.elapsed)
    timer.reset()
