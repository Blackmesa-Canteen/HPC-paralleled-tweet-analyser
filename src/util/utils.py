
'''
定义线程任务
'''

import queue
from src.handler.lang_calc_handler import LangCalcHandler


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


# TODO 用来测试
def test_task():
    pass

def generate(num):
    q = queue.Queue()
    # TODO
    return q
    
