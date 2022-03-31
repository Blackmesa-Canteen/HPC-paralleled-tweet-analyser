
'''
定义线程任务
'''

import queue
import random
from decimal import Decimal
from src.handler.lang_calc_handler import LangCalcHandler
from src.util.lang_tag_json_parser import LangTagJsonParser

class utils:

    def task_lang_calc(thread_id, args):

        print("[INFO] Thread ", thread_id, " start job {0}".format(args))
        main_queue, step = args
        lang_calc_handler = LangCalcHandler(thread_id)

        print(main_queue.empty())
        while step:
            if main_queue.empty():
                break
            else:
                message = main_queue.get()
                lang_calc_handler.handle(message)
            step -= 1
        return lang_calc_handler.result()

    def sample_generator(num):
        x_left = 150.7655
        x_right = 151.3655
        y_top = -33.55412
        y_down = -34.15412
        tag_list = list(LangTagJsonParser.get_tag_lang_map())
        q = queue.Queue()
        while num:
            record = {}
            # random location
            x = Decimal(random.uniform(x_left, x_right))
            y = Decimal(random.uniform(y_down, y_top))
            tag = tag_list[random.randint(0, len(tag_list - 1))]
            record['coordinates'] = [x, y]
            record['lang_tag'] = tag
            q.put(record)
            num -= 1        
        return q
    
    def visualise(table):
        pass

    # TODO 用来测试
    def test_task():
        pass

