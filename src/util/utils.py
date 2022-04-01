
# author: YuanZhi Shang
import logging
import queue
import random
from decimal import Decimal
from socket import timeout
from threading import Lock
from time import time

from pip import main
from src.handler.lang_calc_handler import LangCalcHandler
from src.util.lang_tag_json_parser import LangTagJsonParser

class Utils:

    @staticmethod
    def lang_calc(thread_id, args):
        # print("[INFO] Thread ", thread_id, " start job")
        main_queue, step, grid_parser, lang_tag_parser = args
        lang_calc_handler = LangCalcHandler(thread_id, grid_parser, lang_tag_parser)

        '''
        这里可能存在并发问题 
        '''
        records = 0

        while step != 0:
            if main_queue.empty():
                break
            else:
                try:
                    message = main_queue.get(block=False)
                    lang_calc_handler.handle(message)
                    records += 1
                except Exception as e:
                    logging.exception(e)
                    # print("[INFO] {0} finish jobs: {1} records".format(thread_id, records))
                    return lang_calc_handler.result()
                # print("{0} get message: {1} and current step: {2}".format(thread_id, message, step))
            step -= 1
        # print("[INFO] {0} finish jobs: {1} records".format(thread_id, records))
        return lang_calc_handler.result()

    @staticmethod
    def sample_generator(num):

        print("[INFO] Generating test data, length: ", num)
        x_left = 150.7655
        x_right = 151.3655
        y_top = -33.55412
        y_down = -34.15412
        tag_list = list(LangTagJsonParser().get_tag_lang_map())
        q = queue.Queue()
        while num:
            record = {}
            # random location
            x = Decimal(str(random.uniform(x_left, x_right)))
            y = Decimal(str(random.uniform(y_down, y_top)))
            tag = tag_list[random.randint(0, len(tag_list) - 1)]
            record['coordinates'] = [x, y]
            record['lang_tag'] = tag
            q.put(record)
            num -= 1
        print("[INFO] Finish generation !")        
        return q
    
    @staticmethod
    def view(table):
        print("[INFO] No implementation for visualization")

        pass

    @staticmethod
    def test_task():
        pass

    @staticmethod
    def table_union(table_list, grid_parser):
        raw_table = grid_parser.get_raw_table()    
        # table: {'A1':[num, (), {}], 'A1':[num, (), {}], .... }
        # table_list: [table1, table2, table3, ... ]
        for table in table_list:
            for key in table.keys():
                # update num
                raw_table[key][0] += table[key][0]

                # update lang num
                raw_table[key][1] = raw_table[key][1].union(table[key][1])

                # update lang rank
                raw_lang_dict = raw_table[key][2]
                lang_dict = table[key][2]

                for lang in lang_dict.keys():
                    
                    if lang in raw_lang_dict:
                        raw_lang_dict[lang] += lang_dict[lang]
                    else:
                        raw_lang_dict[lang] = lang_dict[lang]
        
        for key in raw_table.keys():
            raw_table[key][1] = len(raw_table[key][1])
            # add [:10] to get top 10
            raw_table[key][2] = list(sorted(raw_table[key][2].items(), key=lambda x: x[1], reverse=True))


        return raw_table