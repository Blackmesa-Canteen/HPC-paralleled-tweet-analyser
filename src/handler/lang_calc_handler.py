# TODO 在这里写统计语言和位置的计算逻辑， 最好是做成一个对象，因为可能要存储下结果，最后提供给给main调用。
'''
可能需要用到多线程，使用线程池，每个线程计算的步长范围step定义到了config.yml里

写逻辑的时候，先不考虑多进程（多节点）的情况。 就是一个进程下，给定twitter文件中的起点和范围（这个范围不是线程的处理步长范围step，这个范围整个进程需要处理的更大的步长范围）
，从twitter取出关键的线程安全的队列(已经写出这个方法)， 然后每个线程从队列中中消费对应的step个信息进行计算，

 目前阶段，最终计算结果等所有线程搞完后，主线程打印最终结果在命令行页面即可，

等这里逻辑完备，再到main里我们一起考虑下如何处理多进程的情况

[补充]: 如果进程量过少, 文本过大, 如果把文件均分的话, 单个进程仍然太大, 所以在配置文件中引入了upper-bound-rows-per-iteration量, 用于设置每次迭代时装入内存的行数的数目
'''


from curses import raw
from src.util.twitter_json_parser import TwitterJsonParser
from src.config.config_handler import ConfigHandler
from src.util.grid_json_parser import GridJsonParser

class LangCalcHandler:

    def __init__(self, thread_id, table, grid_parser, lang_parser):

        self._thread_id = thread_id
        self._table = table
        self._grid_parser = grid_parser
        self._lang_parser = lang_parser

    def handle(self, message):
        # self._table.append(self._thread_id)
        
        area = self._grid_parser.which_grid(tuple(message['coordinates']))
        lang = self._lang_parser.get_lang_by_tag_name(message['lang_tag'])
        record = self._table[area]

        # update tweet num
        record[0] += 1

        # update lang num
        record[1].add(lang)

        # update ranks
        lang_dict = record[2]

        if lang in lang_dict:
            lang_dict[lang] += 1
        else:
            lang_dict[lang] = 1

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
                raw_table[key][1].add(table[key][1])

                # update lang rank
                raw_lang_dict = raw_table[key][2]
                lang_dict = table[key][2]

                for lang in lang_dict.keys():
                    
                    if lang in raw_lang_dict:
                        raw_lang_dict[lang] += lang_dict[lang]
                    else:
                        raw_lang_dict[lang] = lang_dict[lang]
        return raw_table



    def result(self):
        return self._table

    def get_thread_id(self):
        return self._thread_id