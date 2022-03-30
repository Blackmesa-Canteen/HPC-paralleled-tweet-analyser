# TODO 在这里写统计语言和位置的计算逻辑， 最好是做成一个对象，因为可能要存储下结果，最后提供给给main调用。
'''
可能需要用到多线程，使用线程池，每个线程计算的步长范围step定义到了config.yml里

写逻辑的时候，先不考虑多进程（多节点）的情况。 就是一个进程下，给定twitter文件中的起点和范围（这个范围不是线程的处理步长范围step，这个范围整个进程需要处理的更大的步长范围）
，从twitter取出关键的线程安全的队列(已经写出这个方法)， 然后每个线程从队列中中消费对应的step个信息进行计算，

 目前阶段，最终计算结果等所有线程搞完后，主线程打印最终结果在命令行页面即可，

等这里逻辑完备，再到main里我们一起考虑下如何处理多进程的情况

[补充]: 如果进程量过少, 文本过大, 如果把文件均分的话, 单个进程仍然太大, 所以在配置文件中引入了upper-bound-rows-per-iteration量, 用于设置每次迭代时装入内存的行数的数目
'''


from src.util.twitter_json_parser import TwitterJsonParser
from src.config.config_handler import ConfigHandler
from src.util.grid_json_parser import GridJsonParser

class LangCalcHandler:

    def __init__(self, thread_id, table):

        # self._twitter_parser = twitter_parser
        # self._grid_parser = gridparser
        # self._args = args
        self._thread_id = thread_id
        self._table = table

    def handle(self, message):
        self._table.append(self._thread_id)        

    def result(self):
        return self._table

    # def lang_calc(self):
    #     #[{'coordinates': [x, y], 'lang_tag': 'en'},{...},{...}, ...]
    #     for dict in self._args:
    #         try:
    #             coord = dict['coordinates']
    #             lang_tag = dict['lang_tag']
    #         except Exception as e:
    #             print("[WARNING] Missing keys")
    #             continue


