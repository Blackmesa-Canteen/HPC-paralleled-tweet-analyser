# author: YuanZhi Shang

from src.util.twitter_json_parser import TwitterJsonParser
from src.config.config_handler import ConfigHandler
from src.util.grid_json_parser import GridJsonParser

class LangCalcHandler:

    def __init__(self, thread_id, grid_parser, lang_parser):

        self._thread_id = thread_id
        self._table = grid_parser.get_raw_table()
        self._grid_parser = grid_parser
        self._lang_parser = lang_parser

    def handle(self, message):
                
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

    def result(self):
        return self._table

    def get_thread_id(self):
        return self._thread_id