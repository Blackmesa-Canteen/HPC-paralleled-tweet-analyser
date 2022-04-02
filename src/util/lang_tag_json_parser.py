# author: Xiaotian Li
# desc: parse language tags from json and store them
import os

import ijson

from src.util.path_util import PathUtil


class LangTagJsonParser:
    # {en: English, zh-tw: Chinese}
    __tag_lang_map = {}

    def __init__(self):
        path_util = PathUtil()
        root_path = path_util.get_root_path()

        lang_tag_json_path = os.path.join(root_path, 'src', 'langTag.json')
        with open(lang_tag_json_path, 'r') as load_f:
            objects = ijson.items(load_f, 'item')

            for obj in objects:
                self.__tag_lang_map[obj['subtag']] = obj['name']

    def get_lang_by_tag_name(self, tag_name):
        return self.__tag_lang_map[tag_name]

    # {en: English, zh-tw: Chinese, ...}
    def get_tag_lang_map(self):
        return self.__tag_lang_map
