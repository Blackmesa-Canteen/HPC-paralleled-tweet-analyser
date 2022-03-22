# author: Xiaotian li
# desc: a handler can parse config and store them in this object

import os.path
import sys

from src.config.twitter_type_enum import InputTwitterType
from src.util.path_util import PathUtil
from src.util.singleton_decorator import singleton
import yaml

# TODO 这个对象你可能会用到，它存储了配置文件里的一些元数据，需要数据的时候，调用对应的Getter就好
@singleton
class ConfigHandler:

    # init configs while constructing the object
    def __init__(self):
        # note: this path is based on the first caller of the constructor, not based on config_handler.py

        # get root path of the project
        path_util = PathUtil()
        root_path = path_util.get_root_path()

        config_file_path = os.path.join(root_path, 'src', 'config.yml')
        print("[DEBUG] config path:", config_file_path)

        if os.path.exists(config_file_path):
            with open(config_file_path, 'r', encoding='utf-8') as f:
                cfgs = yaml.safe_load(f)

                self.__step = cfgs['app']['step']
                if self.__step < 0:
                    print('[ERR] config step is error!')
                    sys.exit(-1)

                self.__grid_path = cfgs['app']['grid-path']
                self.__tiny_twitter_path = cfgs['app']['tiny-twitter-path']
                self.__small_twitter_path = cfgs['app']['small-twitter-path']
                self.__big_twitter_path = cfgs['app']['big-twitter-path']
                self.__thread_pool_size = cfgs['app']['thread-pool']['size']

                flag = cfgs['app']['input-twitter-type']
                if flag == 2:
                    self.__input_twitter_type = InputTwitterType.BIG
                elif flag == 1:
                    self.__input_twitter_type = InputTwitterType.SMALL
                elif flag == 0:
                    self.__input_twitter_type = InputTwitterType.TINY
                else:
                    print('[ERR] config input-twitter-type is error!')
                    sys.exit(-1)

                self.__grid_rows = cfgs['app']['grid-rows']
                self.__grid_columns = cfgs['app']['grid-columns']
                if self.__grid_rows < 0 or self.__grid_columns < 0:
                    print('[ERR] config grid columns/rows is error!')
                    sys.exit(-1)

                print('[debug] loaded cfg, big_twitter_path is: ', self.__big_twitter_path)

        else:
            print('warning: config/config.yml not found, using default settings')
            self.__step = 100
            self.__input_twitter_type = InputTwitterType.TINY
            self.__grid_path = '/data/projects/COMP90024/sydGrid.json'
            self.__tiny_twitter_path = '/data/projects/COMP90024/tinyTwitter.json'
            self.__small_twitter_path = '/data/projects/COMP90024/smallTwitter.json'
            self.__big_twitter_path = '/data/projects/COMP90024/bigTwitter.json'
            self.__thread_pool_size = 2
            self.__grid_rows = 4
            self.__grid_columns = 4

    # getters
    def get_grid_rows(self):
        return self.__grid_rows

    def get_grid_columns(self):
        return self.__grid_columns

    def get_step(self):
        return self.__step

    def get_input_twitter_type(self):
        return self.__input_twitter_type

    def get_grid_path(self):
        # check file exist or not
        if not os.path.exists(self.__grid_path):
            print("ERROR: sydGrid.json not exist, or config.yml is mistaken!")
            sys.exit(-1)

        return self.__grid_path

    # get the path of the twitter.json you need
    def get_twitter_path(self):
        twitter_type = self.__input_twitter_type

        if twitter_type == InputTwitterType.TINY:
            # check file exist or not
            if not os.path.exists(self.__tiny_twitter_path):
                print("ERROR: tinyTwitter.json not exist, or config.yml is mistaken!")
                sys.exit(-1)

            return self.__tiny_twitter_path
        elif twitter_type == InputTwitterType.SMALL:
            # check file exist or not
            if not os.path.exists(self.__small_twitter_path):
                print("ERROR: smallTwitter.json not exist, or config.yml is mistaken!")
                sys.exit(-1)

            return self.__small_twitter_path
        else:
            # check file exist or not
            if not os.path.exists(self.__big_twitter_path):
                print("ERROR: bigTwitter.json not exist, or config.yml is mistaken!")
                sys.exit(-1)

            return self.__big_twitter_path

    def get_thread_pool_size(self):
        return self.__thread_pool_size
