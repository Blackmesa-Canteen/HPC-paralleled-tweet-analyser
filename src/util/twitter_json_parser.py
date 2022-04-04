# author: Xiaotian Li
# desc: util for parsing input twitter json file and store some meta data(Not all, otherwise will blow up memory)
from decimal import Decimal
import json
import logging
from queue import Queue
import queue
import random

import ijson

from src.config.config_handler import ConfigHandler
from src.config.twitter_type_enum import InputTwitterType
from src.util.lang_tag_json_parser import LangTagJsonParser
from src.util.singleton_decorator import singleton

# magic number for parsing total rows
TOTAL_ROWS_START_INDEX = 14


# static function for parsing the line 1 of the file
def parse_total_rows(f, twitter_type):

    if twitter_type != InputTwitterType.BIG:
        # try to parse the total row number
        line = f.readline()
        line = line[TOTAL_ROWS_START_INDEX:]

        comma_index = 0
        while line[comma_index] != ',':
            comma_index += 1
        result = int(line[:comma_index])
        return result

    else:
        line = f.readline()
        line = line[TOTAL_ROWS_START_INDEX:]

        comma_index = 0
        while line[comma_index] != ',':
            comma_index += 1

        total_rows = int(line[:comma_index])

        offset_ptr = comma_index
        while line[offset_ptr] != ':':
            offset_ptr += 1
        # move the pointer to the offset number
        offset_ptr += 1

        offset_end_ptr = offset_ptr
        while line[offset_end_ptr] != ',':
            offset_end_ptr += 1

        offset = int(line[offset_ptr:offset_end_ptr])

        return total_rows - offset

@singleton       
class TwitterJsonParser:

    # A thread-safe queue to contain twitter info
    __twitter_queue = Queue()

    def __init__(self):
        # get configuration
        config_handler = ConfigHandler()

        self.__input_file_path = config_handler.get_twitter_path()
        self.__input_file_type = config_handler.get_input_twitter_type()

        # get total lines
        with open(self.__input_file_path, 'r', encoding='utf-8') as f:
            self.__total_rows = parse_total_rows(f, self.__input_file_type)
    '''
    parse twitters start from specific start index and within specific step
    only parse out useful information as list of dicts
    start from 0, Ends in (totalRows - 2), 1 line is for total_rows info
    useful info: doc.metadata.iso_language_code, doc.geo, doc.coordinates
    
    returns: a thread-safe queue [{'coordinates': [x, y], 'lang_tag': 'en'},{...},{...}, ...]
    '''
    def parse_valid_coordinate_lang_maps_in_range(self, start_index, step):
        upper_bound_index = self.__total_rows - 2

        # check
        if start_index > upper_bound_index or start_index < 0:
            print('[ERR] start_index of parse_valid_coordinate_lang is out of bound!')
            return None

        if step < 0:
            print('[WARN] step is less than 0, no output of parse_valid_coordinate_lang')
            return None
            
        # decide scanning range
        # delta between start and uppermost index
        delta = upper_bound_index - start_index
        end_index = upper_bound_index
        if step <= delta:
            end_index = start_index + step
        '''
        parse twitter Json
        
        condition: 1.coordinate is not null
                   2. language tag is not null or und
        '''
        index = 0
        with open(self.__input_file_path, 'r', encoding='utf-8') as f:
            objects = ijson.items(f, 'rows.item')
            while index <= end_index:
                try:

                    # move pointer until get start_index
                    if index < start_index:
                        objects.__next__()
                        index += 1
                        continue

                    obj = objects.__next__()

                    coordinates = obj['doc']['coordinates']
                    lang_tag = obj['doc']['metadata']['iso_language_code']

                    # condition 1
                    has_coordinate = coordinates is not None

                    # condition 2
                    has_correct_lang_tag = lang_tag is not None and lang_tag != 'und'

                    if has_coordinate and has_correct_lang_tag:
                        point = coordinates['coordinates']
                        wrapper = {'coordinates': point, 'lang_tag': lang_tag}
                        self.__twitter_queue.put(wrapper)

                    index += 1
                except StopIteration as e:
                    break

        return self.__twitter_queue

    '''
    Duplicate function from above that use standard python json lib
    '''
    def parse_valid_coordinate_lang_maps_in_range_v2(self, start_index, step):
        upper_bound_index = self.__total_rows - 2

        # check
        if start_index > upper_bound_index or start_index < 0:
            print('[ERR] start_index of parse_valid_coordinate_lang is out of bound!')
            return None

        if step < 0:
            print('[WARN] step is less than 0, no output of parse_valid_coordinate_lang')
            return None
            
        # decide scanning range
        # delta between start and uppermost index
        delta = upper_bound_index - start_index
        end_index = upper_bound_index
        if step <= delta:
            end_index = start_index + step
        '''
        parse twitter Json
        
        condition: 1. coordinate is not null
                   2. language tag is not null or und
        '''
        index = 0
        with open(self.__input_file_path, 'r', encoding='utf-8') as f:
            f.readline()
            try:
                print("Start index: ", start_index, " End index: ", end_index)
                while index <= end_index:
                    if index <  start_index:
                        line = f.readline()
                        index += 1
                        continue

                    line = f.readline()
                    line = line.strip()
                    if index == upper_bound_index:
                        line = line.rstrip('}')
                        line = line.rstrip(']')
                    else:

                        line = line.rstrip(',')

                    obj = json.loads(line)
                    coordinates = obj['doc']['coordinates']
                    lang_tag = obj['doc']['metadata']['iso_language_code']

                    # condition 1
                    has_coordinate = coordinates is not None

                    # condition 2
                    has_correct_lang_tag = lang_tag is not None and lang_tag != 'und'

                    if has_coordinate and has_correct_lang_tag:
                        point = coordinates['coordinates']
                        wrapper = {'coordinates': point, 'lang_tag': lang_tag}
                        self.__twitter_queue.put(wrapper)
                    index += 1
                
            except Exception as e:
                pass
        return self.__twitter_queue


    def get_total_rows(self):
        return self.__total_rows

    def free_twitter_queue(self):
        self.__twitter_queue = Queue()

    def get_twitter_queue(self):
        return self.__twitter_queue

    def test_parse_coordinates(self):
        pass
    
    '''
    A test data generator that would result a Queue contain num records

    Usage: q = test_queue_generator(num)

    q : [{'coordinates': [x, y], 'lang_tag': 'en'},{...},{...}, ...] num records

    '''
    def test_queue_generator(self, num):
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
        return q
    

