# author: Xiaotian Li
# desc: util for parsing input twitter json file and store some meta data(Not all, otherwise will blow up memory)
import ijson

from src.config.config_handler import ConfigHandler
from src.util.singleton_decorator import singleton

# magic number for parsing total rows
TOTAL_ROWS_START_INDEX = 14


# static function for parsing the line 1 of the file
def parse_total_rows(f):
    # try to parse the total row number
    line = f.readline()
    line = line[TOTAL_ROWS_START_INDEX:]
    comma_index = 0
    while line[comma_index] != ',':
        comma_index += 1
    result = int(line[:comma_index])
    return result


@singleton
class TwitterJsonParser:

    def __init__(self):
        # get configuration
        config_handler = ConfigHandler()

        self.__input_file_path = config_handler.get_twitter_path()

        # get total lines
        with open(self.__input_file_path, 'r', encoding='utf-8') as f:
            self.__total_rows = parse_total_rows(f)

    # TODO 这是你可能会用到的实例方法，给定开始行和步长，从twitter文件中解析出坐标和 lang_tag的部分集合
    '''
    parse twitters start from specific start index and within specific step
    only parse out useful information as list of dicts
    start from 0, Ends in (totalRows - 2), 1 line is for total_rows info
    useful info: doc.metadata.iso_language_code, doc.geo, doc.coordinates
    
    returns: [{'coordinates': [x, y], 'lang_tag': 'en'},{...},{...}, ...]
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
        res = []
        index = start_index
        with open(self.__input_file_path, 'r', encoding='utf-8') as f:
            objects = ijson.items(f, 'rows.item')
            while index <= end_index:
                try:
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
                        res.append(wrapper)

                    index += 1
                except StopIteration as e:
                    break

        return res

    # TODO 这是你可能会用到的实例方法，得到当前twitter文档的总行数
    def get_total_rows(self):
        return self.__total_rows

    def test_parse_coordinates(self):
        pass
