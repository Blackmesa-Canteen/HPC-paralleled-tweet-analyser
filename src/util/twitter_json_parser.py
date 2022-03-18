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

    # parse twitters start from specific start index
    # the step is pre-defined in the config.yml!!
    # TODO: parse line logic
    def parse_lines(self, start_index):
        pass

    def get_total_rows(self):
        return self.__total_rows
