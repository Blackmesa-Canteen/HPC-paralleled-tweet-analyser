# Here we will write main script with

import src.config.config_handler
from src.util.grid_json_parser import GridJsonParser
from src.util.lang_tag_json_parser import LangTagJsonParser
from src.util.twitter_json_parser import TwitterJsonParser


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    config_handler = src.config.config_handler.ConfigHandler()
    print(config_handler.get_grid_path(), " ", config_handler.get_grid_columns())
    print('upper-bound-rows', config_handler.get_upper_bound_rows_per_iteration())

    lang_tag_parser = LangTagJsonParser()
    print(lang_tag_parser.get_tag_lang_map())

    grid_parser = GridJsonParser()
    print(grid_parser.get_all_grids())
    print(grid_parser.get_grid_by_name('B1'))
    print(grid_parser.get_grid_by_name('B2'))
    print(grid_parser.get_grid_by_name('B3'))
    print(grid_parser.get_grid_by_name('B4'))

    print('----------------------------------------')

    print(grid_parser.get_grid_by_name('A2'))
    print(grid_parser.get_grid_by_name('B2'))
    print(grid_parser.get_grid_by_name('C2'))
    print(grid_parser.get_grid_by_name('D2'))

    twitter_json_parser = TwitterJsonParser()
    print(twitter_json_parser.get_total_rows())

    res_queue = twitter_json_parser.parse_valid_coordinate_lang_maps_in_range(start_index=0, step=500000000)

    while not res_queue.empty():
        print(res_queue.get())




# See PyCharm help at https://www.jetbrains.com/help/pycharm/
