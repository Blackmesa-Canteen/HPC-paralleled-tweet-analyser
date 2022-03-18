# author: Xiaotian Li
# desc: parse grids from json and store them
import ijson

from src.config.config_handler import ConfigHandler
from src.util.singleton_decorator import singleton


@singleton
class GridJsonParser:
    # one grid rectangles: [[x1,y1],[x2,y2]]
    # grids: {A1:[[x1,y1],[x2,y2]], B1:[[x1,y1],[x2,y2]], C1:[[x1,y1],[x2,y2]] }
    __grids = {}

    def __init__(self):
        # read points from json and put ranctangles in sboxes
        config_handler = ConfigHandler()

        grid_file_path = config_handler.get_grid_path()
        grid_rows = config_handler.get_grid_rows()
        grid_columns = config_handler.get_grid_columns()

        with open(grid_file_path, 'r') as load_f:
            objects = list(ijson.items(load_f, 'features.item'))
            index = 0

            for col in range(grid_columns):
                for row in range(grid_rows):

                    # if grid file provided is not a square matrix, prevent out of bound
                    if index >= len(objects):
                        break

                    col_id = str(col + 1)
                    row_id = chr(ord('A') + row)

                    useful_point_A = objects[index]['geometry']['coordinates'][0][0]
                    useful_point_B = objects[index]['geometry']['coordinates'][0][2]

                    point = [[useful_point_A[0], useful_point_A[1]], [useful_point_B[0], useful_point_B[1]]]

                    self.__grids['' + row_id + col_id] = point

                    index += 1

    # grids: {A1:[[x1,y1],[x2,y2]], B1:[[x1,y1],[x2,y2]], C1:[[x1,y1],[x2,y2]] }
    def get_all_grids(self):
        return self.__grids

    # input grid name, like 'A1', then get A1's coordinate [[x1,y1],[x2,y2]]
    def get_grid_by_name(self, name):
        return self.__grids[name]
