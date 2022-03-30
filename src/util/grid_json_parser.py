# author: Xiaotian Li
# desc: parse grids from json and store them
from matplotlib.pyplot import grid
import ijson
        
from decimal import Decimal
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

            # Need sort the file!
            objects.sort(key=(lambda x: x['properties']['id']), reverse=False)

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

    # TODO 这是可以获得所有方格关键点坐标的函数
    # grids: {A1:[[x1,y1],[x2,y2]], B1:[[x1,y1],[x2,y2]], C1:[[x1,y1],[x2,y2]] }
    def get_all_grids(self):
        return self.__grids

    # TODO 这是可以获得指定名字的方格的关键点坐标的函数
    # input grid name, like 'A1', then get A1's coordinate [[x1,y1],[x2,y2]]
    def get_grid_by_name(self, name):
        return self.__grids[name]


    def which_grid(self,pos):
        
        grid_info = self.__grids
        GAP = Decimal('0.15')
        achor1 = grid_info['A1'][0]
        achor2 = grid_info['A1'][1]
        x_0, y_0 = achor1
        x_1, y_1 = achor2

        x, y = pos
        x_step = -GAP
        y_step = GAP

        x_count = 0
        y_count = 0

        while True:
            if x <= x_1 and x >= x_0:
                break
            x += x_step
            x_count += 1

        while True:
            if y > y_1 and y <= y_0:
                break
            y += y_step
            y_count += 1
        
        index = (x_count) * 4 + y_count

        for key in grid_info.keys():
            if index == 0:
                print(str(key))
                return str(key)
            index -= 1

        return 'OUT OF RANGE'