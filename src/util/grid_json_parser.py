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

    '''
    yuanzhis: input a tuple coordinate, return a area tag
    '''
    def which_grid(self,pos):
        grids = self.__grids
    
        x_interval = [(grids['A1'][0][0], grids['A2'][0][0]), 
                        (grids['A2'][0][0], grids['A3'][0][0]),
                        (grids['A3'][0][0], grids['A4'][0][0]),
                        (grids['A4'][0][0], grids['B4'][1][0])]

        y_interval = [(grids['A1'][0][1], grids['B1'][0][1]), 
                        (grids['B1'][0][1], grids['C1'][0][1]),
                        (grids['C1'][0][1], grids['D1'][0][1]),
                        (grids['D1'][0][1], grids['D2'][1][1])]
        x, y = pos
        x_grid = 0
        y_grid = 0

        for interval in x_interval:
            if x == x_interval[0][0]:
                x_grid = 0
                break
            if x > interval[0] and x <= interval[1]:
                break
            x_grid += 1
        for interval in y_interval:
            if y == y_interval[3][1]:
                y_grid = 3
                break
            if y <= interval[0] and y > interval[1]:
                break
            y_grid += 1

        index = x_grid * 4 + y_grid
        grid_list = list(grids)  

        return grid_list[index]


    # grids: {A1:[[x1,y1],[x2,y2]], B1:[[x1,y1],[x2,y2]], C1:[[x1,y1],[x2,y2]] }
    def get_all_grids(self):
        return self.__grids

    # input grid name, like 'A1', then get A1's coordinate [[x1,y1],[x2,y2]]
    def get_grid_by_name(self, name):
        return self.__grids[name]
    
    '''
    Table format
    '''
    def get_raw_table(self):
        raw_table = {}
        for key in self.__grids.keys():
            raw_table[key] = [None]*3
            raw_table[key][0] = 0
            raw_table[key][2] = {}
            raw_table[key][1] = set()
        return raw_table
 
