# author: Xiaotian Li
# desc: parse grids from json and store them

from src.util.singleton_decorator import singleton


@singleton
class GridJsonParser:

    # one grid rectangles: [[x1,y1],[x2,y2]]
    # sboxes: {A1:[[x1,y1],[x2,y2]], B1:[[x1,y1],[x2,y2]], C1:[[x1,y1],[x2,y2]] }
    sboxes = {}

#TODO finish it
    def __init__(self):
        # read points from json and put ranctangles in sboxes
        pass