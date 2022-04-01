# author: Xiaotian Li
# desc: enum for input json type

import enum

class InputTwitterType(enum.Enum):
    TINY = 0
    SMALL = 1
    BIG = 2

# @enum.unique
# class ThreadPoolSign(enum.Enum):
#     STOP = 0
#     CANCEL = 1
#     KILL = 2
