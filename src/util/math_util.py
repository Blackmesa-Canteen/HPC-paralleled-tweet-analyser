# author: Xiaotian Li
# desc: static tools for math/geo calc

# Is the point in the outer rectangle

# TODO: 这里是你可能会用到的一些数学、几何的静态函数
def is_point_in_box(poi, sbox, toler=0.0001):
    # sbox=[[x1,y1],[x2,y2]]
    if sbox[0][0] <= poi[0] <= sbox[1][0] and sbox[0][1] <= poi[1] <= sbox[1][1]:
        return True
    if toler > 0:
        pass
    return False


def is_rectA_above_of_B(sbox_A, sbox_B):
    return True if is_pointA_above_of_B(sbox_A[0], sbox_B[0]) else False


def is_rectA_left_of_B(sbox_A, sbox_B):
    return True if is_pointA_left_of_B(sbox_A[0], sbox_B[0]) else False


def is_pointA_above_of_B(point_A, point_B):
    return True if point_A[1] > point_B[1] else False


def is_pointA_left_of_B(point_A, point_B):
    return True if point_A[0] < point_B[0] else False
