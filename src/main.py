# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# 思路
# rank_0 will perform as a master node to get results from COMM_WORLD and calc total values,
# if all done, show final res on the screen

# the other ranks will calc language separately, then send res to COMM_WORLD
# other ranks need multi-thread?

# If there is only one rank here, run calc logic in rank_0.
import src.config.config_handler


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    config_handler = src.config.config_handler.ConfigHandler()
    print(config_handler.get_thread_pool_size())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
