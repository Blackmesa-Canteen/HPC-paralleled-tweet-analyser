import sys
import os

from src.util.singleton_decorator import singleton


@singleton
class PathUtil(object):
    """tools for get project root path"""

    def __init__(self):
        # 判断调试模式
        debug_vars = dict((a, b) for a, b in os.environ.items()
                          if a.find('IPYTHONENABLE') >= 0)
        # 根据不同场景获取根目录
        if len(debug_vars) > 0:
            """当前为debug运行时"""
            self.__rootPath = sys.path[2]
        elif getattr(sys, 'frozen', False):
            """当前为exe运行时"""
            self.__rootPath = os.getcwd()
        else:
            """正常执行"""
            self.__rootPath = sys.path[1]
        # 替换斜杠
        self.__rootPath = self.__rootPath.replace("\\", "/")

    def get_path_from_resources(self, file_name):
        """按照文件名拼接资源文件路径"""
        file_path = "%s/resources/%s" % (self.__rootPath, file_name)
        return file_path

    def get_root_path(self):
        return self.__rootPath



if __name__ == '__main__':
    """测试"""
    # path = PathUtil.getPathFromResources("context.ini")
    PathUtil = PathUtil()
    print(PathUtil.get_root_path())
