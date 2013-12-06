__author__ = 'Ryan'


class File(object):
    __file = None
    __file_name = ""

    def __init__(self, file_name):
        self.__file_name = file_name
        self.__file = open(file_name, "r")

    def read_line(self):
        return self.__file.readline()
