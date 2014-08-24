__author__ = 'Ryan'

from os.path import join
from datetime import *
import os
import Frameworks.File as File
import Frameworks.MySQL as MySQL


def import_data(des, db_name):
    tables = []
    for root, dirs, files in os.walk(des):
        for file_name in files:
            if file_name.find('.txt') == -1:
                continue
            else:
                table_name = file_name.split('.')[0]
                tables.append(table_name)
                full_file_name = join(root, file_name)
                txt_file = File.File(full_file_name)
                stock_name = txt_file.read_line().decode('gbk').encode('utf-8')
                #for str in stock_name.split(" ", 3):
                #    print str.lstrip().rstrip()
                title = txt_file.read_line().decode('gbk').encode('utf-8')
                #for str in title.lstrip().split("\t", 7):
                #    print str.lstrip()
                mysql = MySQL.MySQL(db_name)
                mysql.connect()
                col_type = list()
                col_type.append("`ID` INT NOT NULL AUTO_INCREMENT")
                col_type.append("`DATE` DATETIME NULL")
                col_type.append("`START_PRICE` FLOAT NULL")
                col_type.append("`HIGH_PRICE` FLOAT NULL")
                col_type.append("`LOW_PRICE` FLOAT NULL")
                col_type.append("`CLOSE_PRICE` FLOAT NULL")
                col_type.append("`DEAL_AMOUNT` INT NULL")
                col_type.append("`DEAL_PRICE` FLOAT NULL")
                mysql.create_table(table_name, "ID", col_type)
                content = txt_file.read_line()
                data = list()
                while content:
                    content = content.replace('\n', '')
                    contents = content.split(';', 7)
                    content = txt_file.read_line()
                    if len(contents) < 7:
                        continue
                    else:
                        contents[0] = datetime.strptime(contents[0], "%m/%d/%Y").strftime("%Y-%m-%d %H:%M:%S")
                        data.append(contents)
                mysql.insert_many(table_name, "`DATE`, `START_PRICE`, `HIGH_PRICE`, `LOW_PRICE`, \
                `CLOSE_PRICE`, `DEAL_AMOUNT`, `DEAL_PRICE`", data)
                mysql.close_connect()

if __name__ == '__main__':
    import_data("C:\\new_gdzq_v6\\T0002\\export", "daily")