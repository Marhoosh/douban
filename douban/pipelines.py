# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import pandas as pd
import pymysql



class DoubanPipeline:


    def __init__(self):
        # 在初始化时创建一个空的列表来存储所有的 item 数据
        self.movies_data = []

    def process_item(self, item, spider):
        # 将每个爬取的 item 数据添加到列表中
        self.movies_data.append({
            'title': item.get('title', 'N/A'),  # Use 'N/A' or another default value if title is missing
            'rating_num': item.get('rating_num', 'N/A'),
            'inq': item.get('inq', 'N/A')
        })
        return item

    def close_spider(self, spider):
        # 爬虫结束时，将所有爬取的数据写入 Excel 文件
        df = pd.DataFrame(self.movies_data)
        df.to_excel('douban_top250_movies.xlsx', index=False)
        spider.log('Data has been written to Excel file.')


class MySQLPipeline:
    def __init__(self):
        # 在初始化时连接到 MySQL 数据库
        self.conn = pymysql.connect(
            host='110.41.68.46',    # MySQL 服务器地址
            port=3307,
            user='root',         # 数据库用户名
            password='CL@1023st', # 数据库密码
            database='libohong',    # 要连接的数据库名
            charset='utf8mb4'    # 设置字符集，避免中文乱码
        )
        self.cursor = self.conn.cursor()
        self.data_list = []
    def process_item(self, item, spider):
        # 插入数据到数据库
        self.insert_query = """
        INSERT INTO movie (title, rating_num, inq)
        VALUES (%s, %s, %s)
        """
        data = (item['title'], item['rating_num'], item['inq'])
        self.data_list.append(data)
        if(len(self.data_list)>=100):
            self.cursor.executemany(self.insert_query, self.data_list)
            self.data_list.clear()

        # return item





    def close_spider(self, spider):

        if (len(self.data_list) > 0):
            self.cursor.executemany(self.insert_query, self.data_list)
            self.data_list.clear()

        self.conn.commit()

        # 爬虫结束时，关闭数据库连接
        self.cursor.close()
        self.conn.close()
        spider.log('Data has been written to MySQL database.')

