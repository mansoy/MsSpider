# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


import copy
import pymysql
import scrapy
import re
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
from twisted.enterprise import adbapi
from MsSpider.items import TeamItem
from MsSpider.items import LeagueItem
from MsSpider.items import BetsItem
from ..comm import GGlobal


from MsSpider.comm.MsDebug import MsLog


class BasePipeline(object):
    def __init__(self, db_pool, img_store):
        self.db_pool = db_pool
        self.initdata('b_bets').addCallback(self.parseData, 'b_bets')
        self.initdata('b_league').addCallback(self.parseData, 'b_league')
        self.initdata('b_fteam').addCallback(self.parseData, 'b_fteam')
        self.iCount = 0
        self.img_store = img_store

    @classmethod
    def from_settings(cls, settings):
        db_pool = adbapi.ConnectionPool(
            'pymysql',
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DB"],
            user=settings["MYSQL_USER"],
            password=settings["MYSQL_PASSWORD"],
            charset="utf8",
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True)
        return cls(db_pool, 'D:\ImgStore')

    def parseData(self, datas, table):
        for data in datas:
            if table == 'b_bets':
                GGlobal.bets[data['name']] = data['id']
            elif table == 'b_league':
                GGlobal.league[data['name']] = data['id']
            elif table == 'b_fteam':
                GGlobal.fteam[data['name']] = data['id']

    def initdata(self, table):
        print('MS - initdata.....')
        return self.db_pool.runQuery("select * from {0}".format(table))

    def handle_error(self, failure, item):
        print('插入数据失败，原因：{}，错误对象：{}'.format(failure, item))

    def _addBaseItem(self, id, item):
        if isinstance(item, TeamItem):
            GGlobal.bets[item['name']] = id
        elif isinstance(item, LeagueItem):
            GGlobal.fteam[item['name']] = id
        elif isinstance(item, BetsItem):
            GGlobal.bets['name'] = id

    def addBaseItem(self, cur, item):
        try:
            sql, values = item.get_insert_sql()
            cur.execute(sql, values)
            return cur.connection.insert_id()
        except Exception as e:
            print('addBaseItem err: {}'.format(e))

    def updBaseItem(self, cur, item):
        try:
            sql, values = item.get_update_sql()
            cur.execute(sql, values)
        except Exception as e:
            print('addBaseItem err: {}'.format(e))

    def process_item(self, item, spider):
        try:
            # 对象拷贝   深拷贝
            asynItem = copy.deepcopy(item)  # 需要导入import copy
            query = self.db_pool.runInteraction(self.process_base_item, asynItem)
            query.addErrback(self.handle_error, asynItem)
        except Exception as e:
            print('process_item err:{0}'.format(e))

    def process_base_item(self, cursor, item):
        try:
            id = -1
            if isinstance(item, TeamItem):
                id = self.b_fteam.get(item['name'], -1)
            elif isinstance(item, LeagueItem):
                id = self.b_league.get(item['name'], -1)

            if id == -1:
                self.addBaseItem(cursor, item).addCallback(self._addBaseItem, item)
            else:
                item['id'] = id
                self.updBaseItem(cursor, item)
        except Exception as e:
                print(e)
                return False

    def close_spider(self, spider):
        self.db_pool.close()
        MsLog.debug('[{0}] 结束'.format(spider.name))


# 下载图片的类
class BaseImgPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        try:
            if isinstance(item, TeamItem):
                image_url = item['imgurl']
                yield scrapy.Request(image_url, meta={'name': item['imgname']})
        except Exception as e:
            print(e)

    def item_completed(self, results, item, info):
        if isinstance(item, TeamItem):
            # 是一个元组，第一个元素是布尔值表示是否成功
            if not results[0][0]:
                raise DropItem('下载失败')
        return item

    # 重命名，若不重写这函数，图片名为哈希，就是一串乱七八糟的名字
    def file_path(self, request, response=None, info=None):
        # 接收上面meta传递过来的图片名称
        name = request.meta['name']
        # 提取url前面名称作为图片名
        image_name = request.url.split('/')[-1]
        # 清洗Windows系统的文件夹非法字符，避免无法创建目录
        folder_strip = re.sub(r'[？\\*|“<>:/]', '', str(name))
        # 分文件夹存储的关键：{0}对应着name；{1}对应着image_guid
        # filename = u'{0}/{1}'.format(folder_strip, image_name)
        filename = u'{0}.png'.format(folder_strip, name)
        return filename
