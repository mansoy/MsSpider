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
from ..items import CountryItem
from ..items import LeagueItem
from ..items import SeasonItem


from ..comm.MsDebug import MsLog


class LeagueDataPipeline(object):
    def __init__(self, db_pool, img_store):
        self.db_pool = db_pool
        self.b_bets = {}
        self.b_league = {}
        self.b_fteam = {}
        self.b_country = {}
        self.b_season = {}
        self.initdata('b_bets').addCallback(self.parseData, 'b_bets')
        self.initdata('b_league').addCallback(self.parseData, 'b_league')
        self.initdata('b_fteam').addCallback(self.parseData, 'b_fteam')
        self.initdata('b_country').addCallback(self.parseData, 'b_country')
        self.initdata('b_season').addCallback(self.parseData, 'b_season')
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

    def get_dicts(self, item):
        if isinstance(item, CountryItem):
            dicts = self.b_country
        elif isinstance(item, LeagueItem):
            dicts = self.b_league
        elif isinstance(item, SeasonItem):
            dicts = self.b_season
        return dicts

    def parseData(self, datas, table):
        for data in datas:
            if table == 'b_bets':
                self.b_bets[data['name']] = data['id']
            elif table == 'b_league':
                self.b_league[data['name']] = data['id']
            elif table == 'b_fteam':
                self.b_fteam[data['name']] = data['id']
            elif table == 'b_country':
                self.b_country[data['name']] = data['id']
            elif table == 'b_season':
                self.b_season[data['ssid']] = data['id']

    def initdata(self, table):
        return self.db_pool.runQuery("select * from {0}".format(table))

    def handle_error(self, failure, item):
        print('插入数据失败，原因：{}，错误对象：{}'.format(failure, item))

    def addBaseItem(self, cur, item):
        try:
            sql, values = item.get_insert_sql()
            cur.execute(sql, values)
            id = cur.connection.insert_id()
            dicts = self.get_dicts(item)
            if isinstance(item, SeasonItem):
                dicts[item['ssid']] = id
            else:
                dicts[item['name']] = id
        except Exception as e:
            # print('addBaseItem err: {}'.format(e))
            pass

    def updBaseItem(self, cur, item):
        try:
            sql, values = item.get_update_sql()
            cur.execute(sql, values)
        except Exception as e:
            # print('addBaseItem err: {}'.format(e))
            pass

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
            if isinstance(item, SeasonItem):
                id = self.b_season.get(item['ssid'], -1)
                lid = self.b_league.get(item['lname'], -1)
                if lid == -1:
                    return False
                item['lid'] = lid
            else:
                dicts = self.get_dicts(item)
                id = dicts.get(item['name'], -1)

            if id == -1:
                self.addBaseItem(cursor, item)
            else:
                item['id'] = id
                self.updBaseItem(cursor, item)
        except Exception as e:
                print('process_base_item error:{}'.format(e))
                return False

    def close_spider(self, spider):
        self.db_pool.close()
        MsLog.debug('[{0}] 结束'.format(spider.name))


# 下载图片的类
class BaseImgPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        try:
            if isinstance(item, CountryItem):
                image_url = item['imgurl']
                yield scrapy.Request(image_url, meta={'name': item['imgname']})
        except Exception as e:
            print(e)

    def item_completed(self, results, item, info):
        if isinstance(item, CountryItem):
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
