# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


import copy
import pymysql
import time
from twisted.enterprise import adbapi
# from twisted.internet import reactor
from MsSpider.items import MatchItem
from MsSpider.items import ImmMatchItem
from MsSpider.items import OuOddsItem
from MsSpider.items import YaOddsItem
from MsSpider.items import DxOddsItem

from ..comm import GDict_bets
from ..comm import GDict_fteam
from ..comm import GDict_league

from MsSpider.comm.MsDebug import MsLog


class MsspiderPipeline(object):
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.initdata('b_bets').addCallback(self.parseData, 'b_bets')
        self.initdata('b_league').addCallback(self.parseData, 'b_league')
        self.initdata('b_fteam').addCallback(self.parseData, 'b_fteam')
        self.iCount = 0
        self.tickcount = int(round(time.time() * 1000))

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
        return cls(db_pool)

    def parseData(self, datas, table):
        print("MS - printData[{0}]...".format(table))
        for data in datas:
            if table == 'b_bets':
                GDict_bets[data['name']] = data['id']
            elif table == 'b_league':
                GDict_league[data['name']] = data['id']
            elif table == 'b_fteam':
                GDict_fteam[data['name']] = data['id']

    def initdata(self, table):
        return self.db_pool.runQuery("select * from {0}".format(table))

    def handle_error(self, failure, item):
        print('当前操作失败，原因：{}，错误对象：{}'.format(failure, item))

    # 添加基础数据记录
    def addBaseItem(self, cur, tablename, name, fname=''):
        try:
            sql = 'INSERT INTO {0}(`name`, `fname`, `remark`) VALUES(%(name)s, %(fname)s, %(remark)s)'.format(tablename)
            values = {
                'name': name,
                'fname': fname,
                'remark': ''
            }
            cur.execute(sql, values)
            return cur.connection.insert_id()
        except Exception as e:
            print(e)
            return -1

    # 添加记录
    def addDataItem(self, cur, item):
        try:
            sql, values = item.get_insert_sql()

            cur.execute(sql, values)
            return True
        except Exception as e:
            print(e)
            return False

    def process_item(self, item, spider):
        try:
            # 对象拷贝   深拷贝
            asynItem = copy.deepcopy(item)  # 需要导入import copy

            if isinstance(asynItem, MatchItem) or isinstance(asynItem, ImmMatchItem):
                query = self.db_pool.runInteraction(self.process_md_item, asynItem)
                query.addErrback(self.handle_error, asynItem)
            elif isinstance(asynItem, OuOddsItem) or isinstance(asynItem, YaOddsItem) or isinstance(asynItem, DxOddsItem):
                query = self.db_pool.runInteraction(self.process_odds_item, asynItem)
                query.addErrback(self.handle_error, asynItem)
            # return item
            if self.iCount > 0 and self.iCount % 100 == 0:
                print('MS - iCount: {} TimeCount: {}'.format(self.iCount, int(round(time.time() * 1000)) - self.tickcount))
            self.iCount += 1
        except Exception as e:
            print('process_item err:{0}'.format(e))

    def process_md_item(self, cursor, item):
        try:
            # 补全联赛信息
            # lsid = GDict_league.get(item['lname'], -1)
            # if lsid == -1:
            if item['lname'] not in GDict_league:
                lsid = self.addBaseItem(cursor, 'b_league', item['lname'])
                if lsid == -1:
                    return
                GDict_league[item['lname']] = lsid

            item['lid'] = GDict_league[item['lname']]

            # 补全球队信息-主队
            # mtid = GDict_fteam.get(item['mtname'], -1)
            # if mtid == -1:
            if item['mtname'] not in GDict_fteam:
                mtid = self.addBaseItem(cursor, 'b_fteam', item['mtname'], item['mtfname'])
                if mtid == -1:
                    return
                GDict_fteam[item['mtname']] = mtid

            item['mtid'] = GDict_fteam[item['mtname']]

            # 补全球队信息-客队
            # dtid = GDict_fteam.get(item['dtname'], -1)
            # if dtid == -1:
            if item['dtname'] not in GDict_fteam:
                dtid = self.addBaseItem(cursor, 'b_fteam', item['dtname'], item['dtfname'])
                if dtid == -1:
                    return
                GDict_fteam[item['dtname']] = dtid

            item['dtid'] = GDict_fteam[item['dtname']]

            return self.addDataItem(cursor, item)
        except Exception as e:
            print(e)
            return False

    def process_odds_item(self, cursor, item):
        try:
            # 补全博彩公司数据
            if item['bname'] not in GDict_bets:
                bid = self.addBaseItem(cursor, 'b_bets', item['bname'], '')
                if bid == -1:
                    return False
                GDict_bets[item['bname']] = bid

            item['bid'] = GDict_bets[item['bname']]

            return self.addDataItem(cursor, item)
        except Exception as e:
            print(e)
            return False

    # def close_spider(self, spider):
    #     # self.db_pool.close()
    #     # MsLog.debug('[{0}] 结束'.format(spider.name))
