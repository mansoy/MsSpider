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
from .items import MatchItem
from .items import OddsItem
from .items import OuOddsItem
from .items import YaOddsItem
from .items import DxOddsItem
from .items import ImmOuOddsItem
from .items import ImmYaOddsItem
from .items import ImmDxOddsItem
from .comm.MsDebug import MsLog


class MsPipeline(object):
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.b_bets = {}
        self.b_league = {}
        self.b_fteam = {}
        self.initdata('b_bets').addCallback(self.parseData, 'b_bets')
        self.initdata('b_league').addCallback(self.parseData, 'b_league')
        self.initdata('b_fteam').addCallback(self.parseData, 'b_fteam')
        self.iCount = 0
        self.tickcount = int(round(time.time() * 1000))
        self.odds_item_list = []

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
                use_unicode=True,
                autocommit=0
        )
        return cls(db_pool)

    def parseData(self, datas, table):
        print("MS - printData[{0}]...".format(table))
        for data in datas:
            if table == 'b_bets':
                self.b_bets[data['name']] = data['id']
            elif table == 'b_league':
                self.b_league[data['name']] = data['id']
            elif table == 'b_fteam':
                self.b_fteam[data['name']] = data['id']

    def _initCommit(self, cur):
        print('MS - set autocommit')
        cur.execute('set autocommit=0;')

    def initCommit(self):
        return self.db_pool.runInteraction(self._initCommit)

    def initdata(self, table):
        print('MS - initdata.....')
        return self.db_pool.runQuery("select * from {0}".format(table))

    def handle_error(self, failure, item):
        print('插入数据失败，原因：{}，错误对象：{}'.format(failure, item))

    # 获取博彩公司ID
    def getbid(self, cur, tablename, name):
        try:
            sql = 'SELECT id FROM {0} WHERE name=%(name)s'.format(tablename)
            values = {
                'name': name
            }
            cur.execute(sql, values)
            data = cur.fetchone()
            if data is not None:
                return data['id']
            return -1
        except Exception as e:
            print(e)

    # 添加博彩公司记录
    def addBaseItem(self, cur, tablename, name, fname=''):
        try:
            sql = 'INSERT INTO {0}(`name`, `fname`, `remark`) VALUES(%(name)s, %(fname)s, %(remark)s)'.format(tablename)
            values = {
                'name': name,
                'fname': fname,
                'remark': ''
            }
            cur.execute(sql, values)
            return True
        except Exception as e:
            print(e)
            return False

    # 获取赔率ID
    def getOddsId(self, cur, item):
        try:
            tablename = self.getTableNameByItem(item)

            if tablename == '':
                return -2

            sql = '''
                SELECT id FROM {0} WHERE mid=%(mid)s and bid=%(bid)s and cdate=%(cdate)s
                '''.format(tablename)
            values = {
                'mid': item['mid'],
                'bid': item['bid'],
                'cdate': item['cdate']
            }

            cur.execute(sql, values)
            data = cur.fetchone()
            if data is not None:
                return data['id']
            return -1
        except Exception as e:
            print(e)

    # 获取比赛ID
    def getmid(self, cur, mid):
        try:
            sql = 'SELECT id FROM matchdata WHERE mid=%(mid)s'
            values = {
                'mid': mid
            }
            cur.execute(sql, values)
            data = cur.fetchone()
            if data is not None:
                return data['id']
            return -1
        except Exception as e:
            print(e)

    # 添加比赛记录
    def addmItem(self, cur, item):
        try:
            sql = '''
                    INSERT INTO matchdata(`mid`, `lid`, `mtid`, `jq`, `dtid`, `sq`, `mdate`)
                    VALUES(%(mid)s, %(lid)s, %(mtid)s, %(jq)s, %(dtid)s, %(sq)s, %(mdate)s)                
                '''

            values = {
                'mid': item['mid'],
                'lid': item['lid'],
                'mtid': item['mtid'],
                'jq': item['jq'],
                'dtid': item['dtid'],
                'sq': item['sq'],
                'mdate': item['mdate']
            }
            cur.execute(sql, values)
            return True
        except Exception as e:
            print(e)
            return False

    # 更新比赛记录
    def updmItem(self, cur, item):
        try:
            sql = '''
                    UPDATE matchdata
                    SET `mid`=%(mid)s, `lid`=%(lid)s, `mtid`=%(mtid)s, `jq`=%(jq)s, 
                        `dtid`=%(dtid)s, `sq`=%(sq)s, `mdate`=%(mdate)s
                    WHERE `id` = %(id)s                 
                '''

            values = {
                'id': item['id'],
                'mid': item['mid'],
                'lid': item['lid'],
                'mtid': item['mtid'],
                'jq': item['jq'],
                'dtid': item['dtid'],
                'sq': item['sq'],
                'mdate': item['mdate']
            }
            cur.execute(sql, values)
            return True
        except Exception as e:
            print(e)
            return False

    def process_item(self, items, spider):
        try:
            # 对象拷贝   深拷贝
            asynItems = copy.deepcopy(items)  # 需要导入import copy

            if isinstance(asynItems, MatchItem):
                query = self.db_pool.runInteraction(self.process_md_item, asynItems)
            elif isinstance(asynItems, OuOddsItem) or isinstance(asynItems, YaOddsItem) or isinstance(asynItems, DxOddsItem):
                query = self.db_pool.runInteraction(self.process_odds_item, asynItems)
            elif isinstance(asynItems, OddsItem):
                query = self.db_pool.runInteraction(self.insert_odds, asynItems)
            query.addErrback(self.handle_error, asynItems)
            # return item
            # print('MS - iCount: {} TimeCount: {}'.format(self.iCount, int(round(time.time() * 1000)) - self.tickcount))
            self.iCount += 1
        except Exception as e:
            print('process_item err:{0}'.format(e))

    def process_md_item(self, cursor, item):
        try:
            # 补全联赛信息
            # lsid = self.getbid(cursor, 'b_league', item['lname'])
            lsid = self.b_league.get(item['lname'], -1)
            if lsid == -1:
                self.addBaseItem(cursor, 'b_league', item['lname'])
                lsid = self.getbid(cursor, 'b_league', item['lname'])
            if lsid == -1:
                return
            self.b_league[item['lname']] = lsid
            item['lid'] = lsid

            # 补全球队信息-主队
            # mtid = self.getbid(cursor, 'b_fteam', item['mtname'])
            mtid = self.b_fteam.get(item['mtname'], -1)
            if mtid == -1:
                self.addBaseItem(cursor, 'b_fteam', item['mtname'], item['mtfname'])
                mtid = self.getbid(cursor, 'b_fteam', item['mtname'])

            if mtid == -1:
                return
            self.b_fteam[item['mtname']] = mtid
            item['mtid'] = mtid

            # 补全球队信息-客队
            # dtid = self.getbid(cursor, 'b_fteam', item['dtname'])
            dtid = self.b_fteam.get(item['dtname'], -1)
            if dtid == -1:
                self.addBaseItem(cursor, 'b_fteam', item['dtname'], item['dtfname'])
                dtid = self.getbid(cursor, 'b_fteam', item['dtname'])

            if dtid == -1:
                return

            self.b_fteam[item['dtname']] = dtid
            item['dtid'] = dtid

            # 添加或者更新比赛表数据
            imid = self.getmid(cursor, item['mid'])
            if imid == -1:
                self.addmItem(cursor, item)
            else:
                item['id'] = imid
                return self.updmItem(cursor, item)
            return True
        except Exception as e:
            print(e)
            return False

    def process_odds_item(self, cursor, item):
        try:
            # 补全博彩公司数据
            bid = self.b_bets.get(item['bname'], -1)
            if bid == -1:
                self.addBaseItem(cursor, 'b_bets', item['bname'], '')
                bid = self.getbid(cursor, 'b_bets', item['bname'])
            if bid == -1:
                return False
            self.b_bets[item['bname']] = bid
            item['bid'] = bid

            return self.addOddsItem(cursor, item)
            return True
        except Exception as e:
            print(e)
            return False

    def insert_odds(self, cursor, item):
        try:
            # 补全博彩公司数据
            bid = self.b_bets.get(item['bname'], -1)
            if bid == -1:
                self.addBaseItem(cursor, 'b_bets', item['bname'], '')
                bid = self.getbid(cursor, 'b_bets', item['bname'])
            if bid == -1:
                return False
            self.b_bets[item['bname']] = bid
            item['bid'] = bid

            insert_sql = item.get_insert_sql()
            cursor.execute(insert_sql)
            return True
        except Exception as e:
            print(e)
            return False

    def close_spider(self, spider):
        self.db_pool.close()
        MsLog.debug('[{0}] 结束'.format(spider.name))
