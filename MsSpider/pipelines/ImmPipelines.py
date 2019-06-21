# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


import copy
import pymysql
import time
import re
from twisted.enterprise import adbapi
from ..items import ImmMatchItem
from ..items import OuOddsItem
from ..items import YaOddsItem
from ..items import DxOddsItem
from ..comm.MsDebug import MsLog
from pymysql.err import OperationalError, InterfaceError, DataError, InternalError, IntegrityError
from ..comm import GDict_bets
from ..comm import GDict_fteam
from ..comm import GDict_league

class MsspiderPipeline(object):
    def __init__(self, db_pool):
        self.db_pool = db_pool
        # self.db_pool.cursor.execute('set autocommit=0;')
        # self.initCommit()
        self.initdata('b_bets').addCallback(self.parseData, 'b_bets')
        self.initdata('b_league').addCallback(self.parseData, 'b_league')
        self.initdata('b_fteam').addCallback(self.parseData, 'b_fteam')
        self.iCount = 0
        self.tickcount = int(round(time.time() * 1000))
        self.ou_odds_item_list = []
        self.ya_odds_item_list = []
        self.dx_odds_item_list = []
        self.immmd_item_list = []
        self.commit_limit = 100

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

    def process_item(self, item, spider):
        try:
            # 对象拷贝   深拷贝
            asynItem = copy.deepcopy(item)  # 需要导入import copy
            if isinstance(asynItem, ImmMatchItem):
                self.process_md_item(asynItem)
            elif isinstance(asynItem, OuOddsItem):
                self.process_odds_item(asynItem)
            elif isinstance(asynItem, YaOddsItem):
                self.process_odds_item(asynItem)
            elif isinstance(asynItem, DxOddsItem):
                self.process_odds_item(asynItem)
        except Exception as e:
            print('process_item err:{0}'.format(e))

    def handle_result(self, result, item_list):
        print('MS - {} items inserted with retcode {}'.format(len(item_list), result))

    def _add_immmd_data(self, tnx, item_list):
        insert_sql = '''
                        INSERT IGNORE INTO immmatchdata(`mid`, `rounds`, `lid`, `mtid`, `jq`, `dtid`, `sq`, `mdate`, `bjq`, `bsq`, 
                                                `status`, `mycard`, `mrcard`, `dycard`, `drcard`)
                        VALUES(%(mid)s, %(rounds)s, %(lid)s, %(mtid)s, %(jq)s, %(dtid)s, %(sq)s, %(mdate)s, %(bjq)s, %(bsq)s, 
                                %(status)s, %(mycard)s, %(mrcard)s, %(dycard)s, %(drcard)s)         
                    '''
        return tnx.executemany(insert_sql, item_list)

    def add_immmd_data(self, item_list):
        d = self.db_pool.runInteraction(self._add_immmd_data, item_list)
        d.addCallback(self.handle_result, item_list)

    def process_md_item(self, item):
        try:
            # 补全联赛信息
            if item['lname'] not in GDict_league:
                # self.db_pool.runInteraction().cal
                return False
            item['lid'] = GDict_league[item['lname']]

            # 补全球队信息-主队
            if item['mtname'] not in GDict_fteam:
                return False
            item['mtid'] = GDict_fteam[item['mtname']]

            # 补全球队信息-客队
            if item['dtname'] not in GDict_fteam:
                return False
            item['dtid'] = GDict_fteam[item['dtname']]

            if isinstance(item, ImmMatchItem):
                sql, value = item.get_insert_sql()
                self.immmd_item_list.append(value)
                if len(self.immmd_item_list) >= 100:  # self.commit_limit:
                    item_list = copy.deepcopy(self.immmd_item_list)
                    self.add_immmd_data(list(item_list))
                    self.immmd_item_list.clear()
            return True
        except Exception as e:
            print(e)
            return False

    def _ou_add_data(self, tnx, item_list):
        insert_sql = '''INSERT IGNORE INTO ouodds(`mid`, `bid`, `owin`, `odraw`, `olose`, `retratio`, `kwin`, `kdraw`, `klose`, `cdate`)
                        VALUES(%(mid)s, %(bid)s, %(owin)s, %(odraw)s, %(olose)s, %(retratio)s, %(kwin)s, %(kdraw)s, %(klose)s, %(cdate)s)
                     '''
        return tnx.executemany(insert_sql, item_list)

    def ou_add_data(self, item_list):
        d = self.db_pool.runInteraction(self._ou_add_data, item_list)
        d.addCallback(self.handle_result, item_list)

    def _ya_add_data(self, tnx, item_list):
        insert_sql = '''INSERT IGNORE INTO yaodds(`mid`, `bid`, `odds1`, `disc`, `odds2`, `cdate`)
                        VALUES(%(mid)s, %(bid)s, %(odds1)s, %(disc)s, %(odds2)s, %(cdate)s) '''
        return tnx.executemany(insert_sql, item_list)

    def ya_add_data(self, item_list):
        d = self.db_pool.runInteraction(self._ya_add_data, item_list)
        d.addCallback(self.handle_result, item_list)

    def _dx_add_data(self, tnx, item_list):
        insert_sql = '''INSERT IGNORE INTO dxodds(`mid`, `bid`, `odds1`, `disc`, `odds2`, `cdate`)
                        VALUES(%(mid)s, %(bid)s, %(odds1)s, %(disc)s, %(odds2)s, %(cdate)s) '''
        return tnx.executemany(insert_sql, item_list)

    def dx_add_data(self, item_list):
        d = self.db_pool.runInteraction(self._dx_add_data, item_list)
        d.addCallback(self.handle_result, item_list)

    def process_odds_item(self, item):
        try:
            # 补全博彩公司数据
            if item['bname'] not in GDict_bets:
                return False
            item['bid'] = GDict_bets[item['bname']]

            if isinstance(item, OuOddsItem):
                sql, value = item.get_insert_sql()
                self.ou_odds_item_list.append(value)
                if len(self.ou_odds_item_list) >= self.commit_limit:
                    item_list = copy.deepcopy(self.ou_odds_item_list)
                    self.ou_odds_item_list.clear()
                    self.ou_add_data(list(item_list))

            if isinstance(item, YaOddsItem):
                sql, value = item.get_insert_sql()
                self.ya_odds_item_list.append(value)
                if len(self.ya_odds_item_list) >= self.commit_limit:
                    item_list = copy.deepcopy(self.ya_odds_item_list)
                    self.ya_odds_item_list.clear()
                    self.ya_add_data(list(item_list))

            if isinstance(item, DxOddsItem):
                sql, value = item.get_insert_sql()
                self.dx_odds_item_list.append(value)
                if len(self.dx_odds_item_list) >= self.commit_limit:
                    item_list = copy.deepcopy(self.dx_odds_item_list)
                    self.dx_odds_item_list.clear()
                    self.dx_add_data(list(item_list))

            return True
        except Exception as e:
            print(e)
            return False

    def close_spider(self, spider):
        self.add_immmd_data(self.immmd_item_list)
        self.ou_add_data(self.ou_odds_item_list)
        self.ya_add_data(self.ya_odds_item_list)
        self.dx_add_data(self.dx_odds_item_list)
        self.db_pool.close()
        MsLog.debug('[{0}] 结束'.format(spider.name))
        # spider.crawler.engine.close_spider(spider, '正常结束爬虫')

    def handle_error(self, failure, item_list, retrying):
        # https://twistedmatrix.com/documents/18.7.0/api/twisted.python.failure.Failure.html
        # r = failure.trap(pymysql.err.InternalError)

        args = failure.value.args

        # <class 'pymysql.err.OperationalError'> (1045, "Access denied for user 'username'@'localhost' (using password: YES)")
        # <class 'pymysql.err.OperationalError'> (2013, 'Lost connection to MySQL server during query ([Errno 110] Connection timed out)')
        # <class 'pymysql.err.OperationalError'> (2003, "Can't connect to MySQL server on '127.0.0.1' ([WinError 10061] 由于目标计算机积极拒绝，无法连接。)")
        # <class 'pymysql.err.InterfaceError'> (0, '')    # after crawl started: sudo service mysqld stop
        if failure.type in [OperationalError, InterfaceError]:
            if not retrying:
                self.spider.logger.info('MySQL: exception {} {} \n{}'.format(
                    failure.type, args, item_list))
                self.spider.logger.debug('MySQL: Trying to recommit in %s sec' % self.mysql_reconnect_wait)

                # self._sql(item_list)
                # https://twistedmatrix.com/documents/12.1.0/core/howto/time.html
                from twisted.internet import task
                from twisted.internet import reactor
                task.deferLater(reactor, self.mysql_reconnect_wait, self._sql, item_list, True)
            else:
                self.spider.logger.warn('MySQL: exception {} {} \n{}'.format(
                    failure.type, args, item_list))

            return

        # <class 'pymysql.err.DataError'> (1264, "Out of range value for column 'position_id' at row 2")
        # <class 'pymysql.err.InternalError'> (1292, "Incorrect date value: '1977-06-31' for column 'release_day' at row 26")
        elif failure.type in [DataError, InternalError]:
            m_row = re.search(r'at\s+row\s+(\d+)$', args[1])
            row = m_row.group(1)
            item = item_list.pop(int(row) - 1)
            self.spider.logger.warn('MySQL: {} {} exception from item {}'.format(failure.type, args, item))

            self._sql(item_list)
            return

        # <class 'pymysql.err.IntegrityError'> (1048, "Column 'name' cannot be null") films 43894
        elif failure.type in [IntegrityError]:
            m_column = re.search(r"Column\s'(.+)'", args[1])
            column = m_column.group(1)
            some_items = [item for item in item_list if item[column] is None]
            self.spider.logger.warn('MySQL: {} {} exception from some items: \n{}'.format(
                failure.type, args, some_items))

            self._sql([item for item in item_list if item[column] is not None])
            return

        else:
            self.spider.logger.error('MySQL: {} {} unhandled exception from item_list: \n{}'.format(
                failure.type, args, item_list))

            return
