import copy
import pymysql
import time
from twisted.enterprise import adbapi
from .items import MatchItem
from .items import ImmMatchItem
from .comm.MsDebug import MsLog


class MsspiderPipeline(object):
    def __init__(self, db_pool):
        self.db_pool = db_pool
        # self.db_pool.cursor.execute('set autocommit=0;')
        # self.initCommit()
        self.b_bets = {}
        self.b_league = {}
        self.b_fteam = {}
        self.initdata('b_bets').addCallback(self.parseData, 'b_bets')
        self.initdata('b_league').addCallback(self.parseData, 'b_league')
        self.initdata('b_fteam').addCallback(self.parseData, 'b_fteam')
        self.iCount = 0
        self.tickcount = int(round(time.time() * 1000))
        self.ou_odds_item_list = []
        self.ya_odds_item_list = []
        self.dx_odds_item_list = []
        self.md_item_list = []
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

    def process_item(self, item, spider):
        try:
            # 对象拷贝   深拷贝
            asynItem = copy.deepcopy(item)  # 需要导入import copy

            if isinstance(asynItem, MatchItem) or isinstance(asynItem, ImmMatchItem):
                self.process_md_item(asynItem)
        except Exception as e:
            print('process_item err:{0}'.format(e))

    def _add_immmd_data(self, tnx, item_list):
        insert_sql = '''
                        INSERT INTO immmatchdata(`mid`, `rounds`, `lid`, `mtid`, `jq`, `dtid`, `sq`, `mdate`, `bjq`, `bsq`, 
                                                `status`, `mycard`, `mrcard`, `dycard`, `drcard`)
                        VALUES(%(mid)s, %(rounds)s, %(lid)s, %(mtid)s, %(jq)s, %(dtid)s, %(sq)s, %(mdate)s, %(bjq)s, %(bsq)s, 
                                %(status)s, %(mycard)s, %(mrcard)s, %(dycard)s, %(drcard)s)
                        ON DUPLICATE KEY UPDATE 
                        lid = values(lid), rounds = values(rounds), mtid=values(mtid), jq=values(jq), dtid=values(dtid),
                        sq=values(sq), mdate=values(mdate), bjq=values(bjq), bsq=values(bsq), status=values(status),
                        mycard=values(mycard), mrcard=values(mrcard), dycard=values(dycard), drcard=values(drcard)           
                    '''
        return tnx.executemany(insert_sql, item_list)

    def add_immmd_data(self, item_list):
        d = self.db_pool.runInteraction(self._add_immmd_data, item_list)
        d.addCallback(self.handle_result, item_list)

    def process_md_item(self, item):
        try:
            # 补全联赛信息
            lsid = self.b_league.get(item['lname'], -1)
            # if lsid == -1:
            #     self.addBaseItem(cursor, 'b_league', item['lname'])
            #     lsid = self.getbid(cursor, 'b_league', item['lname'])
            if lsid == -1:
                return
            self.b_league[item['lname']] = lsid
            item['lid'] = lsid

            # 补全球队信息-主队
            mtid = self.b_fteam.get(item['mtname'], -1)
            # if mtid == -1:
            #     self.addBaseItem(cursor, 'b_fteam', item['mtname'], item['mtfname'])
            #     mtid = self.getbid(cursor, 'b_fteam', item['mtname'])

            if mtid == -1:
                return
            self.b_fteam[item['mtname']] = mtid
            item['mtid'] = mtid

            # 补全球队信息-客队
            dtid = self.b_fteam.get(item['dtname'], -1)
            # if dtid == -1:
            #     self.addBaseItem(cursor, 'b_fteam', item['dtname'], item['dtfname'])
            #     dtid = self.getbid(cursor, 'b_fteam', item['dtname'])

            if dtid == -1:
                return

            self.b_fteam[item['dtname']] = dtid
            item['dtid'] = dtid

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

    def close_spider(self, spider):
        self.add_immmd_data(self.immmd_item_list)
        self.db_pool.close()
        MsLog.debug('[{0}] 结束'.format(spider.name))
