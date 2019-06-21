import pymysql
import copy
from ..items import MatchItem
from ..items import OuOddsItem
from ..items import YaOddsItem
from ..items import DxOddsItem
from ..items import ImmOuOddsItem
from ..items import ImmYaOddsItem
from ..items import ImmDxOddsItem


class DataPipeline(object):
    def __init__(self, connect, cursor):
        self.cursor = cursor
        self.connect = connect

    @classmethod
    def from_settings(cls, settings):
        connect = pymysql.connect(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DB"],
            user=settings["MYSQL_USER"],
            password=settings["MYSQL_PASSWORD"],
            charset="utf8",
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True
        )
        cursor = connect.cursor()
        return cls(connect, cursor)

    def handle_error(self, item):
        pass

    def initdata(self):
        try:
            sql = 'SELECT id FROM b_bets'
            self.cursor.execute(sql)
            self.bets = self.cursor.fetchall()
        except Exception as e:
            print(e)

    def getTableNameByItem(self, item):
        tablename = ''
        if isinstance(item, OuOddsItem):
            tablename = 'ouodds'
        elif isinstance(item, YaOddsItem):
            tablename = 'yaodds'
        elif isinstance(item, DxOddsItem):
            tablename = 'dxodds'
        elif isinstance(item, ImmOuOddsItem):
            tablename = 'immouodds'
        elif isinstance(item, ImmYaOddsItem):
            tablename = 'immyaodds'
        elif isinstance(item, ImmDxOddsItem):
            tablename = 'immdxodds'
        return tablename

    # 获取博彩公司ID
    def getbid(self, tablename, name):
        try:
            sql = 'SELECT id FROM {0} WHERE name=%(name)s'.format(tablename)
            values = {
                'name': name
            }
            self.cursor.execute(sql, values)
            data = self.cursor.fetchone()
            if data is not None:
                return data['id']
            return -1
        except Exception as e:
            print(e)

    # 添加博彩公司记录
    def addBaseItem(self, tablename, name, fname=''):
        try:
            sql = 'INSERT INTO {0}(`name`, `fname`, `remark`) VALUES(%(name)s, %(fname)s, %(remark)s)'.format(tablename)
            values = {
                'name': name,
                'fname': fname,
                'remark': ''
            }
            self.cursor.execute(sql, values)
            return True
        except Exception as e:
            print(e)
            return False

    # 获取赔率ID
    def getOddsId(self, item):
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

            self.cursor.execute(sql, values)
            data = self.cursor.fetchone()
            if data is not None:
                return data['id']
            return -1
        except Exception as e:
            print(e)

    # 添加赔率记录
    def addOddsItem(self, item):
        try:
            tablename = self.getTableNameByItem(item)
            if tablename == '':
                return False

            if tablename == 'ouodds':
                sql = '''
                        INSERT INTO ouodds(`mid`, `bid`, `owin`, `odraw`, `olose`, `retratio`, `kwin`, `kdraw`, `klose`, `cdate`)
                        VALUES(%(mid)s, %(bid)s, %(owin)s, %(odraw)s, %(olose)s, %(retratio)s, %(kwin)s, %(kdraw)s, %(klose)s, %(cdate)s)                
                    '''
                values = {
                    'mid': item['mid'],
                    'bid': item['bid'],
                    'owin': item['owin'],
                    'odraw': item['odraw'],
                    'olose': item['olose'],
                    'retratio': item['retratio'],
                    'kwin': item['kwin'],
                    'kdraw': item['kdraw'],
                    'klose': item['klose'],
                    'cdate': item['cdate']
                }
            elif tablename == 'yaodds':
                sql = '''
                        INSERT INTO yaodds(`mid`, `bid`, `odds1`, `disc`, `odds2`, `cdate`)
                        VALUES(%(mid)s, %(bid)s, %(odds1)s, %(disc)s, %(odds2)s, %(cdate)s)                
                    '''
                values = {
                    'mid': item['mid'],
                    'bid': item['bid'],
                    'odds1': item['odds1'],
                    'disc': item['disc'],
                    'odds2': item['odds2'],
                    'cdate': item['cdate']
                }
            elif tablename == 'dxodds':
                sql = '''
                        INSERT INTO dxodds(`mid`, `bid`, `odds1`, `disc`, `odds2`, `cdate`)
                        VALUES(%(mid)s, %(bid)s, %(odds1)s, %(disc)s, %(odds2)s, %(cdate)s)                
                    '''
                values = {
                    'mid': item['mid'],
                    'bid': item['bid'],
                    'odds1': item['odds1'],
                    'disc': item['disc'],
                    'odds2': item['odds2'],
                    'cdate': item['cdate']
                }

            self.cursor.execute(sql, values)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)
            return False

    # 更新赔率记录
    def updOddsItem(self, item):
        try:
            tablename = self.getTableNameByItem(item)
            if tablename == '':
                return False

            if tablename == 'ouodds':
                sql = '''
                        UPDATE ouodds
                        SET `mid`=%(mid)s, `bid`=%(bid)s, `owin`=%(owin)s, `odraw`=%(odraw)s, `olose`=%(olose)s, `retratio`=%(retratio)s,
                            `kwin`=%(kwin)s, `kdraw`=%(kdraw)s, `klose`=%(klose)s, `cdate`=%(cdate)s
                        WHERE id=%(id)s         
                    '''
                values = {
                    'id': item['id'],
                    'mid': item['mid'],
                    'bid': item['bid'],
                    'owin': item['owin'],
                    'odraw': item['odraw'],
                    'olose': item['olose'],
                    'retratio': item['retratio'],
                    'kwin': item['kwin'],
                    'kdraw': item['kdraw'],
                    'klose': item['klose'],
                    'cdate': item['cdate']
                }
            elif tablename == 'yaodds':
                sql = '''
                        UPDATE yaodds
                        SET `mid`=%(mid)s, `bid`=%(bid)s, `odds1`=%(odds1)s, `disc`=%(disc)s, `odds2`=%(odds2)s, `cdate`=%(cdate)s
                        WHERE id=%(id)s              
                    '''
                values = {
                    'id': item['id'],
                    'mid': item['mid'],
                    'bid': item['bid'],
                    'odds1': item['odds1'],
                    'disc': item['disc'],
                    'odds2': item['odds2'],
                    'cdate': item['cdate']
                }
            elif tablename == 'dxodds':
                sql = '''
                        UPDATE dxodds
                        SET `mid`=%(mid)s, `bid`=%(bid)s, `odds1`=%(odds1)s, `disc`=%(disc)s, `odds2`=%(odds2)s, `cdate`=%(cdate)s
                        WHERE id=%(id)s              
                    '''
                values = {
                    'id': item['id'],
                    'mid': item['mid'],
                    'bid': item['bid'],
                    'odds1': item['odds1'],
                    'disc': item['disc'],
                    'odds2': item['odds2'],
                    'cdate': item['cdate']
                }

            self.cursor.execute(sql, values)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)
            return False

    # 获取比赛ID
    def getmid(self, mid):
        try:
            sql = 'SELECT id FROM matchdata WHERE mid=%(mid)s'
            values = {
                'mid': mid
            }
            self.cursor.execute(sql, values)
            data = self.cursor.fetchone()
            if data is not None:
                return data['id']
            return -1
        except Exception as e:
            print(e)

    # 添加比赛记录
    def addmItem(self, item):
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
            self.cursor.execute(sql, values)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)
            return False

    # 更新比赛记录
    def updmItem(self, item):
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
            self.cursor.execute(sql, values)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)
            return False

    def process_item(self, item, spider):
        try:
            # 对象拷贝   深拷贝
            asynItem = copy.deepcopy(item)  # 需要导入import copy

            if isinstance(asynItem, MatchItem):
                self.process_md_item( asynItem)
            elif isinstance(asynItem, OuOddsItem) or isinstance(asynItem, YaOddsItem) or isinstance(asynItem, DxOddsItem):
                self.process_odds_item(asynItem)
            return item
        except Exception as e:
            print('process_item err:{0}'.format(e))

    def process_md_item(self, item):
        try:
            # 补全联赛信息
            lsid = self.getbid('b_league', item['lname'])
            if lsid == -1:
                self.addBaseItem('b_league', item['lname'])
                lsid = self.getbid('b_league', item['lname'])
            if lsid == -1:
                return
            item['lid'] = lsid

            # 补全球队信息-主队
            mtid = self.getbid('b_fteam', item['mtname'])
            if mtid == -1:
                self.addBaseItem('b_fteam', item['mtname'], item['mtfname'])
                mtid = self.getbid('b_fteam', item['mtname'])

            if mtid == -1:
                return
            item['mtid'] = mtid

            # 补全球队信息-客队
            dtid = self.getbid('b_fteam', item['dtname'])
            if dtid == -1:
                self.addBaseItem('b_fteam', item['dtname'], item['dtfname'])
                dtid = self.getbid('b_fteam', item['dtname'])

            if dtid == -1:
                return
            item['dtid'] = dtid

            # 添加或者更新比赛表数据
            imid = self.getmid(item['mid'])
            if imid == -1:
                self.addmItem(item)
            else:
                item['id'] = imid
                return self.updmItem(item)
            return True
        except Exception as e:
            print(e)
            return False

    def process_odds_item(self, item):
        try:
            # 补全博彩公司数据
            bid = self.getbid('b_bets', item['bname'])
            if bid == -1:
                self.addBaseItem('b_bets', item['bname'], '')
                bid = self.getbid('b_bets', item['bname'])
            if bid == -1:
                return False
            item['bid'] = bid

            # # 先检查记录是否已经存在
            # id = self.getOddsId(item)
            # if id == -2:
            #     return False
            # if id == -1:
            #     return self.addOddsItem(item)
            # else:
            #     item['id'] = id
            #     return self.updOddsItem(cursor, item)
            return True
        except Exception as e:
            print(e)
            return False

    def close_spider(self, spider):
        self.connect.close()
