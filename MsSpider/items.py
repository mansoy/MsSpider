# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import json


class MsspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


# 国家Item
class CountryItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    fname = scrapy.Field()
    remark = scrapy.Field()

    def get_insert_sql(self):
        sql = '''
                insert into b_country(`name`, `fname`)
                values(%(name)s, %(fname)s)
            '''
        values = {
            'name': self['name'],
            'fname': self['fname']
        }
        return sql, values

    def get_update_sql(self):
        sql = '''
                update b_country
                set `name` = %(name)s,
                    `fname` = %(fname)s
                where `id` = %(id)s
            '''
        values = {
            'id': self['id'],
            'name': self['name'],
            'fname': self['fname']
        }
        return sql, values


# 赛季表Item
class SeasonItem(scrapy.Item):
    id = scrapy.Field()
    lid = scrapy.Field()
    lname = scrapy.Field()
    name = scrapy.Field()
    ssid = scrapy.Field()

    def get_insert_sql(self):
        sql = '''
                insert into b_season(`name`, `lid`, `ssid`)
                values(%(name)s, %(lid)s, %(ssid)s)
            '''
        values = {
            'name': self['name'],
            'lid': self['lid'],
            'ssid': self['ssid']
        }
        return sql, values

    def get_update_sql(self):
        sql = '''
                update b_country
                set `name` = %(name)s,
                    `lid` = %(lid)s,
                    `ssid` = %(ssid)s
                where `id` = %(id)s
            '''
        values = {
            'id': self['id'],
            'name': self['name'],
            'lid': self['lid'],
            'ssid': self['ssid']
        }
        return sql, values


# 博彩公司
class BetsItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    fname = scrapy.Field()
    remark = scrapy.Field()

    def get_insert_sql(self):
        sql = '''
                    insert IGNORE into b_fteam(`name`, `fname`, `remark`)
                    values(%(name)s, %(fname)s, %(remark)s)
                '''
        values = {
            'name': self['name'],
            'fname': self['fname'],
            'remark': self['remark']
        }
        return sql, values

    def get_update_sql(self):
        sql = '''
                    update b_fteam
                    set `name` = %(name)s,
                        `fname` = %(fname)s, 
                        `remark` = %(remark)s
                    where `id` = %(id)s
                '''
        values = {
            'id': self['id'],
            'name': self['name'],
            'fname': self['fname'],
            'remark': self['remark']
        }
        return sql, values


# 基础表Item
class TeamItem(scrapy.Item):
    id = scrapy.Field()
    cls = scrapy.Field()
    name = scrapy.Field()
    fname = scrapy.Field()
    sid = scrapy.Field()
    imgname = scrapy.Field()
    imgurl = scrapy.Field()
    remark = scrapy.Field()

    def get_insert_sql(self):
        sql = '''
                insert IGNORE into b_fteam(`name`, `fname`, `sid`)
                values(%(name)s, %(fname)s, %(sid)s)
            '''
        values = {
            'name': self['name'],
            'fname': self['fname'],
            'sid': self['sid']
        }
        return sql, values

    def get_update_sql(self):
        sql = '''
                update b_fteam
                set `name` = %(name)s,
                    `fname` = %(fname)s, 
                    `sid` = %(sid)s
                where `id` = %(id)s
            '''
        values = {
            'id': self['id'],
            'name': self['name'],
            'fname': self['fname'],
            'sid': self['sid']
        }
        return sql, values


# 联赛Item
class LeagueItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    fname = scrapy.Field()
    color = scrapy.Field()
    remark = scrapy.Field()

    def get_insert_sql(self):
        sql = '''
                insert into b_league(`name`, `fname`, `sid`, `color`)
                values(%(name)s, %(fname)s, %(sid)s, %(color)s)
            '''
        values = {
            'name': self['name'],
            'fname': self['fname'],
            'sid': self['sid'],
            'color': self['color']
        }
        return sql, values

    def get_update_sql(self):
        sql = '''
                update b_league
                set `name` = %(name)s,
                    `fname` = %(fname)s, 
                    `color` = %(color)s
                where `id` = %(id)s
            '''
        values = {
            'id': self['id'],
            'name': self['name'],
            'fname': self['fname'],
            'color': self['color']
        }
        return sql, values


# 比赛Item
class MatchItem(scrapy.Item):
    id = scrapy.Field()
    # 比赛场次编号
    mid = scrapy.Field()
    # 联赛名称
    lid = scrapy.Field()
    lname = scrapy.Field()
    # 主队名称
    mtid = scrapy.Field()
    mtname = scrapy.Field()
    mtfname = scrapy.Field()
    # 客队名称
    dtid = scrapy.Field()
    dtname = scrapy.Field()
    dtfname = scrapy.Field()
    # 主队进球
    jq = scrapy.Field()
    # 客队进球
    sq = scrapy.Field()
    # 比赛日期
    mdate = scrapy.Field()
    # 状态
    status = scrapy.Field()

    def get_insert_sql(self):
        sql = '''
                    INSERT IGNORE INTO matchdata(`mid`, `lid`, `mtid`, `jq`, `dtid`, `sq`, `mdate`)
                    VALUES(%(mid)s, %(lid)s, %(mtid)s, %(jq)s, %(dtid)s, %(sq)s, %(mdate)s)             
                '''

        values = {
            'mid': self['mid'],
            'lid': self['lid'],
            'mtid': self['mtid'],
            'jq': self['jq'],
            'dtid': self['dtid'],
            'sq': self['sq'],
            'mdate': self['mdate']
        }
        return sql, values


# 比赛Item
class ImmMatchItem(scrapy.Item):
    rounds = scrapy.Field()
    bjq = scrapy.Field()
    bsq = scrapy.Field()

    id = scrapy.Field()
    # 比赛场次编号
    mid = scrapy.Field()
    # 联赛名称
    lid = scrapy.Field()
    lname = scrapy.Field()
    # 主队名称
    mtid = scrapy.Field()
    mtname = scrapy.Field()
    mtfname = scrapy.Field()
    # 客队名称
    dtid = scrapy.Field()
    dtname = scrapy.Field()
    dtfname = scrapy.Field()
    # 主队进球
    jq = scrapy.Field()
    # 客队进球
    sq = scrapy.Field()
    # 比赛日期
    mdate = scrapy.Field()
    # 状态
    status = scrapy.Field()
    # 红牌,黄牌数据
    mycard = scrapy.Field()
    mrcard = scrapy.Field()
    dycard = scrapy.Field()
    drcard = scrapy.Field()

    def get_insert_sql(self):
        sql = '''
                INSERT INTO immmatchdata(`mid`, `rounds`, `lid`, `mtid`, `jq`, `dtid`, `sq`, `mdate`, `bjq`, `bsq`, `status`)
                VALUES(%(mid)s, %(rounds)s, %(lid)s, %(mtid)s, %(jq)s, %(dtid)s, %(sq)s, %(mdate)s, %(bjq)s, %(bsq)s, %(status)s) 
                ON DUPLICATE KEY UPDATE  
                 lid = values(lid), rounds = values(rounds), mtid=values(mtid), jq=values(jq), dtid=values(dtid),
                sq=values(sq), mdate=values(mdate), bjq=values(bjq), bsq=values(bsq), status=values(status)           
            '''

        values = {
            'mid': self['mid'],
            'lid': self['lid'],
            'mtid': self['mtid'],
            'jq': self['jq'],
            'dtid': self['dtid'],
            'sq': self['sq'],
            'mdate': self['mdate'].replace('\xa0', ' '),
            'rounds': self['rounds'],
            'bjq': self['bjq'],
            'bsq': self['bsq'],
            'status': self['status'],
            'mycard': self['mycard'],
            'mrcard': self['mrcard'],
            'dycard': self['dycard'],
            'drcard': self['drcard']
        }
        return sql, values


# 欧赔Item
class OuOddsItem(scrapy.Item):
    id = scrapy.Field()
    # 比赛编号
    mid = scrapy.Field()
    # 博彩公司
    bid = scrapy.Field()
    bname = scrapy.Field()
    # 即时欧赔
    owin = scrapy.Field()
    odraw = scrapy.Field()
    olose = scrapy.Field()
    # 返还率
    retratio = scrapy.Field()
    # 即时凯利
    kwin = scrapy.Field()
    kdraw = scrapy.Field()
    klose = scrapy.Field()
    # 变化时间
    cdate = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = '''
                        INSERT IGNORE INTO ouodds(`mid`, `bid`, `owin`, `odraw`, `olose`, `retratio`, `kwin`, `kdraw`, `klose`, `cdate`)
                        VALUES(%(mid)s, %(bid)s, %(owin)s, %(odraw)s, %(olose)s, %(retratio)s, %(kwin)s, %(kdraw)s, %(klose)s, %(cdate)s)                
                    '''
        values = {
            'mid': self['mid'],
            'bid': self['bid'],
            'owin': self['owin'],
            'odraw': self['odraw'],
            'olose': self['olose'],
            'retratio': self['retratio'],
            'kwin': self['kwin'],
            'kdraw': self['kdraw'],
            'klose': self['klose'],
            'cdate': self['cdate']
        }
        return insert_sql, values


# 亚赔Item
class YaOddsItem(scrapy.Item):
    id = scrapy.Field()
    # 比赛编号
    mid = scrapy.Field()
    # 博彩公司
    bid = scrapy.Field()
    bname = scrapy.Field()
    #
    odds1 = scrapy.Field()
    disc = scrapy.Field()
    odds2 = scrapy.Field()
    # 变化时间
    cdate = scrapy.Field()

    def get_insert_sql(self):
        # yaodds
        insert_sql = '''INSERT IGNORE INTO yaodds(`mid`, `bid`, `odds1`, `disc`, `odds2`, `cdate`)
                        VALUES(%(mid)s, %(bid)s, %(odds1)s, %(disc)s, %(odds2)s, %(cdate)s) '''
        values = {
            'mid': self['mid'],
            'bid': self['bid'],
            'odds1': self['odds1'],
            'disc': self['disc'],
            'odds2': self['odds2'],
            'cdate': self['cdate']
        }

        return insert_sql, values


# 大小指数Item
class DxOddsItem(scrapy.Item):
    id = scrapy.Field()
    # 比赛编号
    mid = scrapy.Field()
    # 博彩公司
    bid = scrapy.Field()
    bname = scrapy.Field()
    #
    odds1 = scrapy.Field()
    disc = scrapy.Field()
    odds2 = scrapy.Field()
    # 变化时间
    cdate = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = '''INSERT IGNORE INTO dxodds(`mid`, `bid`, `odds1`, `disc`, `odds2`, `cdate`)
                        VALUES(%(mid)s, %(bid)s, %(odds1)s, %(disc)s, %(odds2)s, %(cdate)s) '''
        values = {
            'mid': self['mid'],
            'bid': self['bid'],
            'odds1': self['odds1'],
            'disc': self['disc'],
            'odds2': self['odds2'],
            'cdate': self['cdate']
        }

        return insert_sql, values


# 欧赔Item
class ImmOuOddsItem(OuOddsItem):
    pass


# 亚赔Item
class ImmYaOddsItem(YaOddsItem):
    pass


class ImmDxOddsItem(DxOddsItem):
    pass


class OddsItem(scrapy.Item):
    # 标记为那种赔率
    cls = scrapy.Field()
    # 比赛编号
    mid = scrapy.Field()
    # 博彩公司
    bid = scrapy.Field()
    bname = scrapy.Field()
    datas = scrapy.Field(serializer=str)

    def get_insert_sql(self):
        insert_sql = ''
        if len(self['datas']) == 0:
            return

        if self['cls'] == 'OU':
            insert_sql = '''INSERT IGNORE INTO ouodds(`mid`, `bid`, `owin`, `odraw`, `olose`, `retratio`, `kwin`, `kdraw`, `klose`, `cdate`)'''
            sValues = ''
            for data in self['datas']:
                if len(sValues) > 0:
                    sValues += ','

                ss = '''({0},{1},{2},{3},{4},{5},{6},{7},{8},'{9}')'''.format(
                    self['mid'],
                    self['bid'],
                    data['owin'],
                    data['odraw'],
                    data['olose'],
                    data['retratio'],
                    data['kwin'],
                    data['kdraw'],
                    data['klose'],
                    data['cdate']
                )

                sValues += ss

            insert_sql += ' values '
            insert_sql += sValues
        elif self['cls'] == 'YA':
            insert_sql = '''INSERT IGNORE INTO yaodds(`mid`, `bid`, `odds1`, `disc`, `odds2`, `cdate`)'''
            sValues = ''
            for data in self['datas']:
                if len(sValues) > 0:
                    sValues += ','

                ss = '''({0},{1},{2},'{3}',{4},'{5}')'''.format(
                    self['mid'],
                    self['bid'],
                    data['odds1'],
                    data['disc'],
                    data['odds2'],
                    data['cdate']
                )

                sValues += ss
            insert_sql += ' values '
            insert_sql += sValues
        elif self['cls'] == 'DX':
            insert_sql = '''INSERT IGNORE INTO dxodds(`mid`, `bid`, `odds1`, `disc`, `odds2`, `cdate`)'''
            sValues = ''
            for data in self['datas']:
                if len(sValues) > 0:
                    sValues += ','

                ss = '''({0},{1},{2},'{3}',{4},'{5}')'''.format(
                    self['mid'],
                    self['bid'],
                    data['odds1'],
                    data['disc'],
                    data['odds2'],
                    data['cdate']
                )

                sValues += ss
            insert_sql += ' values '
            insert_sql += sValues

        return insert_sql