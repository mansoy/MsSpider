import pymysql


class OpData():
    def __init__(self, host, user, pwd, database):
        # 连接数据库
        self.db = pymysql.connect(host=host,
                                  user=user,
                                  password=pwd,
                                  db=database,
                                  charset="utf8",
                                  cursorclass=pymysql.cursors.DictCursor,
                                  use_unicode=True
                                  )
        # 使用cursor()方法创建一个游标对象
        self.cursor = self.db.cursor()

    def query(self, sql):
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        return data

    def __del__(self):
        self.cursor.close()
        self.db.close()
        print('QData is freed')


if __name__ == '__main__':
    qdata = OpData(host="rm-m5eyk861d8408u3ix1o.mysql.rds.aliyuncs.com",
                  user="root",
                  pwd="Tingxue_147258369",
                  database="zcdata")

    sql = '''
        select mid,
            case when oumid is null then 0 else 1 end as ou,
            case when yamid is null then 0 else 1 end as ya,
            case when dxmid is null then 0 else 1 end as dx
        from matchdata as a left join
        (
            select distinct mid as oumid from ouodds
        ) as b on a.mid = b.oumid left join
        (
            select distinct mid as yamid from yaodds
        ) as c on a.mid = c.yamid left join
        (
            select distinct mid as dxmid from dxodds
        ) as d on a.mid = d.dxmid
    '''

    datas = qdata.query(sql)
    for data in datas:
        print(data)

