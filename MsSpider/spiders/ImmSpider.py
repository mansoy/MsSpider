import scrapy
import json
import arrow
import datetime
from scrapy.selector import Selector
from ..items import ImmMatchItem
from ..items import OuOddsItem
from ..items import YaOddsItem
from ..items import DxOddsItem
from ..comm import funs


class ImmSpider(scrapy.Spider):

    name = "ImmSpider"

    custom_settings = {
		# 'MYSQL_HOST': '192.168.1.19',
        # 'MYSQL_USER': 'isoying',
        # 'MYSQL_PASSWORD': 'isoying123',        
        'MYSQL_PORT': 3306,
        'MYSQL_DB': 'zcdata',
        'ITEM_PIPELINES': {'MsSpider.pipelines.ImmPipelines.MsspiderPipeline': 300},
        'DOWNLOADER_MIDDLEWARES': {
            'MsSpider.middlewares.MsAgentMiddleware': 100,
            'MsSpider.comm.HttpProxyMiddleware.MsHttpProxyMiddleware': 110,
        }
    }

    def __init__(self, params=None, *args, **kwargs):
        super(ImmSpider, self).__init__(*args, **kwargs)
        self.params = json.loads(params)
        self.ou = self.params['ou']
        self.ya = self.params['ya']
        self.dx = self.params['dx']
        self.stime = self.params['stime']
        self.etime = self.params['etime']
        self.ftime = datetime.datetime.now()
        print('MS - 启动时间: {}'.format(str(self.ftime)))
        # option = FirefoxOptions()
        # option.headless = True
        # self.driver = webdriver.Firefox(options=option)

    def start_requests(self):
        try:
            ssDate = arrow.Arrow.now().format('YYYY-MM-DD')
            url = 'http://live.500.com/2h1.php'
            yield scrapy.Request(url=url, callback=self.parseImm, meta={'year': ssDate[:4]})
        except Exception as e:
            print('start_requests err:{0}'.format(e))

    def parseImm(self, response):
        datas = Selector(response).xpath('//tr[@fid]').extract()
        for data in datas:
            try:
                st = Selector(text=data)
                if not st:
                    continue

                item = ImmMatchItem()

                status = st.xpath('//tr/@status').extract()[0]

                # 计算比赛时间
                mDate = st.xpath('//td[@align="center"]/text()').extract()[1].replace('\xa0', ' ')
                mDate = '{0}-{1}:00'.format(response.meta['year'], mDate)
                mDate = datetime.datetime.strptime(str(mDate), '%Y-%m-%d %H:%M:%S')

                item['mdate'] = mDate.strftime('%Y-%m-%d %H:%M:%S')

                tmpNode = st.xpath('//td[@align="center"]//text()').extract()[6]
                tmpNode = tmpNode.split(' ')
                if len(tmpNode) >= 3:
                    item['bjq'] = funs.s2i(tmpNode[0])  # 半场进球数
                    item['bsq'] = funs.s2i(tmpNode[2])  # 半场输球数
                else:
                    item['bjq'] = 0
                    item['bsq'] = 0

                item['mid'] = st.xpath('//tr/@fid').extract()[0]
                item['lname'] = st.xpath('//td[@class="ssbox_01"]/a/text()').extract()[0].replace(' ', '').replace('　', '')
                item['mtname'] = st.xpath('//td[@class="p_lr01" and @align="right"]/a/text()').extract()[0].replace(' ', '')
                item['mtfname'] = ''
                item['dtname'] = st.xpath('//td[@class="p_lr01" and @align="left"]/a/text()').extract()[0].replace(' ', '')
                item['dtfname'] = ''
                item['rounds'] = st.xpath('//td/text()').extract()[0]
                item['status'] = status

                tmpNode = st.xpath('//td[@class="p_lr01" and @align="right"]/span[@class="yellowcard"]/text()').extract()
                if len(tmpNode) > 0:
                    item['mycard'] = funs.s2i(tmpNode[0])
                else:
                    item['mycard'] = 0

                tmpNode = st.xpath('//td[@class="p_lr01" and @align="right"]/span[@class="redcard"]/text()').extract()
                if len(tmpNode) > 0:
                    item['mrcard'] = funs.s2i(tmpNode[0])
                else:
                    item['mrcard'] = 0

                tmpNode = st.xpath('//td[@class="p_lr01" and @align="left"]/span[@class="yellowcard"]/text()').extract()
                if len(tmpNode) > 0:
                    item['dycard'] = funs.s2i(tmpNode[0])
                else:
                    item['dycard'] = 0

                tmpNode = st.xpath('//td[@class="p_lr01" and @align="left"]/span[@class="redcard"]/text()').extract()
                if len(tmpNode) > 0:
                    item['drcard'] = funs.s2i(tmpNode[0])
                else:
                    item['drcard'] = 0

                # 提取比赛得分
                tmpNode = st.xpath('//div[@class="pk"]/a/text()').extract()
                if len(tmpNode) >= 3:
                    item['jq'] = funs.s2i(tmpNode[0])  # 进球数
                    item['sq'] = funs.s2i(tmpNode[2])  # 失球数
                else:
                    item['jq'] = 0
                    item['sq'] = 0

                self.log('{0}-{1}'.format(item['mdate'], item['mid']))
                yield item
                # 检查明细是否需要爬取
                if status != '0':
                    continue

                if self.stime > 0 and self.etime == 0:
                    # 如果起始时间不为零,而截至时间为0, 则爬取, 从起始时间开始到最后全部的数据
                    startDate = self.ftime + datetime.timedelta(seconds=self.stime * 60)
                    if mDate < startDate:
                        continue
                elif self.stime == 0 and self.etime > 0:
                    endDate = self.ftime + datetime.timedelta(seconds=self.etime * 60)
                    if mDate > endDate:
                        continue
                elif self.stime > 0 and self.etime > 0:
                    # 如果两个都大于0, 则爬取一段时间内的数据
                    endDate = self.ftime + datetime.timedelta(seconds=self.etime * 60)
                    if mDate > endDate:
                        continue

                    startDate = self.ftime + datetime.timedelta(seconds=self.stime * 60)
                    if mDate < startDate:
                        continue

                if self.ou == 1:
                    # 欧赔页面url
                    url = 'http://odds.500.com/fenxi/ouzhi-{0}.shtml'.format(item['mid'])
                    # url = 'http://odds.500.com/fenxi1/ouzhi.php?id={0}&ctype=1&start={1}&r=1&style=0&guojia=0&chupan=1'.format(item['mid'], 0)
                    yield scrapy.Request(url=url, callback=self.parseOuOddsPages, meta={'mid': item['mid']})

                if self.ya == 1:
                    # 亚赔页面url
                    url = 'http://odds.500.com/fenxi/yazhi-{0}.shtml'.format(item['mid'])
                    yield scrapy.Request(url=url, callback=self.parseYaOddsPages, meta={'year': response.meta['year'], 'mid': item['mid']})

                if self.dx == 1:
                    # 大小指数url
                    url = 'http://odds.500.com/fenxi/daxiao-{0}.shtml'.format(item['mid'])
                    yield scrapy.Request(url=url, callback=self.parseDxOddsPages, meta={'year': response.meta['year'], 'mid': item['mid']})

            except Exception as e:
                print('parseImm err: {0}'.format(e))

    def parseOuOddsPages(self, response):
        mid = response.meta['mid']
        try:
            tmpNode = Selector(response=response).xpath('//div[@class="table_btm"]//span[@id="nowcnum"]/text()').extract()
            lyCount = funs.s2i(tmpNode[0])
            pageCount = lyCount // 30 + (1 if lyCount % 30 > 0 else 0)

            for i in range(pageCount):
                url = 'http://odds.500.com/fenxi1/ouzhi.php?id={0}&ctype=1&start={1}&r=1&style=0&guojia=0&chupan=1'.format(mid, i * 30)
                yield scrapy.Request(url=url, callback=self.parseOuOdds, meta={'mid': mid})

        except Exception as e:
            self.logger.error('[Parse Error][{0}][{1}]'.format(mid, e))

    def parseOuOdds(self, response):
        mid = response.meta['mid']
        datas = Selector(response).xpath('//tr[@ttl="zy"]').extract()
        for data in datas:
            try:
                # 提取博彩公司名称
                st = Selector(text=data)
                bname = st.xpath('//td[@class="tb_plgs"]/@title').extract()[0].replace(' ', '')

                cid = st.xpath('//tr/@id').extract()[0]
                ctime = st.xpath('//tr/@data-time').extract()[0]
                # 解析明细数据
                stimpstamp = int(arrow.now().float_timestamp * 1000)
                url = 'http://odds.500.com/fenxi1/json/ouzhi.php?_={0}&fid={1}&cid={2}&r=1&time={3}&type=europe'.format(stimpstamp, mid, cid, ctime)
                yield scrapy.Request(url=url, callback=self.parseImmOuOdds, meta={'mid': mid, 'bname': bname})
            except:
                pass

    def parseImmOuOdds(self, response):
        mid = response.meta['mid']
        bname = response.meta['bname']
        datas = json.loads(response.body)
        for data in datas:
            try:
                item = OuOddsItem()
                item['mid'] = mid
                item['bname'] = bname
                item['owin'] = funs.s2f(data[0])
                item['odraw'] = funs.s2f(data[1])
                item['olose'] = funs.s2f(data[2])
                item['retratio'] = funs.s2f(data[3])
                item['kwin'] = 0.0
                item['kdraw'] = 0.0
                item['klose'] = 0.0
                item['cdate'] = data[4]
                yield item
            except Exception as e:
                print('[parseOuOdds Error][{0}][{1}]'.format(mid, e))

    def parseYaOddsPages(self, response):
        mid = response.meta['mid']
        try:
            tmpNode = Selector(response=response).xpath('//div[@class="table_btm"]//span[@id="nowcnum"]/text()').extract()
            lyCount = funs.s2i(tmpNode[0])
            pageCount = lyCount // 30 + (1 if lyCount % 30 > 0 else 0)

            for i in range(pageCount):
                url = 'http://odds.500.com/fenxi1/yazhi.php?id={0}&ctype=1&start={1}&r=1&style=0&guojia=0'.format(mid, i * 30)
                yield scrapy.Request(url=url, callback=self.parseYaOdds, meta={'year': response.meta['year'], 'mid': mid})
        except Exception as e:
            self.logger.error('[Parse Error][{0}][{1}]'.format(mid, e))

    def parseYaOdds(self, response):
        mid = response.meta['mid']
        datas = Selector(response).xpath('//tr[@xls="row"]').extract()
        for data in datas:
            try:
                st = Selector(text=data)
                # 提取博彩公司名称
                bname = st.xpath('//span[@class="quancheng"]/text()').extract()[0]
                mmyid = st.xpath('//tr/@id').extract()[0]
                # 解析明细数据
                sTimpstamp = int(arrow.now().float_timestamp * 1000)
                url = 'http://odds.500.com/fenxi1/inc/yazhiajax.php?fid={0}&id={1}&t={2}&r=1'.format(mid, mmyid, sTimpstamp)
                yield scrapy.Request(url=url, callback=self.parseImmYaOdds, headers={'X-Requested-With': 'XMLHttpRequest'}, meta={'year': response.meta['year'], 'mid': mid, 'bname': bname})
            except Exception as e:
                print(e)

    def parseImmYaOdds(self, response):
        mid = response.meta['mid']
        bname = response.meta['bname']
        try:
            datas = json.loads(response.body)
            for data in datas:
                item = YaOddsItem()
                data = data.replace('&nbsp;', '')
                data = Selector(text=data).xpath('//td/text()').extract()
                item['mid'] = mid
                item['bname'] = bname
                item['odds1'] = funs.s2f(data[0])
                item['disc'] = data[1]
                item['odds2'] = funs.s2f(data[2])
                item['cdate'] = '{0}-{1}:00'.format(response.meta['year'], data[3])
                yield item
        except Exception as e:
            self.log('[parseImmYaOdds Error][{0}][{1}]'.format(mid, e))

    def parseDxOddsPages(self, response):
        mid = response.meta['mid']
        try:
            tmpNode = Selector(response=response).xpath('//div[@class="table_btm"]//span[@id="nowcnum"]/text()').extract()
            lyCount = funs.s2i(tmpNode[0])
            pageCount = lyCount // 30 + (1 if lyCount % 30 > 0 else 0)

            for i in range(pageCount):
                url = 'http://odds.500.com/fenxi1/daxiao.php?id={0}&ctype=1&start={1}&r=1&style=0&guojia=0'.format(mid, i * 30)
                yield scrapy.Request(url=url, callback=self.parseDxOdds, meta={'year': response.meta['year'], 'mid': mid})
        except Exception as e:
            self.logger.error('[Parse Error][{0}][{1}]'.format(mid, e))

    def parseDxOdds(self, response):
        mid = response.meta['mid']
        datas = Selector(response).xpath('//tr[@xls="row"]').extract()
        for data in datas:
            try:
                st = Selector(text=data)

                # 提取博彩公司名称
                bname = st.xpath('//span[@class="quancheng"]/text()').extract()[0]

                mmyid = st.xpath('//tr/@id').extract()[0]

                # 解析明细数据
                sTimpstamp = int(arrow.now().float_timestamp * 1000)
                url = 'http://odds.500.com/fenxi1/inc/daxiaoajax.php?fid={0}&id={1}&t={2}'.format(mid, mmyid, sTimpstamp)
                yield scrapy.Request(url=url, callback=self.parseImmDxOdds, headers={'X-Requested-With': 'XMLHttpRequest'}, meta={'year': response.meta['year'], 'mid': mid, 'bname': bname})
            except Exception as e:
                self.log('[parseDxOdds Error][{0}][{1}]'.format(mid, e))

    def parseImmDxOdds(self, response):
        mid = response.meta['mid']
        bname = response.meta['bname']
        try:
            datas = json.loads(response.body)
            for data in datas:
                item = DxOddsItem()
                data = data.replace('&nbsp;', '')
                data = Selector(text=data).xpath('//td/text()').extract()
                item['mid'] = mid
                item['bname'] = bname
                item['odds1'] = funs.s2f(data[0])
                item['disc'] = data[1]
                item['odds2'] = funs.s2f(data[2])
                item['cdate'] = '{0}-{1}:00'.format(response.meta['year'], data[3])
                yield item
        except Exception as e:
            self.log('[parseImmDxOdds Error][{0}][{1}]'.format(mid, e))