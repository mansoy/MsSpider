import scrapy
import json
import re
from scrapy.selector import Selector
from ..items import CountryItem
from ..items import SeasonItem
from ..comm import funs


class LeagueDataSpider(scrapy.Spider):

    name = "LeagueDataSpider"

    custom_settings = {
        # 'MYSQL_HOST': 'rm-m5eyk861d8408u3ix.mysql.rds.aliyuncs.com',
        'MYSQL_HOST': 'rm-m5eyk861d8408u3ix1o.mysql.rds.aliyuncs.com',
        'MYSQL_USER': 'root',
        'MYSQL_PASSWORD': 'Tingxue_147258369',
        # 'MYSQL_HOST': '192.168.1.19',
        # 'MYSQL_USER': 'isoying',
        # 'MYSQL_PASSWORD': 'isoying123',
        'MYSQL_PORT': 3306,
        'MYSQL_DB': 'zcdata',
        'ITEM_PIPELINES': {
            'MsSpider.pipelines.LeagueDataPipeline.LeagueDataPipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            'MsSpider.middlewares.MsAgentMiddleware': 100,
            'MsSpider.comm.HttpProxyMiddleware.MsHttpProxyMiddleware': 110,
        }
    }

    def __init__(self, params=None, *args, **kwargs):
        super(LeagueDataSpider, self).__init__(*args, **kwargs)
        self.params = json.loads(params)
        self.cmd = self.params['cmd']

    def start_requests(self):
        try:
            url = 'http://liansai.500.com/'
            yield scrapy.Request(url=url, callback=self.parse)
        except Exception as e:
            print('start_requests err:{0}'.format(e))

    def parse(self, response):
        def parse_country(text):
            citems = []
            datas = Selector(text=text).xpath('//li').extract()
            for data in datas:
                cname = Selector(text=data).xpath('//a/span/text()').extract()[0]
                item = CountryItem()
                item['name'] = cname
                item['fname'] = ''
                item['remark'] = ''
                ldatas = Selector(text=data).xpath('//div/a').extract()
                for ldata in ldatas:
                    se = Selector(text=ldata)
                    href = se.xpath('//a/@href').extract()[0]
                    name = se.xpath('//a/text()').extract()[0]
                    fname = se.xpath('//a/@title').extract()[0]
                    print('href:{}  name:{} fname:{}'.format(href, name, fname))

                print(item)
                citems.append(item)
            return citems

        if self.cmd == 0:
            lareas = Selector(response).xpath('//div[@class="lallrace_main"]').extract()
            for i in range(len(lareas)):
                try:
                    if i == 0:
                        continue
                    # 解析国家数据
                    # items = parse_country(lareas[i])
                    # for item in items:
                    #     yield item

                except Exception as e:
                    print('parseImm err: {0}'.format(e))
        elif self.cmd == 1:
            leagues = []
            lareas = Selector(response).xpath('//div[@class="lallrace_main"]').extract()
            for i in range(len(lareas)):
                html = lareas[i]
                if i == 0:
                    datas = Selector(text=html).xpath('//li/a').extract()
                    for data in datas:
                        league = {}
                        se = Selector(text=data)
                        league['href'] = se.xpath('//a/@href').extract()[0]
                        league['name'] = se.xpath('//a/span/text()').extract()[0].replace(' ', '')
                        league['fname'] = ''
                        leagues.append(league)
                else:
                    datas = Selector(text=html).xpath('//div/a').extract()
                    for data in datas:
                        league = {}
                        se = Selector(text=data)
                        league['href'] = se.xpath('//a/@href').extract()[0]
                        league['name'] = se.xpath('//a/text()').extract()[0].replace(' ', '')
                        league['fname'] = se.xpath('//a/@title').extract()[0].replace(' ', '')
                        leagues.append(league)

                # 先奖所有联赛信息拿出来, 然后再一个去处理
                for league in leagues:
                    league['ssid'] = re.findall('[0-9]+', league['href'])[0]
                    yield scrapy.Request(url='http://liansai.500.com/zuqiu-{0}/'.format(league['ssid']), callback=self.get_season_addr, meta={'league': league})

    def get_season_addr(self, response):
        league = response.meta['league']
        ssname = Selector(response).xpath('//a[@class="ldrop_tit"]/span/text()').extract()[0]
        league['ssname'] = ssname

        item = SeasonItem()
        item['lname'] = league['name']
        item['name'] = league['ssname']
        item['ssid'] = league['ssid']
        yield item
        # # 解析当前赛季对应的比赛数据
        # url = Selector(response).xpath('//ul[@class="lpage_race_nav clearfix"]/li/a/@href').extract()[1]
        # if url:
        #     url = 'http://liansai.500.com{0}'.format(url)
        #     yield scrapy.Request(url=url, callback=self.parse_season, meta={'league': league})

    def parse_season(self, response):
        league = response.meta['league']
        # ssname = Selector(response).xpath('//a[@class="ldrop_tit"]/span/text()').extract()[0]
        print(response.url)
        pass
