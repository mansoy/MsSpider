import scrapy
import json
import arrow
import datetime
import re
from scrapy.selector import Selector
from ..items import TeamItem
from ..items import LeagueItem


class BaseDataSpider(scrapy.Spider):

    name = "BaseDataSpider"

    custom_settings = {
        'MYSQL_HOST': '192.168.1.19',
        'MYSQL_USER': 'isoying',
        'MYSQL_PASSWORD': 'isoying123',
        'MYSQL_PORT': 3306,
        'MYSQL_DB': 'zcdata',
        'DOWNLOAD_DELAY': 1,
        'IMAGES_STORE': 'D:\\ImgStore\\',
        'ITEM_PIPELINES': {
            'MsSpider.pipelines.base_pipelines.BaseImgPipeline': 200,
            'MsSpider.pipelines.base_pipelines.BasePipeline': 300
        },
        # 'ITEM_PIPELINES': {'MsSpider.pipelines.MsspiderPipeline': 300},
        'DOWNLOADER_MIDDLEWARES': {
            'MsSpider.middlewares.MsAgentMiddleware': 100,
            # 'MsSpider.comm.HttpProxyMiddleware.MsHttpProxyMiddleware': 110,
        }
    }

    def __init__(self, params=None, *args, **kwargs):
        super(BaseDataSpider, self).__init__(*args, **kwargs)
        self.params = json.loads(params)
        self.ou = self.params['ou']
        self.ya = self.params['ya']
        self.dx = self.params['dx']
        self.sdate = arrow.get(self.params['sDate'])
        self.edate = arrow.get(self.params['eDate'])
        self.ftime = datetime.datetime.now()

    def start_requests(self):
        try:
            ssDate = arrow.Arrow.now().format('YYYY-MM-DD')
            url = 'http://live.500.com/wanchang.php?e={0}'
            for day in arrow.Arrow.range('day', self.sdate, self.edate.shift(days=-1)):
                ssDate = day.format('YYYY-MM-DD')
                yield scrapy.Request(url=url.format(ssDate), callback=self.parse, meta={'year': ssDate[:4]})
        except Exception as e:
            print('start_requests err:{0}'.format(e))

    def parse(self, response):
        datas = Selector(response).xpath('//tr[@gy and @yy]').extract()
        for data in datas:
            try:
                st = Selector(text=data)
                if not st:
                    continue

                url = st.xpath('//td[@class="p_lr01" and @align="right"]/a/@href').extract()[0].replace(' ', '')
                if url[:4] != 'http':
                    url = 'http:{}'.format(url)
                yield scrapy.Request(url=url, callback=self.parse_team)

                url = st.xpath('//td[@class="p_lr01" and @align="left"]/a/@href').extract()[0].replace(' ', '')

                if url[:4] != 'http':
                    url = 'http:{}'.format(url)
                yield scrapy.Request(url=url, callback=self.parse_team)

                url = st.xpath('//td[@class="ssbox_01"]/a/@href').extract()[0].replace(' ', '')
                color = st.xpath('//td[@class="ssbox_01"]/@bgcolor').extract()[0].replace(' ', '')[1:]
                color = int('0x{}'.format(color), 16)
                if url[:4] != 'http':
                    url = 'http:{}'.format(url)
                yield scrapy.Request(url=url, callback=self.parse_league, meta={"color": color})

            except Exception as e:
                print('parseImm err: {0}'.format(e))

    # 解析球队信息
    def parse_team(self, response):
        try:
            url = response.url
            se = Selector(response=response)
            name = se.xpath('//div[@class="lcur_chart"]//p/b/text()').extract()[0].replace(' ', '').replace('　', '')
            fname = se.xpath('//div[@class="itm_logo"]/img/@alt').extract()[0].replace(' ', '').replace('　', '')
            sid = re.findall('[0-9]+', url)[1]
            item = TeamItem()
            item['name'] = fname
            item['fname'] = name
            item['sid'] = sid
            item['imgname'] = '{}'.format(sid)
            item['imgurl'] = 'http://liansai.500.com/static/soccerdata/images/TeamPic/teamsignnew_{0}.png'.format(sid)
            print(item)
            yield item
        except Exception as e:
            print(e)

    # 解析联赛信息
    def parse_league(self, response):
        try:
            color = response.meta['color']
            se = Selector(response=response)
            ss = se.xpath('//title/text()').extract()[0]
            ss = re.findall('^【.*】', ss)[0]
            ss = ss[1:][:-1]
            ss = ss.split('_')
            name = ss[1].replace(' ', '').replace('　', '')
            fname = ss[2].replace(' ', '').replace('　', '')
            item = LeagueItem()
            item['name'] = name
            item['fname'] = fname
            item['color'] = color
            print(item)
            yield item
        except Exception as e:
            print(e)
