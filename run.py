from scrapy import cmdline

cmdline.execute(['scrapy', 'crawl', 'LeagueDataSpider', '-a', 'params={"cmd":1}'])
