from multiprocessing import Pool
from scrapy.cmdline import execute
from multiprocessing import Process


def run(args):
    execute(args)


# scrapy crawl HistorySpider -a params={\"delay\":0.3,\"sDate\":\"2018-01-01\",\"eDate\":\"2018-04-01\",\"ou\":0,\"ya\":0,\"dx\":0}
# scrapy crawl HistorySpider -a params={\"delay\":1,\"sDate\":\"2018-01-01\",\"eDate\":\"2018-04-01\",\"ou\":0,\"ya\":0,\"dx\":0}
# scrapy crawl HistorySpider -a params={\"delay\":1,\"sDate\":\"2018-01-01\",\"eDate\":\"2018-04-01\",\"ou\":0,\"ya\":0,\"dx\":0}
# scrapy crawl HistorySpider -a params={\"delay\":1,\"sDate\":\"2018-01-01\",\"eDate\":\"2018-04-01\",\"ou\":0,\"ya\":0,\"dx\":0}
# scrapy crawl HistorySpider -a params={\"delay\":1,\"sDate\":\"2018-01-01\",\"eDate\":\"2018-04-01\",\"ou\":0,\"ya\":0,\"dx\":0}
# scrapy crawl HistorySpider -a params={\"delay\":1,\"sDate\":\"2018-01-01\",\"eDate\":\"2018-04-01\",\"ou\":0,\"ya\":0,\"dx\":0}


if __name__ == "__main__":
    params = [
        # ['scrapy', 'crawl', 'BaseDataSpider', '-a', 'params={"delay": 0.3, "sDate":"2019-01-01", "eDate":"2019-03-01", "ou": 1, "ya": 1, "dx": 1}'],
        ['scrapy', 'crawl', 'HistorySpider', '-a', 'params={"delay": 0.3, "sDate":"2019-01-01", "eDate":"2019-04-01", "ou": 1, "ya": 1, "dx": 1}'],
        # ['scrapy', 'crawl', 'HistorySpider', '-a', 'params={"delay": 1, "sDate":"2018-04-01", "eDate":"2018-06-01", "ou": 1, "ya": 1, "dx": 1}'],
        # ['scrapy', 'crawl', 'HistorySpider', '-a', 'params={"delay": 1, "sDate":"2018-06-01", "eDate":"2018-08-01", "ou": 1, "ya": 1, "dx": 1}'],
        # ['scrapy', 'crawl', 'HistorySpider', '-a', 'params={"delay": 1, "sDate":"2018-08-01", "eDate":"2018-10-01", "ou": 1, "ya": 1, "dx": 1}'],
        # ['scrapy', 'crawl', 'HistorySpider', '-a', 'params={"delay": 1, "sDate":"2018-10-01", "eDate":"2018-12-01", "ou": 1, "ya": 1, "dx": 1}'],
        # ['scrapy', 'crawl', 'HistorySpider', '-a', 'params={"delay": 1, "sDate":"2018-12-01", "eDate":"2019-01-01", "ou": 1, "ya": 1, "dx": 1}']
    ]
    spider_pool = Pool(1)  # 创建拥有10个进程数量的进程池
    spider_pool.map(run, params)
    spider_pool.close()  # 关闭进程池，不再接受新的进程
    spider_pool.join()  # 主进程阻塞等待子进程的退出
    print('MS - Over!')

# if __name__ == "__main__":
#     args = ['scrapy', 'crawl', 'HistorySpider', '-a', 'params={"delay": 0.3, "sDate":"2018-01-01", "eDate":"2018-04-01", "ou": 1, "ya": 1, "dx": 1}']
#
#     p = Process(target=run, args=(args,))  # 新建一个子进程p，目标函数是run，args是函数f的参数列表
#     p.start()  # 开始执行进程
#     p.join()  # 等待子进程结束