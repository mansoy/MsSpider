from scrapy import cmdline
from multiprocessing import Process


def run(args):
  cmdline.execute(args)


if __name__ == "__main__":
    sdate = '2018-01-01'
    edate = '2019-12-31'
    params = 'params={{"sDate":"{0}", "eDate":"{1}"}}'.format(sdate, edate)
    args = [
        ['scrapy', 'crawl', 'MasterSpider', '-a', params],
        ['scrapy', 'crawl', 'DatailSpider', '-a', params]]

    p = Process(target=run, args=(args[0],))  # 新建一个子进程p，目标函数是run，args是函数f的参数列表
    p.start()  # 开始执行进程
    p.join()  # 等待子进程结束

    p = Process(target=run, args=(args[1],))  # 新建一个子进程p，目标函数是run，args是函数f的参数列表
    p.start()  # 开始执行进程
    p.join()  # 等待子进程结束
