
from scrapy.cmdline import execute
from multiprocessing import Process


def run(args):
    execute(args)


if __name__ == "__main__":
    args = ['scrapy', 'crawl', 'BaseDataSpider', '-a', 'params={"delay": 0.3, "sDate":"2013-01-01", "eDate":"2019-01-01", "ou": 1, "ya": 1, "dx": 1}']

    p = Process(target=run, args=(args,))  # 新建一个子进程p，目标函数是run，args是函数f的参数列表
    p.start()  # 开始执行进程
    p.join()  # 等待子进程结束