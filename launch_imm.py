from multiprocessing import Process
from scrapy import cmdline
import time


def run(args):
  cmdline.execute(args)


if __name__ == "__main__":
    args = ['scrapy', 'crawl', 'ImmSpider', '-a', 'params={"stime":120, "etime":0, "ou": 1, "ya": 1, "dx": 1}']

    while True:
        p = Process(target=run, args=(args,))  # 新建一个子进程p，目标函数是run，args是函数f的参数列表
        p.start()  # 开始执行进程
        p.join()  # 等待子进程结束
        time.sleep(1)
    print('MS - Over!')
