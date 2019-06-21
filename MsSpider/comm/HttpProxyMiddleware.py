import random
import base64
import time


class MsHttpProxyMiddleware(object):
    def __init__(self, ip=''):
        self.ip = ip
        self.tickcount = 0
        self.tunnel = random.randint(1, 10000)

    def base64ify(self, bytes_or_str):
        if isinstance(bytes_or_str, str):
            input_bytes = bytes_or_str.encode('utf8')

        output_bytes = base64.urlsafe_b64encode(input_bytes)
        return output_bytes.decode('ascii')

    # 请求处理
    def process_request(self, request, spider):
        # 代理服务器
        try:
            getTickCount = lambda: int(round(time.time() * 1000))

            if getTickCount() - self.tickcount > random.randint(200, 300):
                self.tickcount = getTickCount()
                self.tunnel = random.randint(1, 10000)

            proxyHost = "n10.t.16yun.cn"
            proxyPort = "6442"

            # 代理隧道验证信息
            proxyUser = "16ZZNQKU"
            proxyPass = "973876"

            request.meta['proxy'] = "http://{0}:{1}".format(proxyHost, proxyPort)

            # 添加验证头
            encoded_user_pass = self.base64ify(proxyUser + ":" + proxyPass)
            request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass

            # 设置IP切换头(根据需求)
            # tunnel = random.randint(1, 10000)
            request.headers['Proxy-Tunnel'] = str(self.tunnel)
        except Exception as e:
            print(e)