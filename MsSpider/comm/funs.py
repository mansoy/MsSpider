# 字符串转Int,给定默认值
def s2i(s, default=0):
    ret = default
    try:
        ret = int(s)
    except:
        # print('s2i faile:{0}'.format(e))
        ret = default
    finally:
        return ret


def s2f(s, default=0.0):
    ret = default
    try:
        ret = float(s)
    except:
        ret = default
    finally:
        return ret
