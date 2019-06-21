import logging
import arrow

# class MsLog():
#     @classmethod
#     def debug(cls, strInfo):
#         # logging.debug(strInfo)
#         logger.debug(strInfo)
#
#     @classmethod
#     def info(cls, strInfo):
#         # logging.info(strInfo)
#         logger.info(strInfo)


class MsLog():
    @classmethod
    def debug(cls, strInfo):
        try:
            flog = open('slog.log', "a+")
            flog.write('[{0}]{1}\n'.format(arrow.Arrow.now().format('YYYY-MM-DD HH:mm:ss'), strInfo))
            flog.close()
        except:
            if not flog:
                flog.close()