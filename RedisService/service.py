from pika import connection
import redis

from .config import HOST, PORT


__all__ = ['PublishService', 'SubscribeService']


class ConnectServiceMixin(object):
    
    @staticmethod
    def connect_to_redis():
        connection = redis.Redis(host=HOST, port=PORT)
        return connection

class CommonService(ConnectServiceMixin):
    """提供连接redis的服务，也可以在实例化当前类的子类时提供已经连接的connection对象
    下面两个类变量的作用在于避免每次实例化都去连接redis并获取pubsub对象
    """
    connection = ConnectServiceMixin.connect_to_redis()
    pubsub = connection.pubsub()
    
    def __new__(cls, *args, **kwargs):
        if len(cls.__mro__) == 3:
            raise ValueError('can not instantiate this class for itself, just can be instantiated by inherited classes')
        else:
            return super().__new__(cls)

    def __init__(self, connection=None) -> None:
        if connection is None:
            connection = self.connection
        self.connection = connection

    def connect_to_redis(self):
        connection = redis.Redis(host=HOST, port=PORT)
        return connection


class PublishService(CommonService):
    def publish(self, channel, message):
        self.connection.publish(channel, message)


class SubscribeService(CommonService):
    def get_messages(self):
        """获取订阅的消息

        Yields:
            [tuple]: [return channel name and data]
        """
        for item in self.pubsub.listen():
            if item['data'] == "KILL":
                self.pubsub.unsubscribe()
                print(self, "unsubscribed and finished")
                break
            else:
                yield (item['channel'], item['data'])

    def subscribe(self, channels=[]):
        """普通订阅，channels必须为列表，可以只有单个值
        """
        if type(channels) != list:
            raise TypeError('channels must to be a list')
        self.pubsub.subscribe(channels)

    def psubscribe(self, channels=[]):
        """模式订阅，channels必须为列表，可以只有单个值
        """
        if type(channels) != list:
            raise TypeError('channels must to be a list')
        self.pubsub.psubscribe(channels)
