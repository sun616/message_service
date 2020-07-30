import pika

from .config import RABBITMQ_CONN_INFO, EXCHANGE_NAME, EXCHANGE_TYPE, QUEUE_NAME


__all__ = ['PublishService', 'SubscribeService']


class ConnectService(object):
    @classmethod
    def get_connection_and_channel(cls):
        connection = cls.connect_to_rbmq(RABBITMQ_CONN_INFO)
        channel = connection.channel()
        return connection, channel

    @staticmethod
    def connect_to_rbmq(rabbitmq_conn_info):
        host, port, username, password = rabbitmq_conn_info.values()
        # 创建一个有凭证的新实例
        credentials = pika.PlainCredentials(username, password)
        # 使用凭证连接RabbitMQ服务器
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host, port, credentials=credentials))
        return connection


class CommonService(ConnectService):
    def __new__(cls):
        if len(cls.__mro__) == 3:  # 也可以通过cls.__name__来判断
            raise Exception('can not instantiate this class for itself, just can be instantiated by inherited classes')
        else:
            return super().__new__(cls)

    def __init__(self, connection=None) -> None:
        if connection is None:
            connection = self.connection
        self.connection = connection

    def exchange_declare(self, exchange_name, exchange_type):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

    def _queue_declare(self, queue_name, used_for_log):
        """声明一个队列，如果用途是日志，则会添加参数exclusive=True，使消费端connection端口后自动删除队列，
        否则添加参数durable=True，使消息持久化
        Args:
            queue_name ([str]): [队列名称]
            used_for_log ([bool]): [此队列是否用于日志]

        Returns:
            [type]: [description]
        """
        if used_for_log:
            result_method_frame = self.channel.queue_declare(queue=queue_name, exclusive=True)
        else:
            result_method_frame = self.channel.queue_declare(queue=queue_name, durable=True)
        return result_method_frame

    def queue_bind(self, queue_name, used_for_log, exchange_name, binding_keys):
        """将queue绑定到exchange

        Args:
            exchange ([str]): [exchange名称]
            binding_keys ([str or list]]): [binging_keys为字符串或者列表]
            queue_name ([type]): [队列名称]
            used_for_log ([type]): [此队列是否用于日志]

        Raises:
            NotImplementedError: [暂不支持除字符串、列表外的其他类型的binding_keys]

        Returns:
            [str]: [队列名称]
        """

        result_method_frame = self._queue_declare(queue_name, used_for_log)
        queue_name = result_method_frame.method.queue

        if type(binding_keys) == str:
            self.channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=binding_keys)
        elif type(binding_keys) == list:
            for binding_key in binding_keys:
                self.channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=binding_key)
        else:
            raise NotImplementedError

        return queue_name

    @staticmethod
    def callback(ch, method, properties, body):
        print(" [x] %r:%r" % (method.routing_key, body))
        print('method.delivery_tag:', method.delivery_tag)

        ch.basic_ack(delivery_tag=method.delivery_tag)      # 加入消息确认机制，防止消费端中断导致此消息丢失


class PublishService(CommonService):
    """
    Args:
        message ([type]): [description]
        routing_key (str, optional): [默认采取广播模式，故为空]. Defaults to ''.
        exchange_name ([type], optional): [description]. Defaults to EXCHANGE_NAME.
    """

    connection, channel = ConnectService.get_connection_and_channel()
    def publish(self, message, routing_key, exchange_name=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE):
        """用于发布消息，需要给定消息内容和路由方式

        Args:
            message ([type]): [需要发布的消息]
            routing_key ([str]): [路由方式]
            exchange_name ([str], optional): [交换机名称]. Defaults to EXCHANGE_NAME.
            exchange_type ([str], optional): [交换机类型]. Defaults to EXCHANGE_TYPE.
        """
        self.exchange_declare(exchange_name, exchange_type)
        self.channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message,
                                   properties=pika.BasicProperties(
                                       delivery_mode=2  # make message persistent
                                   ))
        print(" [x] Sent %r:%r" % (routing_key, message))
        self.connection.close()


class SubscribeService(CommonService):
    """提供RabbitMQ订阅服务
    默认会自动连接到rabbitmq服务，也可在实例化时提供一个已经连接的对象connection
    Args:
        CommonService ([class]): [订阅和发布共有的服务]
    """
    connection, channel = ConnectService.get_connection_and_channel()
    def subscribe(self, binding_keys, queue_name=QUEUE_NAME, used_for_log=False,
                  exchange_name=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE):
        """订阅消息，默认为fanout方式订阅，可自主设置，
        需先运行订阅方再运行发布方，否则在订阅生效之前的消息都无法被接收
        （需先queue_bind，发布方才能通过exchange将消息路由至queue）

        Args:
            binding_keys ([str | list]): [使队列绑定到exchange的绑定值，单个值时为字符串，多个为列表的形式]
        """

        self.exchange_declare(exchange_name, exchange_type)
        queue_name = self.queue_bind(queue_name, used_for_log, exchange_name, binding_keys)

        print(' [*] Waiting for messages. To exit press CTRL+C')

        self.channel.basic_qos(prefetch_count=1)  # 使消息按照处理能力分发，而不是‘公平’分发
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback)
        self.channel.start_consuming()

        # python receive.py #                使用" # "号收所有
        # python receive.py mysql.*          使用"mysql.* "号收来自mysql的信息
        # python receive.py mysql.error.*    使用"mysql.error.* "号收来自mysql的错误信息
        # python receive.py *.django.*       使用"*.django.* "号收来自所有Django的信息
