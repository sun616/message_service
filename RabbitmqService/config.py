try:
    from .security import RABBITMQ_CONN_INFO
except:
    RABBITMQ_CONN_INFO = {'host': 'localhost', 'port': 5672, 'username': '', 'password': ''}
    

# 默认为fanout轮询模式
EXCHANGE_NAME = 'task_exchange'
EXCHANGE_TYPE = 'fanout'
QUEUE_NAME = 'task_queue'