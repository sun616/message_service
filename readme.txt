此package提供基于rabbitmq和redis的消息服务
使用时可以直接from MessageService.RabbitmqService import PublishService, SubscribeService
.
├── RabbitmqService
│   ├── __init__.py
│   ├── config.py      项目的一些配置
│   ├── security.py    涉及到安全问题的设置
│   └── service.py     rabbitmq服务核心模块, 提供rabbitmq的发布订阅服务，支持direct、fanout、topic三种模式
├── RedisService
│   ├── __init__.py
│   ├── config.py      项目的一些配置
│   └── service.py     redis服务核心模块, 提供redis的发布订阅服务
└── readme.txt