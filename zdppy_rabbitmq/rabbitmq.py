import pika
from .exceptions import EmptyError
from typing import Callable


class RabbitMQ:
    def __init__(self, host: str = "127.0.0.1", port: int = 5672, username: str = "guest", password: str = "guest",
                 virtual_host: str = "/", channel_pool_size: int = 100):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.channel_pool_size = channel_pool_size
        self.credentials = pika.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=virtual_host,
                credentials=self.credentials))
        self.channel = self.connection.channel()

    def get_channel_fanout(self, exchange: str = "zdppy_rabbitmq"):
        """
        获取fanout模式下下的channel通道
        :return:
        """
        channel = self.connection.channel()

        # 声明exchange，由exchange指定消息在哪个队列传递，如不存在，则创建。durable = True 代表exchange持久化存储，False 非持久化存储
        channel.exchange_declare(exchange=exchange, durable=True, exchange_type='fanout')

        # 创建临时队列,队列名传空字符，consumer关闭后，队列自动删除
        result = channel.queue_declare('', exclusive=True)

        # 声明exchange，由exchange指定消息在哪个队列传递，如不存在，则创建。durable = True 代表exchange持久化存储，False 非持久化存储
        channel.exchange_declare(exchange=exchange, durable=True, exchange_type='fanout')

        # 绑定exchange和队列  exchange 使我们能够确切地指定消息应该到哪个队列去
        channel.queue_bind(exchange=exchange, queue=result.method.queue)

        return channel, result.method.queue

    def get_channel_direct(self, exchange: str = "zdppy_rabbitmq_direct", routing_key: str = "zdppy_rabbitmq_direct"):
        """
        获取direct模式下下的channel通道
        :return:
        """
        channel = self.connection.channel()

        # 创建临时队列，队列名传空字符，consumer关闭后，队列自动删除
        result = channel.queue_declare('', exclusive=True)

        # 声明exchange，由exchange指定消息在哪个队列传递，如不存在，则创建。durable = True 代表exchange持久化存储，False 非持久化存储
        channel.exchange_declare(exchange=exchange, durable=True, exchange_type='direct')

        # 绑定exchange和队列  exchange 使我们能够确切地指定消息应该到哪个队列去
        channel.queue_bind(exchange=exchange, queue=result.method.queue, routing_key=routing_key)

        return channel, result.method.queue,

    def publish_basic(self, channel,
                      exchange: str = "",
                      routing_key: str = "zdppy_rabbitmq_basic",
                      body: str = None,
                      delivery_mode: int = 2):
        """
        基本的发布
        :param channel: 通道
        :param exchange: 交换器名
        :param routing_key: 路由名
        :param body: 要发送的消息
        :param delivery_mode 2 声明消息在队列中持久化，1 消息非持久化
        :return:
        """
        # 发布内容不存在
        if body is None:
            return EmptyError("发布内容不能为空")

        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body,
                              properties=pika.BasicProperties(delivery_mode=delivery_mode))

    def consume_basic(self, queue_name: str = "zdppy_rabbitmq_basic",
                      callback: Callable = None,
                      auto_ack: bool = False,
                      durable: bool = True):
        """
        基本的消费
        :param queue_name 队列名称
        :param callback 消费方法
        :param auto_ack 设置成 False，在调用callback函数时，未收到确认标识，消息会重回队列。True，无论调用callback成功与否，消息都被消费掉
        :param durable: 是否持久化
        :return:
        """
        channel = self.connection.channel()

        # 申明消息队列，消息在这个队列传递，如果不存在，则创建队列
        channel.queue_declare(queue=queue_name, durable=durable)

        # 告诉rabbitmq，用callback来接收消息
        channel.basic_consume(queue_name, callback, auto_ack)

        # 开始接收信息，并进入阻塞状态，队列里有信息才会调用callback进行处理
        channel.start_consuming()

    def publish_fanout(self, channel, message, exchange: str = "zdppy_rabbitmq_fanout_exchange"):
        """
        以fanout模式发布
        :param channel 通道
        :param message 消息
        :param exchange 交换器名
        :return:
        """
        channel.basic_publish(exchange=exchange, routing_key='', body=message,
                              properties=pika.BasicProperties(delivery_mode=2))

    def consume_fanout(self, callback, exchange: str = "zdppy_rabbitmq_fanout_exchange"):
        """
        以fanout模式消费
        :param callback: 消费的回调函数
        :param exchange 交换器名
        :return:
        """
        channel, queue = self.get_channel_fanout(exchange)

        # 设置成 False，在调用callback函数时，未收到确认标识，消息会重回队列。True，无论调用callback成功与否，消息都被消费掉
        channel.basic_consume(queue, callback, auto_ack=False)
        channel.start_consuming()

    def publish_direct(self, channel, message,
                       exchange: str = "zdppy_rabbitmq_direct_exchange",
                       routing_key: str = "zdppy_rabbitmq_direct_routing_key"):
        """
        以direct模式发布
        :param channel 通道
        :param message 消息
        :param exchange 交换器名
        :param routing_key: 路由器名
        :return:
        """
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message,
                              properties=pika.BasicProperties(delivery_mode=2))

    def consume_direct(self, callback, exchange: str = "zdppy_rabbitmq_direct_exchange",
                       routing_key: str = "zdppy_rabbitmq_direct_routing_key"):
        """
        以fanout模式消费
        :param callback: 消费的回调函数
        :param exchange 交换器名
        :param routing_key: 路由器名
        :return:
        """
        channel, queue = self.get_channel_direct(exchange, routing_key)

        # 告诉rabbitmq，用callback来接受消息
        # 设置成 False，在调用callback函数时，未收到确认标识，消息会重回队列。True，无论调用callback成功与否，消息都被消费掉
        channel.basic_consume(queue, callback, auto_ack=False)
        channel.start_consuming()

    def __del__(self):
        self.connection.close()
