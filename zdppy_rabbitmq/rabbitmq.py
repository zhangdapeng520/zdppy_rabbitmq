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
        self.channel_pool = {}  # 通道池
        self.init_channel_pool()  # 初始化通道池
        self.queue_pool = {}  # 队列池

    def init_channel_pool(self):
        """
        初始化通道池
        """
        self.channel_pool = {
            f"channel_{i}": {
                "channel": self.connection.channel(),  # 通道
                "is_use": False,  # 是否在用
                "queue_list": [],  # 挂载的队列列表
            }
            for i in range(self.channel_pool_size)
        }  # 通道池

    def get_channel(self) -> (str, object):
        """
        获取通道
        """
        # 从前往后取
        for name, channel in self.channel_pool.items():
            if not channel["is_use"]:
                self.channel_pool[name]["is_use"] = True
                return name, channel["channel"]

        # 池子满了，所有都被用到了，使用临时通道
        channel = self.connection.channel()
        return "", channel

    def add_queue(self, queue_name: str):
        """
        添加队列
        """
        # 队列名为空
        if queue_name == "":
            raise EmptyError("queue_name 队列名不能为空")

        # 队列已存在
        if queue_name in self.queue_pool.keys():
            return

        # 创建队列
        name, channel = self.get_channel()
        result = channel.queue_declare(queue=queue_name)
        if name != "":
            self.channel_pool[name]["is_use"] = True
            self.channel_pool[name]["queue_list"].append(result)

        # 添加队列到连接池
        self.queue_pool[queue_name] = result

    def publish_basic(self, exchange: str = "", routing_key: str = "zdppy_rabbitmq", body: str = None):
        """
        基本的发布
        :return:
        """
        # 发布内容不存在
        if body is None:
            return EmptyError("发布内容不能为空")

        # 发布
        name, channel = self.get_channel()
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
        self.channel_pool[name]["is_use"] = False

    def consume_basic(self, queue_name: str = "zdppy_rabbitmq", on_message_callback: Callable = None):
        """
        基本的消费
        :return:
        """
        # 添加队列
        self.add_queue(queue_name=queue_name)

        # 开始消费
        name, channel = self.get_channel()
        channel.basic_consume(queue_name, on_message_callback)
        channel.start_consuming()
        self.channel_pool[name]["is_use"] = False

    def __del__(self):
        self.connection.close()
