import json
from zdppy_rabbitmq import RabbitMQ

mq = RabbitMQ()
channel, queue = mq.get_channel_direct()

for i in range(10):
    message = json.dumps({'OrderId': "1000%s" % i})
    # 指定 routing_key。delivery_mode = 2 声明消息在队列中持久化，delivery_mod = 1 消息非持久化
    mq.publish_direct(channel, message)
    print(message)
