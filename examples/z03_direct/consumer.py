from zdppy_rabbitmq import RabbitMQ

mq = RabbitMQ()
channel, queue = mq.get_channel_direct()


# 定义一个回调函数来处理消息队列中的消息，这里是打印出来
def callback(ch, method, properties, body):
    # 给mq确认收到消息
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(body.decode())


mq.consume_direct(callback)
