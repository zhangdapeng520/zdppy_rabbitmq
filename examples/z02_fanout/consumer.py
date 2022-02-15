from zdppy_rabbitmq import RabbitMQ

mq = RabbitMQ()


# 定义一个回调函数来处理消息队列中的消息，这里是打印出来
def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(body.decode())


mq.consume_fanout(callback)
