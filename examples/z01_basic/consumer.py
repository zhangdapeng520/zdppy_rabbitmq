from zdppy_rabbitmq import RabbitMQ

mq = RabbitMQ()


# 定义一个回调函数来处理消息队列中的消息，这里是打印出来
def callback(ch, method, properties, body):
    # 解析消息
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # 打印消息
    print(body.decode())


# 基本的消费
# mq.consume_basic(callback=callback)
mq.consume_basic(queue_name="hz.lagrange.scada", callback=callback)
