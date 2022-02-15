import json
from zdppy_rabbitmq import *
import time

mq = RabbitMQ()


def producer1():
    for i in range(10):
        message = json.dumps({'OrderId': "1000%s" % i})

        # 向队列插入数值 routing_key是队列名
        mq.publish_basic(mq.channel, body=message)
        print(message)


def producer2():
    while True:
        message = json.dumps({'OrderId': "1000%s" % time.time()})

        # 向队列插入数值 routing_key是队列名
        mq.publish_basic(mq.channel, routing_key="hz.lagrange.scada", body=message)
        print(message)
        time.sleep(1)


if __name__ == '__main__':
    # producer1()
    producer2()
