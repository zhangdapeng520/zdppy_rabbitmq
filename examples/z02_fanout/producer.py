import json
from zdppy_rabbitmq import RabbitMQ

mq = RabbitMQ()

channel, queue = mq.get_channel_fanout()
for i in range(10):
    message = json.dumps({'OrderId': "1000%s" % i})
    mq.publish_fanout(channel, message)
    print(message)
