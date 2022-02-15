class RabbitMQError(Exception):
    """
    rabbitMQ异常
    """
    pass


class EmptyError(RabbitMQError):
    """
    空异常
    """
    pass
