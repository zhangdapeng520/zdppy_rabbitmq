class RabbitMQConfig:
    """
    rabbitMQ配置对象
    """
    host: str = "127.0.0.1"
    port: int = 5672
    username: str = "guest"
    password: str = "guest",
    virtual_host: str = "/"
