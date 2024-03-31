import redis
from config import redis_config


class RedisInterface(object):
    def __init__(self):
        fraud = redis.ConnectionPool(**redis_config)
        self.fraud = redis.Redis(connection_pool=fraud)

