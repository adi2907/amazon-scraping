import json
import uuid

import redis
from decouple import UndefinedValueError, config

from utils import create_logger

# Create the logger
logger = create_logger(__name__)

try:
    REDIS_SERVER_PASSWORD = config('REDIS_SERVER_PASSWORD')
except UndefinedValueError:
    REDIS_SERVER_PASSWORD = None

try:
    REDIS_SERVER_CONF = {
        'servers' : {
            'master': {
                'REDIS_SERVER_HOST' : config('REDIS_SERVER_HOST'),
                'REDIS_SERVER_PORT' : config('REDIS_SERVER_PORT'),
                'REDIS_SERVER_DATABASE': config('REDIS_SERVER_DATABASE'),
            }
        },
    }
except UndefinedValueError:
    # No Redis is used
    REDIS_SERVER_CONF = None


def is_connected(func): 
    def wrapper(*args, **kwargs):
        if hasattr(args[0], 'cache'):
            if getattr(args[0], 'cache') is None:
                raise ValueError("You must connect to the cache!")
            if hasattr(args[0], 'timeout') and 'timeout' in kwargs:
                timeout = kwargs['timeout']
                if timeout is not None and not isinstance(timeout, int):
                    raise ValueError("Timeout must be an integer")
                if timeout == -1:
                    kwargs['timeout'] = getattr(args[0], 'timeout')
        return func(*args, **kwargs)  
    return wrapper 


class Cache():
    shared_state = {}
    timeout = 12 * 60 * 60

    def __init__(self):
        self.__dict__ = self.shared_state
        self.local_cache = {}
        self.use_redis = False
        self.cache = None

    def connect(self, server_key='master', use_redis=True):
        if REDIS_SERVER_CONF is not None and use_redis == True:
            # Use Redis
            self.use_redis = True
            redis_server_conf = REDIS_SERVER_CONF['servers'][server_key]
            if REDIS_SERVER_PASSWORD is None:
                connection_pool = redis.ConnectionPool(host=redis_server_conf['REDIS_SERVER_HOST'], port=redis_server_conf['REDIS_SERVER_PORT'],
                                                    db=redis_server_conf['REDIS_SERVER_DATABASE'])
            else:
                # Use pw
                connection_pool = redis.ConnectionPool(host=redis_server_conf['REDIS_SERVER_HOST'], port=redis_server_conf['REDIS_SERVER_PORT'],
                                                    db=redis_server_conf['REDIS_SERVER_DATABASE'], password=REDIS_SERVER_PASSWORD)
            self.cache = redis.StrictRedis(connection_pool=connection_pool)
            logger.info("Connected to Redis Cache!")
        else:
            self.use_redis = False
            self.cache = self.local_cache
            logger.info("Not using redis. Defaulting to local cache")
    
    @is_connected
    def get(self, key):
        if self.use_redis == False:
            return self.local_cache.get(key)
        else:
            # Use redis to retrieve the key
            value = self.cache.get(key)
            
            if key in self.shared_state:
                if value is None:
                    # Timeout - Remove this link
                    del self.shared_state[key]
                else:
                    value = json.loads(value)
            elif value is not None:
                value = value.decode()
            
            return value
    
    @is_connected
    def set(self, key, value, timeout=-1):
        if self.use_redis == False:
            self.cache[key] = value
        else:
            if isinstance(value, dict) or isinstance(value, uuid.UUID) or isinstance(value, list):
                value = json.dumps(value)
                self.shared_state[key] = ""
            
            self.cache.set(key, value)

            if timeout is not None:
                self.cache.expire(key, timeout)
    
    @is_connected
    def delete(self, key):
        if self.use_redis == False:
            if key in self.cache:
                del self.cache[key]
        else:
            self.cache.delete(key)
            if key in self.shared_state:
                del self.shared_state[key]


if __name__ == '__main__':
    print("Testing Cache")
    
    cache = Cache()
    
    cache.connect('master', use_redis=True)
    
    print(cache.get('foo'))
    
    cache.set('foo', {'a': {1: 2}})
    
    print(cache.get('foo'))
    
    cache.delete('foo')
    
    print(cache.get('foo'))
    print("Completed!")
