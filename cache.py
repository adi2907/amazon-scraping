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


KEY_EXPIRED = -2
KEY_NON_VOLATILE = -1


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
    def ttl(self, key):
        if self.use_redis == True:
            ttl = self.cache.ttl(key)
            if ttl == KEY_NON_VOLATILE:
                return None
            elif ttl == KEY_EXPIRED:
                return 0
            else:
                return ttl
        else:
            return None
    
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

    @is_connected
    def lpush(self, key, value):
        if self.use_redis == False:
            if key in self.cache:
                if isinstance(self.cache.get(key), list):
                    self.cache[key].append(value)
                else:
                    raise ValueError(f"Non-List key already exists for {key}")
            else:
                self.cache[key] = [value]
        else:
            if isinstance(value, dict) or isinstance(value, uuid.UUID) or isinstance(value, list):
                value = json.dumps(value)
                self.shared_state[key] = ""
            
            self.cache.lpush(key, value)
    
    @is_connected
    def lrange(self, key, start=0, end=None):
        if end is None:
            end = 0
        if start is None:
            start = 0
        
        if not isinstance(start, int) or start < 0:
            raise ValueError("start must be a positive integer")
        elif not isinstance(end, int) or end < 0: 
            raise ValueError("end must be a positive integer")

        if self.use_redis == True:
            range_bytes = self.cache.lrange(key, start, end-1)
            
            history = []
            for value in range_bytes:
                value = value.decode()
                if isinstance(value, dict) or isinstance(value, list) or isinstance(value, uuid.UUID):
                    value = json.loads(value)
                else:
                    pass
                history.append(value)
            
            history = history[::-1]
            return history
        else:
            return self.cache.get(key)[start : end]
    
    @is_connected
    def sadd(self, set_name, element):
        if self.use_redis == True:
            if isinstance(element, dict) or isinstance(element, uuid.UUID) or isinstance(element, list):
                element = json.dumps(element)
            
            return self.cache.sadd(set_name, element)
        else:
            _set = self.cache.get(set_name)
            if not isinstance(_set, set):
                raise TypeError
            _set.add(element)
            self.cache[set_name] = _set
            return True
    
    @is_connected
    def sismember(self, set_name, element):
        if self.use_redis == True:
            if isinstance(element, dict) or isinstance(element, uuid.UUID) or isinstance(element, list):
                element = json.dumps(element)
            
            return self.cache.sismember(set_name, element)
        else:
            _set = self.cache.get(set_name)
            if not isinstance(_set, set):
                raise TypeError
            if element in _set:
                return True
            else:
                return False

    @is_connected
    def smembers(self, set_name):
        if self.use_redis == True:
            return self.cache.smembers(set_name)
        else:
            raise ValueError("Only allowed in Redis")


if __name__ == '__main__':
    print("Testing Cache")
    
    cache = Cache()
    
    cache.connect('master', use_redis=True)

    cache.delete('foo')
    
    print(cache.get('foo'))
    
    cache.set('foo', {'a': {1: 2}}, timeout=10)
    
    print(cache.get('foo'))

    print(cache.ttl('foo'))
    
    cache.delete('foo')
    
    print(cache.get('foo'))

    cache.lpush('foo', 'sampl')
    cache.lpush('foo', 'hello')
    cache.lpush('foo', {'a': 1})

    print(cache.lrange('foo'))

    cache.delete('foo')

    print(cache.get('foo'))

    cache.sadd("sample_set", "1234")
    cache.sadd("sample_set", "5678")
    cache.sadd("sample_set", "1234")
    print(cache.sismember("sample_set", "5678"))
    print(cache.smembers("sample_set"))

    print("Completed!")
