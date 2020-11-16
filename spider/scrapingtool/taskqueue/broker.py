
try:
    import dramatiq
except:
    pass

class Broker():
    def __init__(self, broker_type='redis', connection_params={'host': '127.0.0.1', 'port': 6379, 'db': 0, 'password': ''}):
        if broker_type == 'redis':
            from dramatiq.brokers.redis import RedisBroker

            self.broker_type = 'redis'
            self.broker = RedisBroker(**connection_params)
            dramatiq.set_broker(self.broker)
        
        elif broker_type is None:
            self.broker_type = None
        
        else:
            raise ValueError(f"Unsupported broker type: {broker_type}")
