from cache import Cache
from utils import create_logger, listing_categories

logger = create_logger('reset_state')

def main():
    cache = Cache()
    cache.connect('master', use_redis=True)

    for category in listing_categories:
        # Take a backup first
        cache.delete(f"{category}_PIDS_BACKUP")
        pids = cache.smembers(f"{category}_PIDS")
        for pid in pids:
            cache.sadd(f"{category}_PIDS_BACKUP", pid)
        
        cache.delete(f"{category}_PIDS")
        cache.delete(f"COUNTER_{category}")


if __name__ == '__main__':
    main()