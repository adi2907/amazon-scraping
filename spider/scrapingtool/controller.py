import argparse
import concurrent.futures
import random
import signal
import subprocess
import sys
import time
import os
import traceback
from datetime import datetime, timedelta

from decouple import UndefinedValueError, config

import cache
from utils import category_to_domain, create_logger, domain_map, domain_to_db

logger = create_logger('controller')

cache = cache.Cache()
cache.connect('master', use_redis=True)

today = datetime.today().strftime("%d-%m-%y")

def monitor(category="headphones"):
    global cache
    INTERVAL = 60 * 20
    
    logger.info(f"Started the monitor program!")
    
    # Listen for events
    while True:
        logger.info(f"Sleeping for {INTERVAL} seconds...")
        time.sleep(INTERVAL)
        logger.info(f"Woke Up! Checking status")

        key = f"SCRAPING_COMPLETED"
        value = cache.get(key)
        if value is not None:
            # Expired
            logger.warning(f"Scraping done! Shutting down")
            command = f'sudo shutdown'.split(' ')
            p = subprocess.Popen(command)
        else:
            continue


if __name__ == '__main__':
    # Start a session using the existing engine
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    monitor(category="headphones")
