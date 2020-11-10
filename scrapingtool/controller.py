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
    
    with open('instance_id.txt', 'r') as f:
        instance_id = f.read().strip()
    
    with open('num_instances.txt', 'r') as f:
        num_instances = f.read().strip()
    
    with open('pid.txt', 'r') as f:
        pid = f.read().strip()
    
    logger.info(f"Started the monitor program!")
    
    # Listen for events
    while True:
        logger.info(f"Sleeping for {INTERVAL} seconds...")
        time.sleep(INTERVAL)
        logger.info(f"Woke Up! Checking status")
        
        if not os.path.exists('pid.txt'):
            logger.info(f"pid.txt does not exist. Assuming that the process has completed")
            logger.info('Exiting....')
            exit(0)
        
        with open('pid.txt', 'r') as f:
            pid = f.read().strip()
        key = f"TIMESTAMP_ID_{instance_id}"
        value = cache.get(key)
        if value is None:
            # Expired
            logger.warning(f"The process has timed out! Killing and restarting again")
            subprocess.run(["kill", "-15", f"{pid}"])
            command = f'python3 scrapingtool/archive.py --process_archived_pids --categories "{category}" --instance_id {instance_id} --num_instances {num_instances} --num_threads 5'.split(' ')
            p = subprocess.Popen(command)
            logger.info(f"Restarted with new pid {p.pid}")
            with open('pid.txt', 'w') as f:
                f.write(str(p.pid))
            pid = p.pid
        else:
            continue


if __name__ == '__main__':
    # Start a session using the existing engine
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    monitor(category="headphones")
