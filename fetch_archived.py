import random
import signal
import sys
import time
import traceback
from datetime import datetime

from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlitedict import SqliteDict

import cache
import db_manager
import parse_data
import proxy
from utils import create_logger

logger = create_logger('fetch_archived')

cache = cache.Cache()
cache.connect('master', use_redis=True)

cache_file = 'cache.sqlite3'

today = datetime.today().strftime("%d-%m-%y")

try:
    OS = config('OS')
except UndefinedValueError:
    OS = 'Windows'


try:
    speedup = config('speedup')
    if speedup == 'True':
        speedup = True
    else:
        speedup = False
except UndefinedValueError:
    speedup = False

logger.info(f"Speedup is {speedup}")


try:
    ultra_fast = config('ultra_fast')
    if ultra_fast == 'True':
        ultra_fast = True
    else:
        ultra_fast = False
except UndefinedValueError:
    ultra_fast = False

logger.info(f"ultra_fast is {ultra_fast}")

my_proxy = proxy.Proxy(OS=OS)

try:
    my_proxy.change_identity()
except:
    logger.warning('No Proxy available via Tor relay. Mode = Normal')
    logger.newline()
    my_proxy = None

last_product_detail = False
terminate = False

# Database Session setup
try:
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_PORT = config('DB_PORT')
    DB_NAME = config('DB_NAME')
    DB_SERVER = config('DB_SERVER')
    DB_TYPE = config('DB_TYPE')
    engine = db_manager.Database(dbtype=DB_TYPE, username=DB_USER, password=DB_PASSWORD, port=DB_PORT, dbname=DB_NAME, server=DB_SERVER).db_engine
except UndefinedValueError:
    DB_TYPE = 'sqlite'
    engine = db_manager.Database(dbtype=DB_TYPE).db_engine
    logger.warning("Using the default db.sqlite Database")
    logger.newline()


Session = sessionmaker(bind=engine)

db_session = Session()


def exit_gracefully(signum, frame):
    # restore the original signal handler as otherwise evil things will happen
    # in raw_input when CTRL+C is pressed, and our signal handler is not re-entrant
    global last_product_detail
    signal.signal(signal.SIGINT, original_sigint)

    try:
        if input("\nReally quit? (y/n)> ").lower().startswith('y'):
            logger.info("Terminating after finishing pending product Details...")
            last_product_detail = True
            terminate = True

    except KeyboardInterrupt:
        logger.info("Exiting Immediately...")
        sys.exit(1)

    # restore the exit gracefully handler here    
    signal.signal(signal.SIGINT, exit_gracefully)


def scrape_product_detail(category, product_url):
    global my_proxy
    global cache, cache_file
    # session = requests.Session()
    server_url = 'https://www.amazon.in'

    product_id = parse_data.get_product_id(product_url)

    logger.info(f"Going to Details page for PID {product_id}")

    obj = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': f'{product_id}'}))

    if obj is None:
        logger.critical(f"Row with PID {product_id} doesn't exist in ProductListing. Returning....")
        return {}
        
    response = my_proxy.get(server_url)
    setattr(my_proxy, 'category', category)
    
    assert response.status_code == 200
    cookies = dict(response.cookies)
    time.sleep(3)

    while True:
        response = my_proxy.get(server_url + product_url, product_url=product_url)
        
        if hasattr(response, 'cookies'):
            cookies = {**cookies, **dict(response.cookies)}
        
        time.sleep(3) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
        html = response.content
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Get the product details
        try:
            details = parse_data.get_product_data(soup, html=html)
            break
        except ValueError:
            logger.warning(f"Written html to {category}_{product_url}.html")
            logger.warning(f"Couldn't parse product Details for {product_id}. Possibly blocked")
            logger.warning("Trying again...")
            time.sleep(random.randint(3, 10) + random.uniform(0, 4)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
            my_proxy.goto_product_listing(category)

    details['product_id'] = product_id # Add the product ID
    
    # Store to cache first
    with SqliteDict(cache_file, autocommit=True) as mydict:
        mydict[f"ARCHIVED_DETAILS_{product_id}"] = details


def process_archived_pids(category):
    global db_session
    
    pids = cache.smembers(f"ARCHIVED_PRODUCTS_{category}")
    for pid in pids:
        pid = pid.decode()
        instance = db_session.query(db_manager.ProductListing).filter(db_manager.ProductListing.product_id == pid).first()
        if not instance:
            logger.warning(f"PID {pid} not in ProductListing. Skipping this product")
            continue

        url = instance.product_url
        logger.info(f"Scraping Details for: {instance.short_title}")
        try:
            scrape_product_detail(category, url)
            logger.info(f"Finished details for this product: {instance.product_id}")
        except Exception as ex:
            traceback.print_exc()
            logger.critical(f"Exception when fetching Product Details for PID {instance.product_id}: {ex}")
    
    logger.info(f"Finished fetching archived products for Category: {category}")



if __name__ == '__main__':
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)
    process_archived_pids("headphones")
    time.sleep(120)
    process_archived_pids("smarphones")
    time.sleep(120)
    process_archived_pids("ceiling fan")
    time.sleep(120)
    process_archived_pids("refrigerator")
    time.sleep(120)
    process_archived_pids("washing machine")
