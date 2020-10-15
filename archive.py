import argparse
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


def find_archived_products(session, table='ProductListing'):
    from sqlalchemy import asc, desc
    global cache, db_session
    
    _table = db_manager.table_map[table]

    queryset = db_session.query(_table).order_by(asc('category')).order_by(asc('short_title')).order_by(asc('duplicate_set')).order_by(desc('total_ratings'))

    prev = None

    count = 0
    
    for instance in queryset:
        if prev is None:
            prev = instance
            continue
        
        if prev.category == instance.category and prev.duplicate_set != instance.duplicate_set and prev.short_title == instance.short_title:
            # Possibly an archived product
            # Constraint: price(prev) >= price(curr), so prev is more recent
            cache.sadd(f"ARCHIVED_PRODUCTS_{instance.category}", instance.product_id)
            count += 1
            continue

        prev = instance
        continue

    logger.info(f"Found {count} archived products totally")


def update_archive_listing(session, category, table='ProductListing'):
    from sqlalchemy import asc, desc
    global cache, db_session, cache_file

    _table = db_manager.table_map[table]

    # Update only important details
    required_details = ["num_reviews", "curr_price"]

    archived_pids = cache.smembers(f"ARCHIVED_PRODUCTS_{category}")
    archived_pids = [pid.decode() for pid in archived_pids]

    with SqliteDict(cache_file, autocommit=False) as mydict:
        for pid in archived_pids:
            instance = db_session.query(_table).filter(_table.product_id == pid).first()
            if instance is None:
                logger.warning(f"For PID {pid}, no such instance in {table}")
                continue

            detail = mydict.get(f"ARCHIVED_DETAILS_{pid}")

            if detail is None:
                logger.warning(f"For PID {pid}, no such detail info in cache")
                continue

            for field in required_details:
                if field == "num_reviews" and detail.get('num_reviews') is not None:
                    num_reviews = int(detail[field].split()[0].replace(',', '').replace('.', ''))
                    if hasattr(instance, "total_ratings"):
                        setattr(instance, "total_ratings", num_reviews)
                elif field == "curr_price" and detail.get('curr_price') is not None:
                    price = float(detail[field].replace(',', ''))
                    if hasattr(instance, "price"):
                        setattr(instance, "price", price)
        
            try:
                db_session.commit()
            except Exception as ex:
                db_session.rollback()
                logger.critical(f"Error when trying to commit the session for PID {pid}: {ex}")
    
    logger.info(f"Updated Archive Products for category: {category}")


if __name__ == '__main__':
    # Start a session using the existing engine
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--categories', help='List of all categories (comma separated)', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('--find_archived_products', help='Find all archived products from existing ones on Product Listing', default=False, action='store_true')
    parser.add_argument('--process_archived_pids', help='Fetch Product Details of potentially Archived Products', default=False, action='store_true')
    parser.add_argument('--update_archive_listing', help='Updated Product Listing of Archived Products', default=False, action='store_true')

    args = parser.parse_args()

    _categories = args.categories
    _find_archived_products = args.find_archived_products
    _process_archived_pids = args.process_archived_pids
    _update_archive_listing = args.update_archive_listing

    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)

    if _categories is not None:
        categories = _categories
    if _find_archived_products == True:
        find_archived_products(db_session)
    if _process_archived_pids == True:
        if _categories == False:
            raise ValueError(f"Need to specify list of categories for processing archived PIDs")
        for category in _categories:
            process_archived_pids(category)
            time.sleep(120)
    if _update_archive_listing == True:
        if _categories is None:
            raise ValueError(f"Need to specify list of categories for updating archived PIDs")
        for category in _categories:
            update_archive_listing(db_session, category)
