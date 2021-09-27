import argparse
import concurrent.futures
import os
import random
import signal
import sys
import time
import traceback
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlitedict import SqliteDict

import cache
import db_manager
import parse_data
import proxy
from utils import category_to_domain, create_logger, domain_map, domain_to_db

logger = create_logger('fetch_archived')

cache = cache.Cache()
cache.connect('master', use_redis=True)

cache_file = 'cache.sqlite3'

today = datetime.today().strftime("%d-%m-%y")

try:
    OS = config('OS')
except UndefinedValueError:
    OS = 'Linux'

try:
    multithreading = config('MULTITHREADING', cast=bool)
except UndefinedValueError:
    multithreading = False

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

my_proxy = proxy.Proxy(OS=OS, use_tor=False, use_proxy=False)

try:
    use_tor = config('USE_TOR', cast=bool)
except:
    use_tor = False

last_product_detail = False
terminate = False

# Database Session setup
credentials = db_manager.get_credentials()

# Mockup Headers
headers = {
    'authority': 'www.amazon.in',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-US,en;q=0.9',
}


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


def scrape_product_detail(category, product_urls, instance_id=None):
    global my_proxy
    global cache, cache_file
    # session = requests.Session()
    server_url = 'https://' + category_to_domain[category]

    if not isinstance(product_urls, list):
        product_urls = [product_urls]
    
    try:
        engine, SessionFactory = db_manager.connect_to_db(config('DB_NAME'), credentials)

        for product_url in product_urls:
            product_id = parse_data.get_product_id(product_url)

            logger.info(f"Going to Details page for PID {product_id}")
                
            try:
                response = my_proxy.get(server_url, headers=headers)
            except TimeoutError:
                logger.warning(f"For PID {product_id}, couldn't go to {server_url}, request timed out. Skipping this for now...")
                with SqliteDict(cache_file, autocommit=True) as mydict:
                    key = f"INCOMPLETE_ARCHIVED_PIDS"
                    if key not in mydict:
                        mydict[key] = {}
                    mydict[key][product_id] = server_url + product_url
                continue
            
            setattr(my_proxy, 'category', category)
            
            assert response.status_code == 200
            time.sleep(3)

            break_flag = False

            while True:
                try:
                    response = my_proxy.get(server_url + product_url, product_url=product_url, headers=headers)
                except TimeoutError:
                    with SqliteDict(cache_file, autocommit=True) as mydict:
                        key = f"INCOMPLETE_ARCHIVED_PIDS"
                        if key not in mydict:
                            mydict[key] = {}
                        mydict[key][product_id] = server_url + product_url
                    
                    logger.warning(f"For url {server_url + product_url}, request timed out. Skipping this for now...")
                    break_flag = True
                    break
                
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

            if break_flag == True:
                continue
            
            details['product_id'] = product_id # Add the product ID

            # Store to cache first
            with SqliteDict(cache_file, autocommit=True) as mydict:
                mydict[f"ARCHIVED_DETAILS_{product_id}"] = details
            
            with db_manager.session_scope(SessionFactory) as session:
                instance = session.query(db_manager.ProductListing).filter(db_manager.ProductListing.product_id == product_id).first()
                if instance is None:
                    logger.warning(f"For PID {product_id}, no such instance in ProductListing")
                    continue

                instance.date_completed = datetime.now()
                
                required_details = ["num_reviews", "curr_price", "avg_rating"]

                for field in required_details:
                    if field == "num_reviews" and details.get('num_reviews') is not None:
                        num_reviews = int(details[field].split()[0].replace(',', '').replace('.', ''))
                        if hasattr(instance, "total_ratings"):
                            setattr(instance, "total_ratings", num_reviews)
                    elif field == "curr_price" and details.get('curr_price') is not None:
                        price = float(details[field].replace(',', ''))
                        if hasattr(instance, "price"):
                            setattr(instance, "price", price)
                    elif field == "avg_rating" and details.get('avg_rating') is not None and isinstance(details.get('avg_rating'), float):
                        avg_rating = details['avg_rating']
                        if hasattr(instance, "avg_rating"):
                            setattr(instance, "avg_rating", avg_rating)
                
                session.add(instance)
            cache.set(f"TIMESTAMP_ID_{instance_id}", 1, timeout=60 * 20)
    finally:
        db_manager.close_all_db_connections(engine, SessionFactory)


def process_archived_pids(category, top_n=None, instance_id=None, num_instances=None, num_threads=5):
    global multithreading
    from collections import OrderedDict

    from sqlalchemy import asc, desc
    global cache_file
    global credentials
    global OS

    if OS == 'Linux':
        pid = os.getpid()
        with open('pid.txt', 'w') as f:
            f.write(str(pid))
    else:
        raise ValueError(f"Only supported in Linux")

    with open('num_instances.txt', 'w') as f:
        f.write(str(num_instances))
    
    with open('instance_id.txt', 'w') as f:
        f.write(str(instance_id))

    with SqliteDict(cache_file, autocommit=True) as mydict:
        if f"ARCHIVED_INFO_{category}" not in mydict:
            mydict[f"ARCHIVED_INFO_{category}"] = {}

    _, SessionFactory = db_manager.connect_to_db(config('DB_NAME'), credentials)

    _info = OrderedDict()
    info = OrderedDict()

    with db_manager.session_scope(SessionFactory) as session:
        queryset = session.query(db_manager.ProductListing).filter(db_manager.ProductListing.is_active == False, db_manager.ProductListing.category == category, (db_manager.ProductListing.date_completed == None) | (db_manager.ProductListing.date_completed <= datetime.today().date() - timedelta(days=1))).order_by(asc('category')).order_by(desc('total_ratings'))
        logger.info(f"Found {queryset.count()} inactive products totally")
        for instance in queryset:
            _info[instance.product_id] = instance.product_url

    if num_instances is None:
        for idx, pid in enumerate(_info):
            if top_n is not None and idx >= top_n:
                break
            info[pid] = _info[pid]
    else:
        num_ids = (len(_info.keys()) // num_instances) + 1
        for idx, pid in enumerate(_info):
            if idx >= (num_ids * instance_id) and idx < (num_ids * (instance_id + 1)):
                info[pid] = _info[pid]

    
    if multithreading == True and num_threads is not None and num_threads > 0:
        urls = list(info.values())        
        split_urls = [urls[(i*len(urls)) // num_threads: ((i+1)*len(urls)) // num_threads] for i in range(num_threads)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_category = {executor.submit(scrape_product_detail, category, urls, instance_id): category for urls in split_urls}

        for future in concurrent.futures.as_completed(future_to_category):
            category = future_to_category[future]
            try:
                _ = future.result()
            except Exception as exc:
                logger.critical('%r generated an exception: %s' % (category, exc))
                logger.critical(f"Thread {category} generated an exception {exc}")
                logger.critical("".join(traceback.TracebackException.from_exception(exc).format()))
            else:
                logger.info(f"Category {category} is done!")

        if os.path.exists('pid.txt'):
            os.remove('pid.txt')

    else:
        for pid in info:
            url = info[pid]
            logger.info(f"Scraping Details for: {pid}")
            try:
                scrape_product_detail(category, url)
                logger.info(f"Finished details for this product: {pid}")

                with SqliteDict(cache_file, autocommit=True) as mydict:
                    date_completed = datetime.now()
                    mydict[f"ARCHIVED_INFO_{category}"][pid] = date_completed
            
            except Exception as ex:
                traceback.print_exc()
                logger.critical(f"Exception when fetching Product Details for PID {pid}: {ex}")
    
    logger.info(f"Updated date_completed!")

    logger.info(f"Finished fetching archived products for Category: {category}")


def update_archive_listing(category, table='ProductListing'):
    from sqlalchemy import asc, desc
    global cache, cache_file
    global credentials

    _, Session = db_manager.connect_to_db(config('DB_NAME'), credentials)

    _table = db_manager.table_map[table]

    # Update only important details
    required_details = ["num_reviews", "curr_price", "avg_rating"]

    archived_pids = cache.smembers(f"ARCHIVED_PRODUCTS_{category}")
    archived_pids = [pid.decode() for pid in archived_pids]

    with SqliteDict(cache_file, autocommit=False) as mydict:
        with db_manager.session_scope(Session) as session:
            for pid in archived_pids:
                instance = session.query(_table).filter(_table.product_id == pid).first()
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
                    elif field == "avg_rating" and detail.get('avg_rating') is not None and isinstance(detail.get('avg_rating'), float):
                        avg_rating = detail['avg_rating']
                        if hasattr(instance, "avg_rating"):
                            setattr(instance, "avg_rating", avg_rating)
                session.add(instance)

    logger.info(f"Updated Archive Products for category: {category}")


if __name__ == '__main__':
    # Start a session using the existing engine
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--categories', help='List of all categories (comma separated)', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('--find_archived_products', help='Find all archived products from existing ones on Product Listing', default=False, action='store_true')
    parser.add_argument('--process_archived_pids', help='Fetch Product Details of potentially Archived Products', default=False, action='store_true')
    parser.add_argument('--update_archive_listing', help='Updated Product Listing of Archived Products', default=False, action='store_true')
    parser.add_argument('--instance_id', help='Get the instance id (from 0 to num_instances - 1)', default=None, type=int)
    parser.add_argument('--num_instances', help='Get the number of instances', default=None, type=int)
    parser.add_argument('--num_threads', help='Get the number of worker threads to scrape the Archive Details', default=None, type=int)
    parser.add_argument('--top_n', help='Get only the Top N SKUs', default=None, type=int)

    args = parser.parse_args()

    _categories = args.categories
    _find_archived_products = args.find_archived_products
    _process_archived_pids = args.process_archived_pids
    _update_archive_listing = args.update_archive_listing

    _instance_id = args.instance_id
    _num_instances = args.num_instances
    _num_threads = args.num_threads
    _top_n = args.top_n

    if _instance_id is not None or _num_instances is not None:
        if _instance_id is None or _num_instances is None:
            raise ValueError(f"Both --instance_id and --num_instances must be specified")
        if _instance_id >= _num_instances:
            raise ValueError(f"instance_d must be between 0 to num_intances - 1")
        assert _instance_id >= 0 and _instance_id < _num_instances
    
    if _num_threads is not None:
        assert _num_threads > 0 and _num_threads <= 100

    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)

    if _categories is not None:
        categories = _categories
    if _find_archived_products == True:
        if _categories is None:
            raise ValueError(f"Need to specify list of categories for processing archived PIDs")
        for category in _categories:
            process_archived_pids(category=category, top_n=_top_n)
    if _process_archived_pids == True:
        if _categories is None:
            raise ValueError(f"Need to specify list of categories for processing archived PIDs")
        for category in _categories:
            process_archived_pids(category, top_n=_top_n, instance_id=_instance_id, num_instances=_num_instances, num_threads=_num_threads)
            logger.info(f"Completed category {category}!")
    if _update_archive_listing == True:
        if _categories is None:
            raise ValueError(f"Need to specify list of categories for updating archived PIDs")
        for category in _categories:
            update_archive_listing(category)
