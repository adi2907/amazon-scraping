import argparse
import concurrent.futures
import itertools
import json
import os
import pickle
import random
import re

from sqlalchemy.sql.operators import is_
import parse_data
import signal
import sqlite3
import sys
import time
import traceback
from collections import OrderedDict
from copy import deepcopy
from datetime import date, datetime, timedelta
from string import Template

import requests
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from sqlalchemy import asc, desc, text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlitedict import SqliteDict

import cache
import db_manager
import parse_data
import proxy
from utils import (category_to_domain, create_logger,
                   customer_reviews_template, domain_map, domain_to_db,
                   listing_categories, listing_templates, qanda_template,
                   subcategory_map, url_template)

logger = create_logger('scraper')

error_logger = create_logger('errors')

exception_logger = create_logger('threads')

headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

cookies = dict()

cache = cache.Cache()

today = datetime.today().strftime("%d-%m-%y")

try:
    USE_REDIS = config('USE_REDIS')
    if USE_REDIS == 'True':
        USE_REDIS = True
    else:
        USE_REDIS = False
except UndefinedValueError:
    USE_REDIS = False

(f"USE_REDIS = {USE_REDIS}")

try:
    USE_PROXY = config('USE_PROXY', cast=bool)
except UndefinedValueError:
    USE_PROXY = True

logger.info(f"USE_PROXY = {USE_PROXY}")

cache.connect('master', use_redis=USE_REDIS)

try:
    OS = config('OS')
except UndefinedValueError:
    OS = 'Windows'

try:
    DEVELOPMENT = config('DEVELOPMENT', cast=bool)
except:
    DEVELOPMENT = False


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


try:
    use_multithreading = config('MULTITHREADING')
    if use_multithreading == 'True':
        use_multithreading = True
    else:
        use_multithreading = False
except UndefinedValueError:
    use_multithreading = False

logger.info(f"Multithreading is - {use_multithreading}")


try:
    use_cache = config('use_cache')
    if use_cache == 'True':
        use_cache = True
    else:
        use_cache = False
except UndefinedValueError:
    use_cache = False

logger.info(f"use_cache is - {use_cache}")

try:
    cache_file = config('cache_file')
except UndefinedValueError:
    cache_file = 'cache.sqlite3'

logger.info(f"Using Sqlite3 Cache File = {cache_file}")

try:
    USE_DB = config('USE_DB')
    if USE_DB == 'False':
        USE_DB = False
    else:
        USE_DB = True
except UndefinedValueError:
    USE_DB = True


try:
    USE_TOR = config('USE_TOR', cast=bool)
except UndefinedValueError:
    USE_TOR = False

logger.info(f"USE_DB is - {USE_DB}")

# Start the session
session = requests.Session()

# Use a proxy if possible
my_proxy = proxy.Proxy(OS=OS, use_proxy=USE_PROXY) if USE_PROXY == True else None


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
#session_factory = sessionmaker(bind=engine)
#Session = scoped_session(session_factory)

db_session = Session()

last_product_detail = False
terminate = False

pids = set()


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


def store_page(filename, html=None):
    if html is None:
        return
    try:
        DUMP_DIR = os.path.join(os.getcwd(), 'html')
        if not os.path.exists(DUMP_DIR):
            os.mkdir(DUMP_DIR)
        with open(os.path.join(DUMP_DIR, filename), 'wb') as f:
            f.write(html)
        return
    except Exception as ex:
        logger.critical(f"Error when trying to store html: {ex}")
        return


def store_to_cache(key, value, html=None):
    global cache_file, use_cache
    global cache
    try:
        with SqliteDict(cache_file, autocommit=True) as mydict:
            mydict[key] = value
        return True
    except RecursionError:
        error_logger.critical(f"Recursion Depth exceeded when trying to store key -> {key}")
        store_page(f"{key}.html", html=html)
        return False
    except Exception as ex:
        error_logger.critical(f"Error when trying to store key -> {key}: {ex}")
        store_page(f"{key}.html", html=html)
        return False
 

def remove_from_cache(category):
    global cache
    value = cache.atomic_decrement(f"COUNTER_{category}")
    logger.info(f"For category {category}, decremented counter. Counter is now {value}")
    if value <= 0:
        logger.info(f"All processes finished category - {category}. Resetting the set now...")
        cache.delete(f"COUNTER_{category}")
        cache.delete(f"{category}_PIDS")


def process_product_detail(category, base_url, num_pages, change=False, server_url='https://amazon.in', no_listing=False, detail=False, jump_page=0, subcategories=None, no_refer=False, threshold_date=None, listing_pids=None, qanda_pages=50, review_pages=500, override=False):
    """
    Function which processes the details URL of all given product ids, one by one

    Args:
        category ([str]): Category of the product 
        base_url ([str]): Base URL (domain name)
        server_url (str, optional): Server URL. Defaults to 'https://amazon.in'.
        jump_page (int, optional): Whether or not to jump to page N of reviews directly. Defaults to 0.
        threshold_date ([datetime]): The threshold date for scraping QandAs + Reviews.
        qanda_pages (int, optional): Number of QandA pages. Defaults to 50.
        review_pages (int, optional): Number of Review pages. Defaults to 500.
        listing_pids (list): List of all the product_ids to scrape
    """
    
    global cache
    global headers, cookies
    global last_product_detail
    global cache
    global speedup
    global use_multithreading
    global cache_file, use_cache
    global USE_DB, USE_PROXY
    global pids
    
    if use_multithreading == True:
        my_proxy = proxy.Proxy(OS=OS, stream_isolation=True, server_url=server_url, use_proxy=USE_PROXY) # Separate Proxy per thread
        try:
            my_proxy.change_identity()
        except:
            logger.warning('No Proxy available via Tor relay. Mode = Normal')
            logger.newline()
            my_proxy = None

        if my_proxy is None:
            session = requests.Session()
        
    else:
        my_proxy = proxy.Proxy(OS=OS, stream_isolation=False, server_url=server_url, use_proxy=USE_PROXY) # Separate Proxy per thread
        try:
            my_proxy.change_identity()
        except:
            logger.warning('No Proxy available via Tor relay. Mode = Normal')
            logger.newline()
            my_proxy = None

        if my_proxy is None:
            session = requests.Session()

    credentials = db_manager.get_credentials()
    domain = category_to_domain[category]
    db_name = domain_to_db[domain]
    engine, SessionFactory = db_manager.connect_to_db(db_name, credentials)
    
    for idx, product_id in enumerate(listing_pids):
        curr_page = 1
        curr_url = base_url

        factor = 0
        cooldown = False

        rescrape = 0

        product_url = ''

        try:
            # Check if the product_id is a member of the set
            # This is needed to ensure that only a single instance, single thread can scrape a unique product_id
            if (product_id not in pids) and (cache.sismember(f"{category}_PIDS", product_id) == False):
                logger.info(f"PID {product_id} not in set")
                pids.add(product_id)
                cache.atomic_set_add(f"{category}_PIDS", product_id)
            else:
                # Skip this. Already done
                logger.info(f"PID {product_id} in set. Skipping this product")
                with db_manager.session_scope(Session) as db_session:
                    obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
                    if obj is not None:
                        continue

            if product_id is not None:
                _date = threshold_date
                with db_manager.session_scope(SessionFactory) as db_session:
                    # Check if this product_id exists in ProductDetails
                    obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
                
                    recent_date = None

                    if obj is not None:
                        # It exists already!
                        if obj.completed == True:
                            # Query Listing Table for surity
                            a = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': f'{product_id}'}))
                            if a is None:
                                error_logger.info(f"{idx}: Product with ID {product_id} not in ProductListing. Skipping this, as this will give an integrityerror")
                                continue
                            else:
                                # Get the most recently scraped product_id of this duplicate_set
                                recent_obj = db_session.query(db_manager.ProductListing).filter(db_manager.ProductListing.duplicate_set == a.duplicate_set).order_by(desc(text('detail_completed'))).first()
                                if recent_obj is None:
                                    error_logger.info(f"{idx}: Product with ID {product_id} not in duplicate set filter")
                                    continue

                                # Check if the duplicate_set is already there in the cache
                                if cache.sismember("DUPLICATE_SETS", str(recent_obj.duplicate_set)):
                                    error_logger.info(f"{idx}: Product with ID {product_id} is a duplicate. Skipping this...")
                                    continue

                                if recent_obj.duplicate_set is not None:
                                    # Add to the duplicate_set list in the cache
                                    cache.sadd(f"DUPLICATE_SETS", str(recent_obj.duplicate_set))
                                
                                product_url = recent_obj.product_url
                                
                                recent_date = recent_obj.detail_completed

                            if override == False and recent_date is not None:
                                # This product has already been scraped before. Set the date to the date of last scraped
                                _date = recent_date
                                logger.info(f"Set date as {_date}")
                                delta = datetime.now() - _date
                                if delta.days < 6:
                                    logger.info(f"Skipping this product. within the last week")
                                    continue
                            else:
                                # Set it to the default threshold_date (Aug 1)
                                _date = threshold_date

                        if hasattr(obj, 'product_details') and obj.product_details in (None, {}, '{}'):
                            # Some important data is NULL. Let's scrape this again properly
                            rescrape = 1
                            error_logger.info(f"Product ID {product_id} has NULL product_details. Scraping it again...")
                            if hasattr(obj, 'completed') and obj.completed is None:
                                # Scrape from scratch
                                # 2 means to do from scratch, while 1 means to do it partially (only details page, without qandas + reviews)
                                rescrape = 2
                        else:
                            if _date != threshold_date:
                                # New product, since it's not in ProductDetails
                                rescrape = 0
                            else:
                                rescrape = 2
                                if override == True:
                                    pass
                                else:
                                    logger.info(f"Product with ID {product_id} already in ProductDetails. Skipping this product")
                                    error_logger.info(f"Product with ID {product_id} already in ProductDetails. Skipping this product")
                                    continue
                    else:
                        error_logger.info(f"{idx}: Product with ID {product_id} not in DB. Scraping this from scratch")
                        rescrape = 0
                        a = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': f'{product_id}'}))
                        if a is None:
                            error_logger.info(f"{idx}: Product with ID {product_id} not in ProductListing. Skipping this, as this will give an integrityerror")
                            continue
                        else:
                            # Scrape only the most recent object per duplicate set
                            recent_obj = db_session.query(db_manager.ProductListing).filter(db_manager.ProductListing.duplicate_set == a.duplicate_set).order_by(desc(text('date_completed'))).first()
                            if recent_obj is None:
                                error_logger.info(f"{idx}: Product with ID {product_id} not in duplicate set filter")
                                continue
                            
                            instances = db_manager.query_table(db_session, 'ProductListing', 'all', filter_cond=({'duplicate_set': f'{a.duplicate_set}'}))
                            
                            if cache.sismember("DUPLICATE_SETS", str(recent_obj.duplicate_set)):
                                error_logger.info(f"{idx}: Product with ID {product_id} is a duplicate. Skipping this...")
                                continue

                            if recent_obj.duplicate_set is not None:
                                cache.sadd(f"DUPLICATE_SETS", str(recent_obj.duplicate_set))
                            
                            product_url = recent_obj.product_url
                            
                            recent_date = recent_obj.detail_completed

                        if override == False and recent_date is not None:
                            # This has already been scraped before. Set the checkpoint
                            _date = recent_date
                            logger.info(f"Set date as {_date}")
                            delta = datetime.now() - _date
                            if delta.days < 6:
                                logger.info(f"Skipping this product. within the last week")
                                continue
                            
                        elif override == False and hasattr(recent_obj, 'detail_completed') and recent_obj.detail_completed is not None:
                            # Go until this point only
                            _date = recent_obj.detail_completed
                            logger.info(f"Set date as {_date}")
                            delta = datetime.now() - _date
                            if delta.days < 6:
                                logger.info(f"Skipping this product. within the last week")
                                continue
                        else:
                            # Default threshold date
                            _date = threshold_date
            
            if override == True:
                # Override = True means that full details will always be scraped
                _date = threshold_date
                rescrape = 2
                logger.info(f"Overriding: Scraping FULL Details for {product_id}")

            if rescrape == 0:
                _ = scrape_product_detail(product_url,category, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=_date, listing_url=curr_url, total_ratings=0)
            elif rescrape == 1:
                # Only details
                _ = scrape_product_detail(product_url,category, review_pages=0, qanda_pages=0, threshold_date=_date, listing_url=curr_url, total_ratings=0, incomplete=True)
            elif rescrape == 2:
                # Whole thing again
                _ = scrape_product_detail(product_url,category, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=_date, listing_url=curr_url, total_ratings=0, incomplete=True, jump_page=jump_page)

            idx += 1
        except Exception as ex:
            traceback.print_exc()
            if product_id is not None:
                logger.critical(f"During scraping product details for ID {product_id}, got exception: {ex}")
            else:
                logger.critical(f"Product ID is None, got exception: {ex}")

        if last_product_detail == True:
            logger.info("Completed pending products. Exiting...")
            return

        if my_proxy is not None and no_refer == False:
            if num_products is None or idx <= num_products:
                response = my_proxy.get(curr_url, server_url + product_url)
                time.sleep(random.randint(3, 5)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
            elif num_products is not None and idx > num_products:
                # We're done for this product
                logger.info(f"Scraped {num_products} for category {category}. Moving to the next one")
                error_logger.info(f"Scraped {num_products} for category {category}. Moving to the next one")
                overflow = True
                continue
        
        cooldown = False

        time.sleep(4)

        change = True


def fetch_category(category, base_url, num_pages, change=False, server_url='https://amazon.in', no_listing=False, detail=False, jump_page=0, subcategories=None, no_refer=False, threshold_date=None, listing_pids=None):
    # global my_proxy, session
    global headers, cookies
    global last_product_detail
    global cache
    global speedup
    global use_multithreading
    global cache_file, use_cache
    global USE_DB
    global pids

    if use_multithreading == True:
        my_proxy = proxy.Proxy(OS=OS, stream_isolation=True, server_url=server_url) # Separate Proxy per thread
        try:
            my_proxy.change_identity()
        except:
            logger.warning('No Proxy available via Tor relay. Mode = Normal')
            logger.newline()
            my_proxy = None

        if my_proxy is None:
            session = requests.Session()
        
        db_session = Session()
        #db_session = scoped_session(Session)
    else:
        my_proxy = proxy.Proxy(OS=OS, stream_isolation=False, server_url=server_url) # Separate Proxy per thread
        try:
            my_proxy.change_identity()
        except:
            logger.warning('No Proxy available via Tor relay. Mode = Normal')
            logger.newline()
            my_proxy = None

        if my_proxy is None:
            session = requests.Session()
        
        db_session = Session()
    
    try:
        logger.info(f"Now at category {category}, with num_pages {num_pages}")

        if detail == False:
            value = cache.atomic_increment(f"COUNTER_{category}")
            logger.info(f"Now, for category - {category}, counter = {value}")

        final_results = dict()

        idx = 1 # Total number of scraped product details
        curr_serial_no = 1 # Serial Number from the top
        overflow = False

        final_results[category] = dict()

        if my_proxy is not None:
            if change == True:
                change = False
                my_proxy.change_identity()
                time.sleep(random.randint(2, 5))
            logger.info(f"Proxy Cookies = {my_proxy.cookies}")
            if no_refer == True:
                response = my_proxy.get(base_url)
            else:
                response = my_proxy.get(base_url,server_url)
            setattr(my_proxy, 'category', category)
            logger.info(f"Proxy Cookies = {my_proxy.cookies}")
        else:
            response = session.get(base_url, headers=headers, cookies=cookies)
        
        if response.status_code != 200:
            logger.newline()
            logger.newline()
            logger.error(response.content)
            logger.newline()
            logger.newline()
            raise ValueError(f'Error: Got code {response.status_code}')
        
        if hasattr(response, 'cookies'):
            cookies = {**cookies, **dict(response.cookies)}
        
        time.sleep(5) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
        curr_page = 1
        curr_url = base_url

        factor = 0
        cooldown = False

        while curr_page <= num_pages:
            time.sleep(6) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
            if (detail == False) or (detail == True and DEVELOPMENT == False):
                html = response.content
            else:
                # Open from the dump directory (Listing files are here)
                DUMP_DIR = os.path.join(os.getcwd(), 'dumps')
                with open(os.path.join(DUMP_DIR), f"listing_{category}_{curr_page}.html", "rb") as f:
                    html = f.read()
            soup = BeautifulSoup(html, 'lxml')

            if DEVELOPMENT == True and category == 'headphones' and detail == False:
                logger.info(f"Dumping page {curr_page} for headphones")
                DUMP_DIR = os.path.join(os.getcwd(), 'dumps')
                if not os.path.exists(DUMP_DIR):
                    os.mkdir(DUMP_DIR)
                
                with open(os.path.join(DUMP_DIR, f'LISTING_{category}_PAGE_{curr_page}.html'), 'wb') as f:
                    f.write(html)

            product_info, curr_serial_no = parse_data.get_product_info(soup, curr_serial_no=curr_serial_no)

            final_results[category][curr_page] = product_info

            page_element = soup.find("ul", class_="a-pagination")
            
            # Check if captcha or final page
            if page_element is None:
                if my_proxy is None:
                    response = session.get(base_url, headers=headers, cookies=cookies)
                else:
                    response = my_proxy.get(base_url,curr_url)
                
                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}
                
                logger.warning(f"Curr Page = {curr_page}. Pagination Element is None")

                # Check if this is a CAPTCHA page
                captcha_id = "captchacharacters"
                captcha_node = soup.find("input", id=captcha_id)
                if captcha_node is not None:
                    # We need to retry
                    if factor >= 4:
                        if cooldown == False:
                            logger.critical(f"Time limit exceeded during backoff. Cooling down for sometime before trying...")
                            factor = 0
                            time.sleep(random.randint(200, 350))
                            my_proxy.change_identity()
                            cooldown = True
                            continue
                        else:
                            cooldown = False
                            logger.critical("Time limit exceeded during backoff even after cooldown. Shutting down...")
                            time.sleep(3)
                            break

                    logger.warning(f"Encountered a CAPTCHA page. Using exponential backoff. Current Delay = {my_proxy.delay}")
                    factor += 1
                    my_proxy.delay *= 2
                    continue
                else:
                    # This is probably the last page
                    time.sleep(3)
                    break
            

            listing = []

            page_results = dict()

            # All results from category in page_results
            page_results[category] = final_results[category]

            if detail == False:
                temp = deepcopy(page_results)

                for title in temp[category][curr_page]:
                    value = temp[category][curr_page][title]
                    
                    if 'total_ratings' not in value or 'price' not in value or value['total_ratings'] is None or value['price'] is None:
                        continue
                    
                    total_ratings = int(value['total_ratings'].replace(',', '').replace('.', ''))
                    price = int(value['price'][1:].replace(',', '').replace('.', ''))

                    small_title = ' '.join(word.lower() for word in title.split()[:6])

                    product_id = value['product_id']

                    duplicate = False

                    t = listing

                    for i, item in enumerate(t):
                        a = (item['small_title'] == small_title)
                        b = ((abs(item['total_ratings'] - total_ratings) / max(item['total_ratings'], total_ratings)) < 0.1)
                        c = ((abs(item['price'] - price) / max(item['price'], price)) < 0.1)
                        if ((a & b) | (b & c) | (c & a)):
                            # Majority function
                            logger.info(f"Found duplicate match! For title - {small_title}")
                            logger.info(f"Existing product is {title}, but old one is {item['title']}")
                            
                            pid = item['product_id']
                            a = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': f'{pid}'}))
                            if a is not None and a.is_duplicate == True:
                                if final_results[category][curr_page][item['title']].get('is_duplicate') != True:
                                    final_results[category][curr_page][item['title']]['is_duplicate'] = True
                                    try:
                                        del listing[i]
                                    except:
                                        pass
                                    continue
                            else:
                                if a is None:
                                    final_results[category][curr_page][item['title']]['is_duplicate'] = True
                            
                            duplicate = True
                            break
                    
                    if duplicate == True:
                        final_results[category][curr_page][title]['is_duplicate'] = True
                    else:
                        listing.append({'product_id': product_id, 'title': title, 'small_title': small_title, 'total_ratings': total_ratings, 'price': price})
                
                # Reset it
                listing = []
                del temp
            
            if subcategories is not None:
                for title in final_results[category][curr_page]:
                    product_url = final_results[category][curr_page][title]['product_url']
                    if product_url is not None:
                        product_id = parse_data.get_product_id(product_url)
                        if product_id is None:
                            continue
                        for subcategory in subcategories:
                            with SqliteDict(cache_file, autocommit=True) as mydict:
                                try:
                                    _set = mydict[f"SUBCATEGORIES_{category}_{subcategory}"]
                                except KeyError:
                                    _set = set()
                                _set.add(product_id)
                                mydict[f"SUBCATEGORIES_{category}_{subcategory}"] = _set

            if use_cache:
                # Store to cache first
                with SqliteDict(cache_file, autocommit=True) as mydict:
                    mydict[f"LISTING_{category}_PAGE_{curr_page}_{today}"] = page_results
            
            if detail == False: 
                if USE_DB == True:
                    try:
                        status = db_manager.insert_product_listing(db_session, page_results)
                        if not status:
                            raise ValueError("Yikes. Status is False")
                    except Exception as ex:
                        print(f"Exception when trung to store to Listing: {ex}")
                    finally:
                        try:
                            store_to_cache(f"LISTING_{category}_PAGE_{curr_page}_{today}", page_results, html=html)
                        except NameError:
                            store_to_cache(f"LISTING_{category}_PAGE_{curr_page}_{today}", page_results, html=None)
                        
                if no_listing == False:
                    # Dump the results of this page to the DB
                    if USE_DB == True:
                        try:
                            status = db_manager.insert_daily_product_listing(db_session, page_results)
                            if not status:
                                raise ValueError("Yikes. Status is False")
                        except:
                            try:
                                store_to_cache(f"DAILYLISTING_{category}_PAGE_{curr_page}_{today}", page_results, html=html)
                            except NameError:
                                store_to_cache(f"DAILYLISTING_{category}_PAGE_{curr_page}_{today}", page_results, html=None)
            

            if detail == True:
                for title in final_results[category][curr_page]:
                    product_url = final_results[category][curr_page][title]['product_url']
                    if product_url is not None:
                        if re.search('^/s\?.+=', product_url) is not None:
                            # Probably the heading. SKip this
                            logger.info(f"Encountered the heading -> Title = {title}")
                            error_logger.info(f"Encountered the heading -> Title = {title}")
                            logger.newline()
                            continue

                        product_id = None
                        rescrape = 0

                        try:
                            product_id = parse_data.get_product_id(product_url)
                            if product_id not in pids and (cache.sismember(f"{category}_PIDS", product_id) == False):
                            #if (product_id not in pids):
                                logger.info(f"PID {product_id} not in set")
                                pids.add(product_id)
                                cache.atomic_set_add(f"{category}_PIDS", product_id)
                            else:
                                logger.info(f"PID {product_id} in set. Skipping this product")
                                continue

                            if product_id is not None:
                                _date = threshold_date
                                obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
                                
                                recent_date = None

                                if obj is not None:
                                    if obj.completed == True:
                                        a = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': f'{product_id}'}))
                                        if a is None:
                                            error_logger.info(f"{idx}: Product with ID {product_id} not in ProductListing. Skipping this, as this will give an integrityerror")
                                            continue
                                        else:
                                            recent_obj = db_session.query(db_manager.ProductDetails).filter(db_manager.ProductDetails.duplicate_set == a.duplicate_set).order_by(desc(text('date_completed'))).first()
                                            if recent_obj is None:
                                                error_logger.info(f"{idx}: Product with ID {product_id} not in duplicate set filter")
                                                continue
                                                                                        
                                            if cache.sismember("DUPLICATE_SETS", str(recent_obj.duplicate_set)):
                                                error_logger.info(f"{idx}: Product with ID {product_id} is a duplicate. Skipping this...")
                                                continue

                                            #if hasattr(a, 'is_duplicate') and getattr(a, 'is_duplicate') == True:
                                            #    error_logger.info(f"{idx}: Product with ID {product_id} is a duplicate. Skipping this...")
                                            #    continue

                                            if recent_obj.duplicate_set is not None:
                                                cache.sadd(f"DUPLICATE_SETS", str(recent_obj.duplicate_set))
                                            
                                            recent_date = recent_obj.date_completed

                                        if recent_date is not None:
                                            _date = recent_date
                                            logger.info(f"Set date as {_date}")
                                            delta = datetime.now() - _date
                                            if delta.days < 6:
                                                logger.info(f"Skipping this product. within the last week")
                                                continue
                                            
                                        elif hasattr(obj, 'date_completed') and obj.date_completed is not None:
                                            # Go until this point only
                                            _date = obj.date_completed
                                            logger.info(f"Set date as {_date}")
                                            delta = datetime.now() - _date
                                            if delta.days < 6:
                                                logger.info(f"Skipping this product. within the last week")
                                                continue
                                        else:
                                            _date = threshold_date

                                    if hasattr(obj, 'product_details') and obj.product_details in (None, {}, '{}'):
                                        rescrape = 1
                                        error_logger.info(f"Product ID {product_id} has NULL product_details. Scraping it again...")
                                        if hasattr(obj, 'completed') and obj.completed is None:
                                            rescrape = 2
                                    else:
                                        if _date != threshold_date:
                                            rescrape = 0
                                        else:
                                            rescrape = 2
                                            logger.info(f"Product with ID {product_id} already in ProductDetails. Skipping this product")
                                            error_logger.info(f"Product with ID {product_id} already in ProductDetails. Skipping this product")
                                            continue
                                else:
                                    error_logger.info(f"{idx}: Product with ID {product_id} not in DB. Skipping this, as this may be a duplicate")
                                    continue

                            # Let's try to approximate the minimum reviews we need
                            value = final_results[category][curr_page][title]
                            if 'total_ratings' not in value or value['total_ratings'] is None:
                                total_ratings = None
                            else:
                                total_ratings = int(value['total_ratings'].replace(',', '').replace('.', ''))
                            
                            if rescrape == 0:
                                _ = scrape_product_detail(product_url,category, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=_date, listing_url=curr_url, total_ratings=total_ratings)
                            elif rescrape == 1:
                                # Only details
                                _ = scrape_product_detail(product_url,category, review_pages=0, qanda_pages=0, threshold_date=_date, listing_url=curr_url, total_ratings=total_ratings, incomplete=True)
                            elif rescrape == 2:
                                # Whole thing again
                                _ = scrape_product_detail(product_url,category, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=_date, listing_url=curr_url, total_ratings=total_ratings, incomplete=True, jump_page=jump_page)

                            idx += 1
                        except Exception as ex:
                            if product_id is not None:
                                logger.critical(f"During scraping product details for ID {product_id}, got exception: {ex}")
                            else:
                                logger.critical(f"Product ID is None, got exception: {ex}")

                        if last_product_detail == True:
                            logger.info("Completed pending products. Exiting...")
                            return final_results

                        if my_proxy is not None and no_refer == False:
                            if num_products is None or idx <= num_products:
                                response = my_proxy.get(curr_url, server_url + product_url)
                                time.sleep(random.randint(3, 5)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
                            elif num_products is not None and idx > num_products:
                                # We're done for this product
                                logger.info(f"Scraped {num_products} for category {category}. Moving to the next one")
                                error_logger.info(f"Scraped {num_products} for category {category}. Moving to the next one")
                                overflow = True
                                break

            # Delete the previous page results
            if category in final_results and curr_page in final_results[category]:
                del final_results[category][curr_page]
            
            next_page = page_element.find("li", class_="a-last")
            if next_page is None:
                logger.warning(f"Curr Page = {curr_page}. Next Page Element is None")
                if my_proxy is None:
                    response = session.get(base_url, headers=headers, cookies=cookies)
                else:
                    response = my_proxy.get(base_url)
                
                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}

                time.sleep(3)
            
            
            if next_page is None:
                logger.info("Next Page is None. Exiting catgory...")
                error_logger.info("Next Page is None. Exiting catgory...")
                break
            
            if next_page is not None:
                page_url = next_page.find("a")
                if page_url is None:
                    logger.warning(f"Curr Page = {curr_page}. Next Page Element is not None, but URL is None")
                    error_logger.warning(f"For category {category}, after page {curr_page}, next page is NOT none, but URL is none")
                    time.sleep(3)
                    break
                
                page_url = page_url.attrs['href']

                if my_proxy is None:       
                    response = session.get(server_url + page_url, headers={**headers, 'referer': curr_url}, cookies=cookies)
                else:
                    response = my_proxy.get(server_url + page_url, curr_url)
                
                if DEVELOPMENT == True and detail == False and category == 'headphones':
                    logger.info(f"CURR PAGE: {curr_page}, next URL = {server_url + page_url}")
                    
                    DUMP_DIR = os.path.join(os.getcwd(), 'dumps')
                    if not os.path.exists(DUMP_DIR):
                        os.mkdir(DUMP_DIR)
                    
                    with open(os.path.join(DUMP_DIR, f'LISTING_{category}_PAGE_{curr_page}.html'), 'wb') as f:
                        f.write(response.content)

                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}
                next_url = server_url + page_url

                time.sleep(5) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
            
            
            logger.info(f"Finished Scraping Listing Page {curr_page} of {category}")
            curr_url = next_url
            curr_page += 1

            cooldown = False

            if overflow == True:
                overflow = False
                break
        
        # Dump the category results
        results = dict()
        results[category] = final_results[category]
        
        if dump == True:
            if not os.path.exists(os.path.join(os.getcwd(), 'dumps')):
                os.mkdir(os.path.join(os.getcwd(), 'dumps'))
            
            with open(f'dumps/{category}.pkl', 'wb') as f:
                pickle.dump(results, f)
        
        if detail == False:
            if use_cache:
                # Store to cache first
                with SqliteDict(cache_file, autocommit=True) as mydict:
                    mydict[f"LISTING_{category}_PAGE_{curr_page}_{today}"] = results

            if USE_DB == True:
                # Insert to the DB
                try:
                    status = db_manager.insert_product_listing(db_session, results)
                    if status == False:
                        # Store to Cache
                        raise ValueError("Status is False")
                except:
                    try:
                        store_to_cache(f"LISTING_{category}_PAGE_{curr_page}_{today}", results, html=html)
                    except NameError:
                        store_to_cache(f"LISTING_{category}_PAGE_{curr_page}_{today}", results, html=None)

        logger.info(f"Finished Scraping the LAST page {curr_page} of {category}")

        time.sleep(4)

        change = True
    finally:
        if use_multithreading == True:
            db_session.close()
            # Session.remove()
        if detail == True:
            remove_from_cache(category)


def scrape_product_detail(product_url,category=None, review_pages=None, qanda_pages=None, threshold_date=None, listing_url=None, total_ratings=None, incomplete=False, jump_page=0):
    """
    Scrapes the product details + qanda + reviews, given the detail URL of a product

    Args:
        category ([str]): Category of the product
        product_url ([str]): Detail url of a Product

    Raises:
        ValueError: If the page is blocked due to captcha
    """
    global my_proxy, session
    global headers, cookies
    global cache
    global use_multithreading
    global cache_file, use_cache
    global USE_DB
    # set default domain
    domain = category_to_domain[category] if category is not None else 'amazon.in'
    
    server_url = 'https://www.' + domain

    product_id = parse_data.get_product_id(product_url)

    # Get DB credentials and connect to DB
    connection_params = db_manager.get_credentials()
    _engine, SessionFactory = db_manager.connect_to_db(domain_to_db[domain], connection_params)

    reviews_completed = False
    qanda_completed = False
    detail_completed = False
    
    #Assign threshold_date if none
    if threshold_date is None:
        threshold_date = datetime.datetime.now() - datetime.timedelta(months=3)
        
    logger.info(f"Going to Details page for PID {product_id}")

    with db_manager.session_scope(SessionFactory) as db_session:
        obj = db_session.query(db_manager.ProductListing).filter(db_manager.ProductListing.product_id == f'{product_id}').first()

        if obj is None:
            logger.critical(f"Row with PID {product_id} doesn't exist in ProductListing. Returning....")
            return {}
    
        duplicate_set = obj.duplicate_set

    dont_update = False

    # Store to cache first
    with SqliteDict(cache_file, autocommit=True) as mydict:
        try:
            _set = mydict[f"DETAILS_SET_{category}"]
        except KeyError:
            _set = set()
        _set.add(product_id)
        mydict[f"DETAILS_SET_{category}"] = _set

    if review_pages is None:
        review_pages = 500
    
    if qanda_pages is None:
        qanda_pages = 50
    
    if my_proxy is None:
        response = session.get(server_url, headers=headers,cookies=cookies)
    else:
        response = my_proxy.get(server_url)
        setattr(my_proxy, 'category', category)
    
    assert response.status_code == 200
    cookies = dict(response.cookies)
    time.sleep(3)

    is_completed = False

    REVIEWS_PER_PAGE = 10
    LIMIT = 3
    tries = 1
    while tries <= LIMIT:
        # Keep looping until scraping is complete for the product details (not reviews and Q&A)
        if my_proxy is None:
            response = session.get(server_url + product_url, headers=headers, cookies=cookies)
        else:
            if listing_url is not None:
                response = my_proxy.get(server_url + product_url, listing_url)
            else:
                response = my_proxy.get(server_url + product_url)
        
        assert response.status_code == 200
        
        if hasattr(response, 'cookies'):
            cookies = {**cookies, **dict(response.cookies)}
        
        time.sleep(10) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))

        final_results = dict()

        time.sleep(3) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
        html = response.content
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Get the product details 
        try:
            details = parse_data.get_product_data(soup, html=html)
            break
        except ValueError:
            tries+=1
            logger.warning(f"Written html to {category}_{product_url}.html")
            logger.warning(f"Couldn't parse product Details for {product_id}. Possibly blocked")
            logger.warning(f"Try No {tries}")         
            time.sleep(random.randint(3, 10) + random.uniform(0, 4)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))

    details['product_id'] = product_id # Add the product ID
    details['duplicate_set'] = duplicate_set
    
    # Check if the product is sponsored
    sponsored = parse_data.is_sponsored(product_url)

    if use_cache:
        # Store to local cache first
        with SqliteDict(cache_file, autocommit=True) as mydict:
            mydict[f"DETAILS_{product_id}"] = details
            mydict[f"IS_SPONSORED_{product_id}"] = sponsored
    
    # Validate some important fields
    important_fields = ['product_title', 'product_details', 'reviews_url', 'customer_qa']
    empty_fields = []
    for field in important_fields:
        if details.get(field) in [None, "", {}, []]:
            empty_fields.append(field)
    
    if empty_fields != []:
        msg = ','.join([field for field in empty_fields])
        logger.critical(f"{msg} fields are missing in product details" )
    

    if USE_DB == True:
        # Insert to the DB
        try:
            with db_manager.session_scope(SessionFactory) as db_session:
                status = db_manager.insert_product_details(db_session, details, is_sponsored=sponsored)
                # if Database insertion fails, store in cache to examine later
                if status == True:
                    detail_completed = True
                else:
                    try:
                        store_to_cache(f"DETAILS_{product_id}", details, html=html)
                    except NameError:
                        store_to_cache(f"DETAILS_{product_id}", details, html=None)
                    store_to_cache(f"IS_SPONSORED_{product_id}", sponsored)
        except:
            try:
                status = store_to_cache(f"DETAILS_{product_id}", details, html=html)
            except NameError:
                status = store_to_cache(f"DETAILS_{product_id}", details, html=None)
            _ = store_to_cache(f"IS_SPONSORED_{product_id}", sponsored)
    
    time.sleep(4)

    # Scrape QandA 
    logger.info(f"Scraping QandA for product {product_id}")
    qanda_url = details['customer_qa']
    if qanda_url is None:
        qanda_completed = True
    else:
        # Check last review currently in database. Assign threhold date as last reviewed date else default    
        last_qanda_date = db_manager.get_last_qanda_date(db_session,product_id)
        
        # If reviews exist in DB, scrape only till last review date
        if last_qanda_date is not None:
            threshold_date = last_qanda_date + datetime.timedeltatimedelta(days=1) if last_qanda_date > threshold_date else threshold_date
        
        # scrape reviews till threshold date sorted by most recent
        qanda_completed = scrape_qanda(server_url,qanda_url,product_id,threshold_date)
        if qanda_completed is not True:
            logger.error(f"QandA for product id {product_id} could not be completed")
    
    # Scrape reviews  
    logger.info(f"Scraping reviews for product {product_id}") 
    reviews_url = details['reviews_url']
    # Check if reviews_url exists, If no reviews, move on
    if reviews_url is None:
        reviews_completed = True
    else:
        # Check last review currently in database. Assign threhold date as last reviewed date else default    
        last_review_date = db_manager.get_last_review_date(db_session,product_id)
        
        # If reviews exist in DB, scrape only till last review date
        if last_review_date is not None:
            threshold_date = last_review_date + datetime.timedeltatimedelta(days=1) if last_review_date > threshold_date else threshold_date
        
        # scrape reviews till threshold date sorted by most recent
        reviews_completed = scrape_reviews(server_url,reviews_url,product_id,threshold_date)
        if reviews_completed is not True:
            logger.error(f"Reviews for product id {product_id} could not be completed")

    is_completed = detail_completed and qanda_completed and reviews_completed
    # Finally add completed flag and date_completed flag to ProductDetails table
    # Also update the detail_completed flag in Listing table
    if dont_update == True:
        pass
    else:
        with db_manager.session_scope(SessionFactory) as db_session:
            #Update ProductDetails table
            obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
            if obj is not None:
                logger.info(f"Product with ID {product_id} is completed = {is_completed}")             
                setattr(obj, 'completed', is_completed)
                if obj.completed == True:
                    obj.date_completed = datetime.now()

                    # Update ProductListing table detail_completed field
                    listing_obj = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': f'{product_id}'}))
                    if listing_obj is not None:
                        if hasattr(listing_obj, 'detail_completed'):
                            listing_obj.detail_completed = datetime.now()
                        else:
                            error_logger.critical(f"Unable to update listing table for product id {product_id}")
                    else:
                            error_logger.critical(f"Product with ID {product_id} not in DB. This shouldn't happen")

                db_session.add(obj)
            else:
                error_logger.critical(f"Product with ID {product_id} not in DB. This shouldn't happen")
           

    time.sleep(3)

    return final_results

#TODO: Need to handle following usecases
# 1. Not able to scrape all QandA till a date - retry mechanism or delete all entries in case not successful
# 2. Only one page of QandA

def scrape_qanda(server_url,qanda_url,product_id,threshold_date):
    '''
    Scrapes all Q&A for a product

    Important Parameters:
        qanda_url: URL for QandA of the product
        server_url: The domain e.g. amazon.in
        product_id: ID of the prodct to be scraped
        threshold_date: The date prior to which Q&A won't be scrapped
    '''
    curr =  1
    is_completed = False

    # Sort Q&A by most recent first
    prev_url = qanda_url
    qanda_url = qanda_url+"?sort=SUBMIT_DATE"

    while qanda_url is not None and is_completed == False:
        if my_proxy is None:
            response = session.get(qanda_url, headers={**headers, 'referer':prev_url}, cookies=cookies)
        else:  
            response = my_proxy.get(qanda_url, prev_url)
        
        assert response.status_code == 200

        time.sleep(5) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
        
        html = response.content
        soup = BeautifulSoup(html, 'lxml')
        
        qandas, next_url = parse_data.get_qanda(soup)
        
        # if qanda date for this page is lower than threshold date, mark true
        for qanda in qandas:
            qanda_date = qanda['date']
            if qanda_date is not None:
                # QandA Date must be greater than threshold
                if qanda_date < threshold_date:
                    error_logger.info(f"{product_id} : QandA (Current Page = {curr}) - Date Limit Exceeded.")
                    is_completed = True
                    break
        if use_cache:
            # Store to cache first
            with SqliteDict(cache_file, autocommit=True) as mydict:
                mydict[f"QandA_{product_id}_{curr}"] = qandas
        
        # Insert QandA to dB
        status = db_manager.insert_product_qanda(db_session, qandas, product_id=product_id)
        if not status:
            logger.error(f"Not able to store QandA for {product_id}")     
        
            
        # Go to next page
        if next_url is not None:        
            prev_url = qanda_url
            qanda_url = next_url
            
            # next_url obtained from parse_data will not contain server_url, hence will need to prefix it for 2nd page onwards
            qanda_url = qanda_url if qanda_url.startswith("http") else server_url + qanda_url
            
            curr += 1
            
            # Break if more than 50 QandA pages
            if curr > 50:
                logger.info(f"Breaking at 50 Q&A pages")
                break
            rand = random.randint(4, 17)
            time.sleep(rand) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(3, 8)))
            logger.info(f"QandA: Going to Page {curr}")
        else:
            is_completed = True
            break
    
    return is_completed

#TODO: Need to handle following usecases
# 1. Not able to scrape all reviews till a date - retry mechanism or delete all DB entries in case not successful
# 2. Only one page of review


def scrape_reviews(server_url,reviews_url,product_id,threshold_date):
    '''
    Scrapes all reviews for a product

    Important Parameters:
        review_url: URL for reviews of the product
        server_url: The domain e.g. amazon.in
        product_id: ID of the prodct to be scraped
        threshold_date: The date prior to which reviews won't be scrapped
    '''
    curr =  0
    is_completed = False

    # Sort reviews by most recent first
    prev_url = reviews_url
    reviews_url = reviews_url + f"&sortBy=recent&pageNumber={curr+1}"

    while reviews_url is not None and is_completed == False:
        if my_proxy is None:
            response = session.get(server_url + reviews_url, headers={**headers, 'referer': server_url + prev_url}, cookies=cookies)
        else:  
            response = my_proxy.get(server_url + reviews_url, server_url + prev_url)
        
        assert response.status_code == 200

        time.sleep(5) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
        
        html = response.content
        soup = BeautifulSoup(html, 'lxml')
        
        reviews, next_url = parse_data.get_customer_reviews(soup)
        
        # if review date for this page is lower than threshold date, mark true
        for review in reviews['reviews']:
            review_date = review['review_date']
            if review_date is not None:
                # Review Date must be greater than threshold
                if review_date < threshold_date:
                    error_logger.info(f"{product_id} : Reviews (Current Page = {curr}) - Date Limit Exceeded.")
                    is_completed = True
                    break
        if use_cache:
            # Store to cache first
            with SqliteDict(cache_file, autocommit=True) as mydict:
                mydict[f"REVIEWS_{product_id}_{curr}"] = reviews
        
        # Insert reviews to dB
        status = db_manager.insert_product_reviews(db_session, reviews, product_id=product_id)
        if not status:
            logger.error(f"Not able to store reviews for {product_id}")     
               
        # Go to next page
        if next_url is not None:
            
            prev_url = reviews_url
            reviews_url = next_url
            curr += 1
            
            if curr>100:
                logger.info(f"Breaking at 100 reivew pages")
                break
            rand = random.randint(4, 17)
            time.sleep(rand) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(3, 8)))
            logger.info(f"Reviews: Going to Page {curr+1}")
        else:
            is_completed = True
            break
   
    return is_completed



def assign_template_subcategories(categories=None, pages=None, dump=False, detail=False, threshold_date=None, products=None, review_pages=None, qanda_pages=None, no_listing=False, num_workers=None, worker_pages=None):
    global subcategory_map
    for category in subcategory_map:
        for subcategory in subcategory_map[category]:
            # Go to the URL
            url = subcategory_map[category][subcategory]
            fetch_category(category, url, 10000, change=False, server_url='https://amazon.in', no_listing=False, detail=False, jump_page=0, subcategories=[subcategory], no_refer=True)


def scrape_categories(categories=None, pages=None, dump=False, detail=False, threshold_date=None, products=None, review_pages=None, qanda_pages=None, no_listing=False, num_workers=None, worker_pages=None, detail_override=False, top_n=None, instance_id=None):
    '''
    Fetches product_ids for all given categories and scrapes the details for all those products.
    The product_ids are taken from the ProductListing table, accounting for duplicate_sets

    Important Parameters:
        categories: List of categories
        threshold_date: Threshold date for scraping QandA + Reviews
        review_pages: Max. number of review pages (default is 500 = 5000 reviews)
        qanda_pages: Max. number of qanda pages (default is 50 = 500 QandAs)
        num_workers: Number of worker threads, if using multi-threading
        top_n: Fetch only the top N SKUs (based on total_ratings field in ProductListing)
        instance_id: EC2 instance ID (Optional)
    '''
    
    global my_proxy, session
    global headers, cookies
    global last_product_detail
    global cache
    global use_multithreading
    global USE_DB
    global listing_templates, listing_categories, domain_map

    if pages is None:
        pages = [100000 for _ in listing_templates] # Keeping a big number
    else:
        if isinstance(pages, int):
            if pages <= 0:
                raise ValueError("pages must be a positive integer")
            pages = [100000 for _ in listing_templates]

    server_url = 'https://www.amazon.in'

    credentials = db_manager.get_credentials()
    domain = category_to_domain[categories[0]]
    db_name = domain_to_db[domain]
    _engine, SessionFactory = db_manager.connect_to_db(db_name, credentials)

    if my_proxy is not None:
        try:
            server_url = 'https://www.' + domain
            my_proxy.server_url = server_url
            response = my_proxy.get(server_url)
        except requests.exceptions.ConnectionError:
            logger.warning('No Proxy available via Tor relay. Mode = Normal')
            logger.newline()
            my_proxy = None
            response = session.get(server_url, headers=headers)
    else:
        response = session.get(server_url, headers=headers)
    assert response.status_code == 200
    cookies = dict(response.cookies)
    
    print(cookies)
    if my_proxy is not None:
        logger.info(f"Proxy Cookies = {my_proxy.cookies}")

    if cookies == {}:
        # Change identity and try again
        while True:
            if my_proxy is not None:
                logger.warning(f"Cookies is Empty. Changing identity and trying again...")
                time.sleep(random.randint(4, 16) + random.uniform(0, 2)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(4, 8)))
                my_proxy.change_identity()
                response = my_proxy.get(server_url)
                cookies = response.cookies
                if cookies != {}:
                    break
            else:
                break

    if my_proxy is not None:
        my_proxy.cookies = cookies
    
    time.sleep(10) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))

    final_results = dict()

    change = False

    if products is None:
        products = itertools.repeat(None)
    
    if use_multithreading == False:
        # Don't use multithreading (not recommended)
        for category, category_template, num_pages in zip(listing_categories, listing_templates, pages):
            fetch_category(category, category_template.substitute(PAGE_NUM=1), num_pages, change, server_url=server_url, no_listing=no_listing, detail=detail)
    else:
        # Assign No of threads based on number of categories
        no_sub = False
        if num_workers is None or not isinstance(num_workers, int):
            num_workers = max(1, min(32, len(listing_categories)))

        # Split threads as per pages
        if len(categories) == 1:
            # Only one category. Split it into pages
            logger.info(f"Only one category. Splitting work into {num_workers} threads")
            
            _categories = [categories[0] for _ in range(1, num_workers+1)]
            listing_categories = _categories

            pages = [1 for _ in range(1, num_workers+1)]
            if worker_pages is not None: 
                assert len(worker_pages) == num_workers
                pages = worker_pages
                templates = [listing_templates[0].substitute(PAGE_NUM=page_num) for page_num in pages]
                listing_templates = templates
            else:
                templates = [listing_templates[0].substitute(PAGE_NUM=page_num) for page_num in range(1, num_workers+1)]
                listing_templates = templates


            no_sub = True
        
        # Add threads if Detail is also required to be scraped concurrently
        try:
            if concurrent_jobs == True and detail == True:
                num_workers *= 2
        except:
            pass
        
        logger.info(f"Have {len(listing_categories)} categories. Splitting work into {num_workers} threads")

        if detail == True:
            logger.info(f"Detail: Scraping ONLY category {categories[0]}")
            category = categories[0]
                        
            # We partition the listing ASINs for every thread equally, so that work can be distributed among them, accounting for duplicate sets
            with db_manager.session_scope(SessionFactory) as dbsession:
                queryset = dbsession.query(db_manager.ProductListing).filter(db_manager.ProductListing.category == category).order_by(desc(text('total_ratings'))).order_by(desc(text('detail_completed')))
                duplicate_sets = set()
                total_listing_pids = []
                idx = 0

                # Do it for whole set or only for top_n SKUs
                if top_n is not None and top_n > 0:
                    for instance in queryset:
                        if idx == top_n:
                            break
                        if instance.duplicate_set in duplicate_sets:
                            continue
                        idx += 1
                        duplicate_sets.add(instance.duplicate_set)
                        total_listing_pids.append(instance.product_id)
                else:
                    total_listing_pids = [getattr(instance, 'product_id') for instance in queryset if hasattr(instance, 'product_id')]
            # Partition PIDs/ASINs to various threads
            listing_partition = [total_listing_pids[(i*len(total_listing_pids))//num_workers:((i+1)*len(total_listing_pids)) // num_workers] for i in range(num_workers)]
        else:
            total_listing_pids = []
            listing_partition = [[] for _ in range(num_workers)]
        
        logger.info(f"Scraping Details of {len(total_listing_pids)} products totally")

        # Spawn the threads and call process_product_detail 
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Start the load operations and mark each future with its URL
            if no_sub == False:
                future_to_category = {executor.submit(process_product_detail, _category, category_template.substitute(PAGE_NUM=1), num_pages, change, server_url, no_listing, detail, 0, None, False, threshold_date, listing_pids, qanda_pages, review_pages, detail_override): _category for _category, category_template, num_pages, listing_pids in zip(listing_categories, listing_templates, pages, listing_partition)}
            else:
                future_to_category = {executor.submit(process_product_detail, _category, category_template, num_pages, change, server_url, no_listing, detail, 0, None, False, threshold_date, listing_pids, qanda_pages, review_pages, detail_override): _category for _category, category_template, num_pages, listing_pids in zip(listing_categories, listing_templates, pages, listing_partition)}
            
            # We must wait for all of them to complete
            for future in concurrent.futures.as_completed(future_to_category):
                category = future_to_category[future]
                try:
                    _ = future.result()
                except Exception as exc:
                    logger.critical('%r generated an exception: %s' % (category, exc))
                    exception_logger.critical(f"Thread {category} generated an exception {exc}")
                    exception_logger.critical("".join(traceback.TracebackException.from_exception(exc).format()))
                else:
                    logger.info(f"Category {category} is done!")
    
    if detail == False:
        logger.info(f"Updating duplicate indices...")
        # Update set indexes
        try:
            with db_manager.session_scope(SessionFactory) as dbsession:
                db_manager.update_duplicate_set(dbsession, table='ProductListing', insert=True)
                logger.info("Updated indexes!")
        except Exception as ex:
            logger.critical(f"Error when updating listing indexes: {ex}")
        
        # Update active product IDs
        try:
            logger.info("Updating active PIDS...")
            db_manager.update_active_products(_engine, pids, table='ProductListing', insert=True)
            logger.info("Updated Active PIDS!")
        except Exception as ex:
            logger.critical(f"Error when updating active PIDS: {ex}")
    else:
        try:
            logger.info(f"Updating date_completed for PIDS....")
            db_manager.update_listing_completed(_engine, table='ProductListing')
        except Exception as ex:
            logger.critical(f"Error when updating date_completed for listing: {ex}")
    
    if instance_id is not None:
        cache.set(f"SCRAPING_COMPLETED_{instance_id}", True, timeout=3*60*60)

    return final_results



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--categories', help='List of all categories (comma separated)', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('-f', '--categories_file', help='List of all categories from a file', type=str, default=None)
    parser.add_argument('--listing', help='Scraping the category listing', default=False, action='store_true')
    parser.add_argument('--detail', help='Scraping individual product details', default=False, action='store_true')
    parser.add_argument('-n', '--number', help='Number of Individual Product Details per category to fetch', type=int, default=0)
    parser.add_argument('--pages', help='(Optional) Number of pages to scrape the listing details, provide a single value or comma separated values for each category', type=lambda s: [int(item.strip()) for item in s.split(',')], default=1)
    parser.add_argument('--num_products', help='Number of products per category to scrape the listing details', type=lambda s: [int(item.strip()) for item in s.split(',')], default=None)
    parser.add_argument('--review_pages', help='Number of pages to scrape the reviews per product', type=int, default=5000) # 500 pages Reviews (5000 reviews)
    parser.add_argument('--qanda_pages', help='Number of pages to scrape the qandas per product', type=int, default=50) # 50 pages QandA (500 QandAs)
    parser.add_argument('--dump', help='Flag for dumping the Product Listing Results for each category', default=False, action='store_true')
    parser.add_argument('-i', '--ids', help='List of all product_ids to scrape product details', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('--date', help='Threshold Limit for scraping Product Reviews', type=lambda s: datetime.strptime(s, '%Y-%m-%d'), default=datetime.now() - timedelta(days=90)) # start from 3 months prior as default
    parser.add_argument('--config', help='A config file for the options', type=str)
    parser.add_argument('--tor', help='To use Tor vs Public Proxies', default=False, action='store_true')
    parser.add_argument('--override', help='To scape using existing filters at utils.py', default=False, action='store_true')
    parser.add_argument('--no_listing', help='To specify if listing is needed while scraping details', default=False, action='store_true')
    parser.add_argument('--concurrent_jobs', help='To specify if listing + details need to be done', default=False, action='store_true')
    parser.add_argument('--num_workers', help='To specify number of worker threads', type=int, default=0)
    parser.add_argument('--worker_pages', help='Page per worker thread for product detail', type=lambda s: [int(item.strip()) for item in s.split(',')], default=None)
    parser.add_argument('--detail_override', help='Override date threshold and scrape details from scratch', default=False, action='store_true')
    parser.add_argument('--top_n', help='Fetch Top N product details', type=int, default=None)
    parser.add_argument('--jump_page', help='Jump page', type=int, default=0)
    parser.add_argument('--assign_subcategories', help='Assign Subcategories', default=False, action='store_true')
    parser.add_argument('--instance_id', help='Instance ID', default=None, type=int)
    parser.add_argument('--reviews_url', help='Scrape reviews for product with given review url', default=None, type=int)

    args = parser.parse_args()

    categories = args.categories
    categories_file = args.categories_file
    listing = args.listing
    detail = args.detail
    num_items = args.number
    pages = args.pages
    review_pages = args.review_pages
    qanda_pages = args.qanda_pages
    dump = args.dump
    product_ids = args.ids
    config = args.config
    threshold_date = args.date
    use_tor = args.tor
    num_products = args.num_products
    override = args.override
    detail_override = args.detail_override
    no_listing = args.no_listing
    concurrent_jobs = args.concurrent_jobs
    num_workers = args.num_workers
    worker_pages = args.worker_pages
    top_n = args.top_n
    jump_page = args.jump_page
    assign_subcategories = args.assign_subcategories
    instance_id = args.instance_id
    reviews_url = args.reviews_url

    if num_workers <= 0:
        num_workers = None
    
    no_scrape = False

    # store the original SIGINT handler
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)

    # If no category specified in command line, open category file
    if categories in [None, []]:
        if categories_file is not None:
            categories = []
            with open(categories_file, 'r') as f:
                for line in f:
                    categories.append(line.strip())

    try:
        # Take details from config or .env file, should be None if running from command line
        if config is not None: 
            # Iterate through command line args
            for arg in vars(args):
                if arg == 'categories_file' and getattr(args, arg) == None:
                    continue
                if arg == 'pages' and getattr(args, arg) == 1:
                    continue
                if arg == 'num_products' and getattr(args, arg) == None:
                    continue
                if args == 'tor':
                    continue
                if args == 'override':
                    continue
                if args == 'no_listing':
                    continue
                if args == 'concurrent_jobs':
                    continue
                if args == 'assign_subcategories':
                    continue
                if arg not in ('config', 'number',) and getattr(args, arg) not in (None, False):
                    raise ValueError("--config file is already specified")
            
            option = None
            categories = []
            product_ids = []
            pages = []
            no_scrape = False # For scraping Listing

            options = ["Listing", "Details"]
            with open(f"{config}", "r") as f: #open .env file
                for line in f:
                    line = line.strip()
                    if line == 'USE_TOR':
                        use_tor = True
                    if len(line) >= 2:
                        if line[0] == '#':
                            # Comment
                            text = line.split()[1]
                            if text in options:
                                option = text
                    if option == 'Listing':
                        # Product Listing
                        if len(line) > 0 and line[0] != '#':
                            if line == 'NO_SCRAPE':
                                listing = False
                                no_scrape = True
                            else:
                                listing = True
                                categories.append(' '.join(line.split()[:-1]))
                                pages.append(int(line.split()[-1]))
                    elif option == 'Details':
                        # Product Details
                        if len(line) > 0 and line[0] != '#':
                            detail = True
                            content = line.split()
                            if content[0] == 'all':
                                # Get all details of the categories
                                product_ids = db_manager.fetch_product_ids(db_session, 'ProductListing', categories)
                                if len(content) == 2:
                                    # Threshold date is the second option
                                    threshold_date = datetime.strptime(content[1], '%Y-%m-%d')
                                break
                            pid, qanda, review = content[0], int(content[1]), int(content[2])
                            product_ids.append(line)
        
        # Pages argument needs to be list of #pages to be scraped for each category, default of 1 if not provided. Will be changed to higher default later
        if isinstance(pages, int):
            if categories is None:
                pass
            else:
                pages = [pages for _ in categories]
        elif len(pages) == 1:
            if categories is None:
                raise ValueError("--categories cannot be None if --pages is provided")
            pages = [pages[0] for _ in categories]
        else:
            if categories is not None and pages is not None:
                if override == False:
                    assert len(pages) == len(categories)
                else:
                    if isinstance(pages, list):
                        assert len(pages) == len(listing_templates)

        #if categories is not None and product_ids is not None:
        #    raise ValueError("Both --categories and --ids cannot be provided")

        if no_scrape == True:
            categories = None
            pages = None

        # Set the attribute for my_proxy
        if my_proxy is not None:
            my_proxy.proxy_list = my_proxy.get_proxy_list()
            my_proxy.switch_proxy()
            logger.info(f"Current proxy is {my_proxy.get_ip()}")
        
        if reviews_url:
            scrape_reviews("https://www.amazon.in",reviews_url,'B07CTLZSQ3',datetime.datetime.now()-datetime.timedelta(days=30))
            
        
        logger.info(f"no_listing is {no_listing}")
        
        if assign_subcategories == True:
            print("Assigning Subcategories")
            assign_template_subcategories(categories=None, pages=None, dump=False, detail=False, threshold_date=None, products=None, review_pages=None, qanda_pages=None, no_listing=False, num_workers=None, worker_pages=None)
            db_session.close()
            logger.info("Closed DB connections!")
            exit(0)

        if categories is not None:
            # Scrape listing
            if listing == True:
                if num_products is not None and isinstance(num_products, list):
                    assert len(num_products) == len(categories)
                
                if override == False:
                    results = scrape_categories(categories, pages=pages, dump=dump, detail=detail, threshold_date=threshold_date, products=num_products, review_pages=review_pages, qanda_pages=qanda_pages, no_listing=no_listing)
                else:
                    # Override
                    if isinstance(pages, list):
                        results = scrape_categories(categories=categories, pages=pages, dump=dump, detail=detail, threshold_date=threshold_date, products=num_products, review_pages=review_pages, qanda_pages=qanda_pages, no_listing=no_listing, num_workers=num_workers, worker_pages=worker_pages, detail_override=detail_override, top_n=top_n, instance_id=instance_id)
                    else:
                        results = scrape_categories(categories=categories, pages=None, dump=dump, detail=detail, threshold_date=threshold_date, products=num_products, review_pages=review_pages, qanda_pages=qanda_pages, no_listing=no_listing, num_workers=num_workers, worker_pages=worker_pages, detail_override=detail_override, top_n=top_n, instance_id=instance_id)
            # Directly scrape details
            else:
                for category in categories:
                    if product_ids is None:
                        # Get product urls for categories with detail_completed = None or done a week prior
                        product_urls = db_manager.fetch_product_urls_unscrapped_details(db_session,category,'ProductListing')
                        for product_url in product_urls:
                            pd_id = product_url.split('/')[3]
                            logger.info(f"Scraping product details for product_id {pd_id}")
                            try:
                                product_detail_results = scrape_product_detail(product_url,category, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
                            except Exception as ex:
                                logger.critical(f"{ex}")
                                logger.warning(f"Could not scrape details of Product - URL = {product_url}")
                                my_proxy.switch_proxy()
                                continue
                    else:
                        # Product ids are also there
                        jobs = []
                        ids = []
                        for product_id in product_ids:
                            obj = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': product_id}))
                            if obj is None:
                                logger.warning(f"Product ID {product_id} not found in the Database")
                                logger.newline()
                                continue

                            assert obj.product_id == product_id

                            if obj.product_url is None or obj.category is None:
                                if obj.product_url is None:
                                    logger.warning(f"Product ID {product_id} has a NULL product_url")
                                else:
                                    logger.warning(f"Product ID {product_id} has a NULL category")
                                logger.newline()
                                continue

                            # Scrape the product
                            product_url = obj.product_url
                            category = obj.category

                            jobs.append([category, product_url, review_pages, qanda_pages, threshold_date, None, None, True, jump_page])
                            ids.append(product_id)
                            
                            if use_multithreading == False:
                                try: 
                                    product_detail_results = scrape_product_detail(product_url,category, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
                                except Exception as ex:
                                    logger.critical(f"{ex}")
                                    logger.warning(f"Could not scrape details of Product ID {product_id} - URL = {product_url}")
                                    logger.newline()
                        
                            if use_multithreading == True:
                                num_jobs = len(jobs)
                                for idx, _job in enumerate(jobs[::num_workers]):
                                    if terminate == True:
                                        logger.info("Terminating....")
                                    batch_size = min(num_jobs - num_workers*idx, num_workers)
                                    logger.info(f"Now going for batch from idx {idx} with batch size {batch_size}...")
                                    time.sleep(5)
                                    with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
                                        future_to_category = dict()
                                        for i, job in enumerate(jobs[num_workers*idx: num_workers*idx + batch_size]):
                                            product_id = ids[num_workers*idx + i]
                                            future_to_category[executor.submit(scrape_product_detail, *job)] = f"{product_id}_detail"   
                                        
                                        for future in concurrent.futures.as_completed(future_to_category):
                                            product_id = future_to_category[future]
                                            try:
                                                _ = future.result()
                                            except Exception as exc:
                                                logger.critical('%r generated an exception: %s' % (category, exc))
                                                exception_logger.critical(f"Thread {product_id} generated an exception {exc}")
                                                exception_logger.critical("".join(traceback.TracebackException.from_exception(exc).format()))
                                            else:
                                                logger.info(f"Product ID {product_id} is done!")

        else:
            # Categories is None
            # See if the ids are there
            for product_id in product_ids:
                obj = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': product_id}))
                if obj is None:
                    logger.warning(f"Product ID {product_id} not found in the Database")
                    logger.newline()
                    continue

                assert obj.product_id == product_id

                if obj.product_url is None or obj.category is None:
                    if obj.product_url is None:
                        logger.warning(f"Product ID {product_id} has a NULL product_url")
                    else:
                        logger.warning(f"Product ID {product_id} has a NULL category")
                    logger.newline()
                    continue

                # Scrape the product
                product_url = obj.product_url
                category = obj.category
                try:
                    product_detail_results = scrape_product_detail(product_url,category, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
                except Exception as ex:
                    logger.critical(f"{ex}")
                    logger.warning(f"Could not scrape details of Product ID {product_id} - URL = {product_url}")
                    logger.newline()
    finally:
        db_session.close()
        #Session.remove()
        logger.info("Closed DB connections!")
