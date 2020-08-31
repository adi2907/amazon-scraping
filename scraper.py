import argparse
import concurrent.futures
import itertools
import json
import os
import pickle
import random
import signal
import sqlite3
import sys
import time
import traceback
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
from string import Template

import requests
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlitedict import SqliteDict

import cache
import db_manager
import parse_data
import proxy
from utils import (create_logger, customer_reviews_template,
                   listing_categories, listing_templates, qanda_template,
                   url_template)

logger = create_logger('scraper')

error_logger = create_logger('errors')

exception_logger = create_logger('threads')

headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

cookies = dict()

cache = cache.Cache()

try:
    USE_REDIS = config('USE_REDIS')
    if USE_REDIS == 'True':
        USE_REDIS = True
    else:
        USE_REDIS = False
except UndefinedValueError:
    USE_REDIS = False

logger.info(f"USE_REDIS = {USE_REDIS}")

cache.connect('master', use_redis=USE_REDIS)

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

logger.info(f"USE_DB is - {USE_DB}")

# Start the session
session = requests.Session()

# Use a proxy if possible
my_proxy = proxy.Proxy(OS=OS)

try:
    my_proxy.change_identity()
except:
    logger.warning('No Proxy available via Tor relay. Mode = Normal')
    logger.newline()
    my_proxy = None

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


def store_to_cache(key, value):
    global cache_file, use_cache
    global cache
    try:
        with SqliteDict(cache_file, autocommit=True) as mydict:
            mydict[key] = value
    except RecursionError:
        error_logger.critical(f"Recursion Depth exceeded when trying to store key -> {key}")
        logger.info("Trying to convert to string and store...")
        try:
            with SqliteDict(cache_file, autocommit=True) as mydict:
                mydict[key] = str(value)
        except RecursionError:
            logger.info(f"Trying to store to Redis Cache")
            try:
                cache.set(key, value, timeout=None)
            except Exception as ex:
                try:
                    logger.info("Trying to convert to string and store...")
                    cache.set(key, str(value), timeout=None)
                except Exception as exc:
                    error_logger.info(f"Redis Cache -> Generated Exception: {exc}")
 

def fetch_category(category, base_url, num_pages, change=False, server_url='https://amazon.in', no_listing=False, detail=False):
    # global my_proxy, session
    global headers, cookies
    global last_product_detail
    global cache
    global speedup
    global use_multithreading
    global cache_file, use_cache
    global USE_DB

    if use_multithreading == True:
        my_proxy = proxy.Proxy(OS=OS, stream_isolation=True) # Separate Proxy per thread
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
        pass
    
    try:
        logger.info(f"Now at category {category}, with num_pages {num_pages}")

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
            response = my_proxy.get(base_url, referer=server_url)
            setattr(my_proxy, 'category', category)
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
            html = response.content
            soup = BeautifulSoup(html, 'lxml')

            product_info, curr_serial_no = parse_data.get_product_info(soup, curr_serial_no=curr_serial_no)

            final_results[category][curr_page] = product_info

            page_element = soup.find("ul", class_="a-pagination")
            
            if page_element is None:
                if my_proxy is None:
                    response = session.get(base_url, headers=headers, cookies=cookies)
                else:
                    response = my_proxy.get(base_url, referer=curr_url)
                
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
            
            next_page = page_element.find("li", class_="a-last")
            if next_page is None:
                if my_proxy is None:
                    response = session.get(base_url, headers=headers, cookies=cookies)
                else:
                    response = my_proxy.get(base_url)
                
                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}
                
                logger.warning(f"Curr Page = {curr_page}. Next Page Element is None")

                time.sleep(3)
            
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
                    response = my_proxy.get(server_url + page_url, referer=curr_url)
                
                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}
                next_url = server_url + page_url

                time.sleep(5) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))

            listing = []

            page_results = dict()
            page_results[category] = final_results[category]

            temp = deepcopy(page_results)

            for title in temp[category][curr_page]:
                value = temp[category][curr_page][title]
                
                if 'total_ratings' not in value or 'price' not in value or value['total_ratings'] is None or value['price'] is None:
                    continue
                
                total_ratings = int(value['total_ratings'].replace(',', '').replace('.', ''))
                price = int(value['price'][1:].replace(',', '').replace('.', ''))

                small_title = title.split()[0].strip()

                duplicate = False

                for item in listing:
                    if item['small_title'] == small_title and item['total_ratings'] == total_ratings and item['price'] == price:
                        logger.info(f"Found duplicate match! For title - {small_title}")
                        logger.info(f"Existing product is {title}, but old one is {item['title']}")
                        duplicate = True
                        break
                
                if duplicate == True:
                    del final_results[category][curr_page][title]
                else:
                    listing.append({'title': title, 'small_title': small_title, 'total_ratings': total_ratings, 'price': price})
            
            # Reset it
            listing = []
            del temp

            if use_cache:
                # Store to cache first
                with SqliteDict(cache_file, autocommit=True) as mydict:
                    mydict[f"LISTING_{category}_PAGE_{curr_page}"] = page_results
            
            if no_listing == False:
                # Dump the results of this page to the DB
                if USE_DB == True:
                    try:
                        status = db_manager.insert_product_listing(db_session, page_results)
                        if not status:
                            raise ValueError("Yikes. Status is False")
                    except:
                        store_to_cache(f"LISTING_{category}_PAGE_{curr_page}", page_results)
                    try:
                        status = db_manager.insert_daily_product_listing(db_session, page_results)
                        if not status:
                            raise ValueError("Yikes. Status is False")
                    except:
                        store_to_cache(f"LISTING_{category}_PAGE_{curr_page}", page_results)

            if detail == True:
                for title in final_results[category][curr_page]:
                    product_url = final_results[category][curr_page][title]['product_url']
                    if product_url is not None:
                        if product_url.startswith(f"/s?k="):
                            # Probably the heading. SKip this
                            logger.info(f"Encountered the heading -> Title = {title}")
                            error_logger.info(f"Encountered the heading -> Title = {title}")
                            logger.newline()
                            continue

                        product_id = None
                        rescrape = 0

                        try:
                            product_id = parse_data.get_product_id(product_url)
                            if product_id is not None:
                                obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
                                if obj is not None:
                                    if hasattr(obj, 'product_details') and obj.product_details in (None, {}, '{}'):
                                        rescrape = 1
                                        error_logger.info(f"Product ID {product_id} has NULL product_details. Scraping it again...")
                                        if hasattr(obj, 'completed') and obj.completed is None:
                                            rescrape = 2
                                    else:
                                        logger.info(f"Product with ID {product_id} already in ProductDetails. Skipping this product")
                                        error_logger.info(f"Product with ID {product_id} already in ProductDetails. Skipping this product")
                                        continue
                                else:
                                    logger.info(f"{idx}: Product with ID {product_id} not in DB. Scraping Details...")
                                    error_logger.info(f"{idx}: Product with ID {product_id} not in DB. Scraping Details...")

                            # Let's try to approximate the minimum reviews we need
                            value = final_results[category][curr_page][title]
                            if 'total_ratings' not in value or value['total_ratings'] is None:
                                total_ratings = None
                            else:
                                total_ratings = int(value['total_ratings'].replace(',', '').replace('.', ''))
                            
                            if rescrape == 0:
                                _ = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date, listing_url=curr_url, total_ratings=total_ratings)
                            elif rescrape == 1:
                                # Only details
                                _ = scrape_product_detail(category, product_url, review_pages=0, qanda_pages=0, threshold_date=threshold_date, listing_url=curr_url, total_ratings=total_ratings)
                            elif rescrape == 2:
                                # Whole thing again
                                _ = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date, listing_url=curr_url, total_ratings=total_ratings)

                            idx += 1
                        except Exception as ex:
                            if product_id is not None:
                                logger.critical(f"During scraping product details for ID {product_id}, got exception: {ex}")
                            else:
                                logger.critical(f"Product ID is None, got exception: {ex}")

                        if last_product_detail == True:
                            logger.info("Completed pending products. Exiting...")
                            return final_results

                        if my_proxy is not None:
                            if num_products is None or idx <= num_products:
                                response = my_proxy.get(curr_url, referer=server_url + product_url)
                                time.sleep(random.randint(3, 5)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
                            elif num_products is not None and idx > num_products:
                                # We're done for this product
                                logger.info(f"Scraped {num_products} for category {category}. Moving to the next one")
                                error_logger.info(f"Scraped {num_products} for category {category}. Moving to the next one")
                                overflow = True
                                break

            if next_page is None:
                logger.info("Next Page is None. Exiting catgory...")
                error_logger.info("Next Page is None. Exiting catgory...")
                break


            # Delete the previous page results
            if category in final_results and curr_page in final_results[category]:
                del final_results[category][curr_page]
            
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
        
        if use_cache:
            # Store to cache first
            with SqliteDict(cache_file, autocommit=True) as mydict:
                mydict[f"LISTING_{category}_PAGE_{curr_page}"] = results

        if USE_DB == True:
            # Insert to the DB
            try:
                status = db_manager.insert_product_listing(db_session, results)
                if status == False:
                    # Store to Cache
                    raise ValueError("Status is False")
            except:
                store_to_cache(f"LISTING_{category}_PAGE_{curr_page}", results)

        logger.info(f"Finished Scraping the LAST page {curr_page} of {category}")

        time.sleep(4)

        change = True
    finally:
        if use_multithreading == True:
            db_session.close()
            # Session.remove()


def scrape_category_listing(categories, pages=None, dump=False, detail=False, threshold_date=None, products=None, review_pages=None, qanda_pages=None, no_listing=False):
    global my_proxy, session
    global headers, cookies
    global last_product_detail
    global cache
    global use_multithreading
    global cache_file, use_cache
    global USE_DB
    # session = requests.Session()

    if pages is None:
        pages = [10000 for _ in categories] # Keeping a big number
    else:
        if isinstance(pages, int):
            if pages <= 0:
                raise ValueError("pages must be a positive integer")
            pages = [pages for _ in categories]

    server_url = 'https://www.amazon.in'
    
    if my_proxy is not None:
        try:
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
                time.sleep(random.randint(4, 16) + random.uniform(0, 2))
                my_proxy.change_identity()
                response = my_proxy.get(server_url)
                cookies = response.cookies
                if cookies != {}:
                    break
            else:
                break

    if my_proxy is not None:
        my_proxy.cookies = cookies
    
    time.sleep(10) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(4, 7)))

    final_results = dict()

    change = False

    if products is None:
        products = itertools.repeat(None)

    for category, num_pages, num_products in zip(categories, pages, products):
        logger.info(f"Now at category {category}, with num_pages {num_pages}")
        
        idx = 1 # Total number of scraped product details
        curr_serial_no = 1 # Serial Number from the top
        overflow = False
        if num_products is not None and idx > num_products:
            continue

        final_results[category] = dict()
        base_url = url_template.substitute(category=category.replace(' ', '+'))
        
        if my_proxy is not None:
            if change == True:
                change = False
                my_proxy.change_identity()
                time.sleep(random.randint(2, 5)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
            logger.info(f"Proxy Cookies = {my_proxy.cookies}")
            response = my_proxy.get(base_url)
            setattr(my_proxy, 'category', category)
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
            html = response.content
            soup = BeautifulSoup(html, 'lxml')
                        
            product_info, curr_serial_no = parse_data.get_product_info(soup, curr_serial_no=curr_serial_no)

            final_results[category][curr_page] = product_info
            
            page_element = soup.find("ul", class_="a-pagination")
            
            if page_element is None:
                if my_proxy is None:
                    response = session.get(base_url, headers=headers, cookies=cookies)
                else:
                    response = my_proxy.get(base_url, referer=curr_url)
                
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
            
            next_page = page_element.find("li", class_="a-last")
            if next_page is None:
                if my_proxy is None:
                    response = session.get(base_url, headers=headers, cookies=cookies)
                else:
                    response = my_proxy.get(base_url)
                
                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}
                
                logger.warning(f"Curr Page = {curr_page}. Next Page Element is None")

                time.sleep(3)
                break
            
            page_url = next_page.find("a")
            if page_url is None:
                logger.warning(f"Curr Page = {curr_page}. Next Page Element is not None, but URL is None")
                time.sleep(3)
                break
            
            page_url = page_url.attrs['href']

            if my_proxy is None:       
                response = session.get(server_url + page_url, headers={**headers, 'referer': curr_url}, cookies=cookies)
            else:
                response = my_proxy.get(server_url + page_url, referer=curr_url)
            
            if hasattr(response, 'cookies'):
                cookies = {**cookies, **dict(response.cookies)}
            next_url = server_url + page_url

            time.sleep(5) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))

            page_results = dict()
            page_results[category] = final_results[category]

            try:
                listing = []

                temp = deepcopy(page_results)

                # Identify Duplicates
                for title in temp[category][curr_page]:
                    value = temp[category][curr_page][title]
                    
                    if 'total_ratings' not in value or 'price' not in value or value['total_ratings'] is None or value['price'] is None:
                        continue
                    
                    total_ratings = int(value['total_ratings'].replace(',', '').replace('.', ''))
                    price = int(value['price'][1:].replace(',', '').replace('.', ''))

                    small_title = title.split()[0].strip()

                    duplicate = False

                    for item in listing:
                        if item['small_title'] == small_title and item['total_ratings'] == total_ratings and item['price'] == price:
                            logger.info(f"Found duplicate match! For title - {small_title}")
                            logger.info(f"Existing product is {title}, but old one is {item['title']}")
                            duplicate = True
                            break
                    
                    if duplicate == True:
                        del final_results[category][curr_page][title]
                    else:
                        listing.append({'title': title, 'small_title': small_title, 'total_ratings': total_ratings, 'price': price})
                
                # Reset it
                listing = []
                del temp
            except Exception as ex:
                logger.warning(f"Exception occured during comparison - Category {category}")
                logger.info(ex)
                logger.newline()
            
            if no_listing == False:
                if USE_DB == True:
                    # Dump the results of this page to the DB
                    try:
                        status = db_manager.insert_product_listing(db_session, page_results)
                        if status == False:
                            # Insert to cache
                            raise ValueError
                    except:
                        store_to_cache(f"LISTING_{category}_PAGE_{curr_page}", page_results)
                    try:
                        status = db_manager.insert_daily_product_listing(db_session, page_results)
                        if status == False:
                            raise ValueError
                    except:
                        store_to_cache(f"LISTING_{category}_PAGE_{curr_page}", page_results)

            if detail == True:
                for title in final_results[category][curr_page]:
                    product_url = final_results[category][curr_page][title]['product_url']
                    if product_url is not None:
                        product_id = parse_data.get_product_id(product_url)
                        only_detail = False
                        incomplete = False
                        if product_id is not None:
                            obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
                            if obj is not None:
                                if hasattr(obj, 'product_details') and obj.product_details not in ({}, [], '', '{}', '[]', None):
                                    if obj.completed == True:
                                        logger.info(f"Product with ID {product_id} already in ProductDetails. Skipping this product")
                                        continue
                                    else:
                                        logger.info(f"Product with ID {product_id} not yet completed. Scraping only details page")
                                        incomplete = True
                                else:
                                    # We only need to scrape product detail page
                                    logger.info(f"Product with ID {product_id} has NULL product_details. Scraping only details page")
                                    only_detail = True
                            else:
                                logger.info(f"{idx}: Product with ID {product_id} not in DB. Scraping Details...")
                        
                        if only_detail == True:
                            _ = scrape_product_detail(category, product_url, review_pages=0, qanda_pages=0, threshold_date=threshold_date, listing_url=curr_url)
                        else:
                            if incomplete == True:
                                _ = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date, listing_url=curr_url, incomplete=True)
                            else:
                                _ = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date, listing_url=curr_url)
                        idx += 1

                        if last_product_detail == True:
                            logger.info("Completed pending products. Exiting...")
                            return final_results

                        if my_proxy is not None:
                            if num_products is None or idx <= num_products:
                                response = my_proxy.get(curr_url, referer=server_url + product_url)
                                time.sleep(random.randint(3, 5)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
                            elif num_products is not None and idx > num_products:
                                # We're done for this product
                                logger.info(f"Scraped {num_products} for category {category}. Moving to the next one")
                                overflow = True
                                break

            # Delete the previous page results
            if category in final_results and curr_page in final_results[category]:
                del final_results[category][curr_page]
            
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
        
        if USE_DB == True:
            # Insert to the DB
            try:
                status = db_manager.insert_product_listing(db_session, results)
                if status == False:
                    raise ValueError
            except:
                store_to_cache(f"LISTING_{category}_PAGE_{curr_page}", results)

        logger.info(f"Finished Scraping the LAST page {curr_page} of {category}")

        time.sleep(4) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))

        change = True
    return final_results


def scrape_product_detail(category, product_url, review_pages=None, qanda_pages=None, threshold_date=None, listing_url=None, total_ratings=None, incomplete=False):
    global my_proxy, session
    global headers, cookies
    global cache
    global use_multithreading
    global cache_file, use_cache
    global USE_DB
    # session = requests.Session()
    server_url = 'https://www.amazon.in'

    product_id = parse_data.get_product_id(product_url)

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
        review_pages = 1000
    
    if qanda_pages is None:
        qanda_pages = 1000
    
    if my_proxy is None:
        response = session.get(server_url, headers=headers)
    else:
        response = my_proxy.get(server_url)
        setattr(my_proxy, 'category', category)
    
    assert response.status_code == 200
    cookies = dict(response.cookies)
    time.sleep(3)

    is_completed = False

    REVIEWS_PER_PAGE = 10

    while True:
        if my_proxy is None:
            response = session.get(server_url + product_url, headers=headers, cookies=cookies)
        else:
            if listing_url is not None:
                response = my_proxy.get(server_url + product_url, referer=listing_url, product_url=product_url)
            else:
                response = my_proxy.get(server_url + product_url, product_url=product_url)
        
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
            # Now check model
            '''
            if 'model' in details:
                model = details['model']
                if model is not None:
                    obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'model': f'{product_id}'}))
                    if obj is not None:
                        logger.info(f"For product {product_id}, encountered duplicate Model. Exiting...")
                        return None
            '''
            break
        except ValueError:
            #DUMP_DIR = os.path.join(os.getcwd(), 'dumps')
            #if not os.path.exists(DUMP_DIR):
            #    os.mkdir(DUMP_DIR)
            #if category is None or product_url is None:
            #    filename = 'none'
            #else:
            #    filename = category.replace('/', '') + product_url.replace('/', '') + '.html'
            #with open(os.path.join(DUMP_DIR, filename), 'wb') as f:
            #    f.write(html)
            logger.warning(f"Written html to {category}_{product_url}.html")
            logger.warning(f"Couldn't parse product Details for {product_id}. Possibly blocked")
            logger.warning("Trying again...")
            time.sleep(random.randint(3, 10) + random.uniform(0, 4)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
            if my_proxy is not None:
                my_proxy.goto_product_listing(category)

    details['product_id'] = product_id # Add the product ID
    
    # Check if the product is sponsored
    sponsored = parse_data.is_sponsored(product_url)

    if use_cache:
        # Store to cache first
        with SqliteDict(cache_file, autocommit=True) as mydict:
            mydict[f"DETAILS_{product_id}"] = details
            mydict[f"IS_SPONSORED_{product_id}"] = sponsored

    if USE_DB == True:
        # Insert to the DB
        try:
            status = db_manager.insert_product_details(db_session, details, is_sponsored=sponsored)
            if status == False:
                store_to_cache(f"DETAILS_{product_id}", details)
                store_to_cache(f"IS_SPONSORED_{product_id}", sponsored)
        except:
            store_to_cache(f"DETAILS_{product_id}", details)
            store_to_cache(f"IS_SPONSORED_{product_id}", sponsored)
    
    time.sleep(4)

    curr_reviews = -1

    if incomplete == True:
        # Don't scrape QandA
        qanda_pages = 0

        num_reviews_none = 0
        num_reviews_not_none = db_session.query(db_manager.Reviews).filter(db_manager.Reviews.product_id == product_id, db_manager.Reviews.page_num != None).count()
        
        if num_reviews_not_none == 0:
            num_reviews_none = db_session.query(db_manager.Reviews).filter(db_manager.Reviews.product_id == product_id, db_manager.Reviews.page_num == None).count()
        
        curr_reviews = max(num_reviews_not_none, num_reviews_none)
    
    # Get the qanda for this product
    if 'customer_lazy' in details and details['customer_lazy'] == True:
        qanda_url = details['customer_qa']
        curr = 0
        factor = 0
        first_request = True
        cooldown = False
        prev_url = product_url
        
        qanda_url = qanda_template.substitute(PID=product_id, PAGE=curr+1) + '?isAnswered=true'
        
        while qanda_url is not None:
            if qanda_pages <= 0:
                break
            if my_proxy is None:
                response = session.get(qanda_url, headers={**headers, 'referer': server_url + product_url}, cookies=cookies)
            else:
                if curr == 0:
                    if first_request == True:
                        response = my_proxy.get(qanda_url, referer=server_url + prev_url, product_url=product_url, ref_count='constant')
                    else:
                        pass
                else:
                    # prev_url has the full path
                    response = my_proxy.get(qanda_url, referer=prev_url, product_url=product_url, ref_count='constant')
            
            if hasattr(response, 'cookies'):
                cookies = {**cookies, **dict(response.cookies)}
            assert response.status_code == 200
            
            time.sleep(5) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
            html = response.content
            soup = BeautifulSoup(html, 'lxml')

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
                        logger.critical("Trying again...")
                        time.sleep(random.randint(7, 15))
                        factor = 0
                        continue

                logger.warning(f"Encountered a CAPTCHA page. Using exponential backoff. Current Delay = {my_proxy.delay}")
                factor += 1
                my_proxy.delay *= 2
                continue

            try:
                if first_request == True:
                    page_num = 0
                else:
                    page_num = curr + 1
            except Exception as e:
                page_num = None
                print(e)
                pass

            qanda, next_url = parse_data.get_qanda(soup, page_num=page_num)

            if use_cache:
                # Store to cache first
                with SqliteDict(cache_file, autocommit=True) as mydict:
                    mydict[f"QANDA_{product_id}_{curr}"] = qanda
            
            if USE_DB == True:
                # Insert to the DB
                try:
                    status = db_manager.insert_product_qanda(db_session, qanda, product_id=product_id)
                    if status == False:
                        store_to_cache(f"QANDA_{product_id}_{curr}", qanda)
                except:
                    store_to_cache(f"QANDA_{product_id}_{curr}", qanda)
            
            if next_url is not None:
                logger.info(f"QandA: Going to Page {curr}")
                t_curr = qanda_url
                t_prev = prev_url
                prev_url = qanda_url
                qanda_url = server_url + next_url
                curr += 1
                rand = random.randint(4, 17)
                time.sleep(rand) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(3, 8)))
                rand = random.randint(0, 100)
                
                if rand <= 15:
                    logger.info("Going back randomly")
                    if curr == 1:
                        # Prev URL doesnt have full path
                        t_prev = server_url + t_prev
                    response = my_proxy.get(t_prev, referer=t_curr, product_url=product_url, ref_count='constant')
                    time.sleep(random.randint(6, 12)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
                    response = my_proxy.get(t_curr, referer=t_prev, product_url=product_url, ref_count='constant')
                    time.sleep(random.randint(6, 12)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))

                if qanda_pages is not None and curr == qanda_pages:
                    error_logger.info(f"QandA (Current Page = {curr}) - Finished last page. Going to Reviews now...")
                    error_logger.newline()
                    break
                
                if first_request == True:
                    # First Request
                    first_request = False
                    qanda_url = qanda_template.substitute(PID=product_id, PAGE=curr+1) + f"?sort=SUBMIT_DATE&isAnswered=true"
                    response = my_proxy.get(qanda_url, referer=prev_url, product_url=product_url, ref_count='constant')
                    assert response.status_code == 200

                    time.sleep(random.randint(4, 5) + random.uniform(0, 1)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5) + random.uniform(0, 1)))
                    
                    # Now sort by date
                    logger.info("Now moving into sorting by most recent.")
                    continue
                else:
                    # We're sorting by most recent
                    qanda_url = qanda_template.substitute(PID=product_id, PAGE=curr+1) + f"?sort=SUBMIT_DATE&isAnswered=true"
                    if threshold_date is None:
                        pass
                    else:
                        limit = False
                        for pair in qanda:
                            qanda_date = pair['date']
                            if qanda_date is not None:
                                # Review Date must be greater than threshold
                                if qanda_date < threshold_date:
                                    error_logger.info(f"{product_id} : QandA (Current Page = {curr}) - Date Limit Exceeded.")
                                    error_logger.newline()
                                    limit = True
                                    break
                        if limit == True:
                            break
            else:
                error_logger.info(f"{product_id} : QandA (Current Page = {curr}) - Next Page is None. Going to Reviews now...")
                break
    
    # Get the customer reviews
    if details is not None and 'reviews_url' in details:
        reviews_url = details['reviews_url']
        prev_url = product_url
        curr = 0
        factor = 0
        first_request = True
        cooldown = False
        
        retry = 0
        MAX_RETRIES = 3

        while reviews_url is not None:
            if review_pages <= 0:
                dont_update = True
                break
            if reviews_url is not None and product_url is not None:
                if my_proxy is None:
                    response = session.get(server_url + reviews_url, headers={**headers, 'referer': server_url + prev_url}, cookies=cookies)
                else:
                    if curr == 0 and first_request == False:
                        response = my_proxy.get(server_url + reviews_url, referer=server_url + prev_url, product_url=product_url, ref_count='constant')
                    else:
                        response = my_proxy.get(server_url + reviews_url, referer=server_url + prev_url, product_url=product_url, ref_count='constant')
               
                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}
                
                if response.status_code != 200:
                    logger.error(f"{product_id} : Review Page - Got code {response.status_code}")
                    error_logger.error(f"{product_id} : Review Page - Got code {response.status_code}")
                    logger.error(f"Content = {response.content}")

                assert response.status_code == 200
                time.sleep(5) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5)))
                
                html = response.content
                soup = BeautifulSoup(html, 'lxml')

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
                            logger.critical("Trying again...")
                            time.sleep(random.randint(7, 15))
                            factor = 0
                            continue

                    logger.warning(f"Encountered a CAPTCHA page. Using exponential backoff. Current Delay = {my_proxy.delay}")
                    factor += 1
                    my_proxy.delay *= 2
                    continue

                try:
                    if first_request == True:
                        page_num = 0
                    else:
                        page_num = curr + 1
                except Exception as e:
                    page_num = None
                    print(e)
                    pass
                
                reviews, next_url, num_reviews = parse_data.get_customer_reviews(soup, page_num=page_num, first_request=first_request)

                if first_request == True:
                    if num_reviews is None:
                        logger.warning(f"For {product_id}, num_reviews is None. Taking it from the listing page")
                    else:
                        logger.info(f"For {product_id}, num_reviews is {num_reviews}")
                        store_to_cache(f"NUM_REVIEWS_{product_id}", num_reviews)
                        total_ratings = num_reviews

                        if not isinstance(total_ratings, int):
                            total_ratings = 1000
                        
                        if incomplete == True:
                            if curr_reviews >= round(int(0.9 * total_ratings)):
                                # Mark as completed
                                logger.info(f"For Product {product_id}, marking as completed")
                                obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
                                if obj is not None:
                                    logger.info(f"Product with ID {product_id} is completed = {is_completed}")
                                    if hasattr(obj, 'completed'):
                                        setattr(obj, 'completed', True)
                                        try:
                                            db_session.commit()
                                            break
                                        except:
                                            db_session.rollback()
                                            logger.warning(f"For Product {product_id}, there is an error with the data.")
                                            logger.newline()
                                            break
                                else:
                                    error_logger.critical(f"Product with ID {product_id} not in DB. This shouldn't happen")
                                    break
                            else:
                                logger.info(f"For Product {product_id}, scraping reviews again. Curr Reviews = {curr_reviews}, while num_reviews = {total_ratings}")                                
                                d = db_manager.Reviews.delete().where(db_manager.Reviews.product_id == product_id)
                                d.execute()
                                logger.info(f"ID {product_id}, deleted old reviews")
                
                if use_cache:
                    # Store to cache first
                    with SqliteDict(cache_file, autocommit=True) as mydict:
                        mydict[f"REVIEWS_{product_id}_{curr}"] = reviews
                
                if USE_DB == True:
                    # Insert the reviews to the DB
                    try:
                        status = db_manager.insert_product_reviews(db_session, reviews, product_id=product_id)
                        if not status:
                            store_to_cache(f"REVIEWS_{product_id}_{curr}", reviews)
                    except:
                        store_to_cache(f"REVIEWS_{product_id}_{curr}", reviews)
                
                #with open(f'dumps/dump_{product_id}_reviews.pkl', 'wb') as f:
                #	pickle.dump(reviews, f)
                
                if first_request == True:
                    # First Request
                    first_request = False
                    response = my_proxy.get(server_url + reviews_url, referer=server_url + prev_url, product_url=product_url, ref_count='constant')
                    assert response.status_code == 200

                    time.sleep(random.randint(4, 5) + random.uniform(0, 1)) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(2, 5) + random.uniform(0, 1)))

                    prev_url = reviews_url
                    reviews_url = reviews_url + f"&sortBy=recent&pageNumber={curr+1}"
                    
                    # Now sort by date
                    logger.info("Now moving into sorting by most recent.")
                    continue
                else:
                    # We're sorting by most recent
                    if threshold_date is None:
                        pass
                    else:
                        limit = False
                        for review in reviews['reviews']:
                            review_date = review['review_date']
                            if review_date is not None:
                                # Review Date must be greater than threshold
                                if review_date < threshold_date:
                                    error_logger.info(f"{product_id} : Reviews (Current Page = {curr}) - Date Limit Exceeded.")
                                    limit = True
                                    is_completed = True
                                    break
                        if limit == True:
                            break
                
                if next_url is not None:
                    t_curr = reviews_url
                    t_prev = prev_url
                    prev_url = reviews_url
                    reviews_url = next_url
                    curr += 1
                    rand = random.randint(4, 17)
                    time.sleep(rand) if not speedup else (time.sleep(1 + random.uniform(0, 2)) if ultra_fast else time.sleep(random.randint(3, 8)))
                    rand = random.randint(0, 100)
                    
                    if rand <= 15 and curr > 1:
                        logger.info("Going back randomly")
                        response = my_proxy.get(server_url + t_prev, referer=server_url + t_curr, product_url=product_url, ref_count='constant')
                        time.sleep(random.randint(6, 12))
                        response = my_proxy.get(server_url + t_curr, referer=server_url + t_prev, product_url=product_url, ref_count='constant')
                        time.sleep(random.randint(6, 12))
                    
                    if review_pages is not None and curr == review_pages:
                        error_logger.info(f"{product_id} : Reviews (Current Page = {curr}) - Finished last page.")
                        is_completed = True
                        break
                    logger.info(f"Reviews: Going to Page {curr}")
                else:
                    # Approximating it to 75% total reviews
                    if total_ratings is not None and curr < round((0.75 * total_ratings) // REVIEWS_PER_PAGE):
                        t_curr = reviews_url
                        t_prev = prev_url
                        error_logger.warning(f"{product_id} : Reviews (Current Page = {curr}). Next Page is None. But total_ratings = {total_ratings}. Is there an error????")
                        error_logger.info("Trying again....")
                        
                        retry += 1
                        
                        if retry <= MAX_RETRIES:
                            response = my_proxy.get(server_url + t_prev, referer=server_url + t_curr, product_url=product_url, ref_count='constant')
                            time.sleep(random.randint(6, 12))
                            response = my_proxy.get(server_url + t_curr, referer=server_url + t_prev, product_url=product_url, ref_count='constant')
                            time.sleep(random.randint(6, 12))
                        else:
                            error_logger.error(f"{product_id} : Reviews (Current Page = {curr}). Next Page is None. Max retries exceeded. Exiting product...")
                            break
                    else:
                        error_logger.info(f"{product_id} : Reviews (Current Page = {curr}). Next Page is None. Finished Scraping Reviews for this product")
                        is_completed = True
                        break
    
    if dont_update == True:
        pass
    else:
        obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
        if obj is not None:
            logger.info(f"Product with ID {product_id} is completed = {is_completed}")
            if hasattr(obj, 'completed'):
                setattr(obj, 'completed', is_completed)
                try:
                    db_session.commit()
                except:
                    db_session.rollback()
                    logger.warning(f"For Product {product_id}, there is an error with the data.")
                    logger.newline()
        else:
            error_logger.critical(f"Product with ID {product_id} not in DB. This shouldn't happen")

    time.sleep(3)

    return final_results


def scrape_template_listing(categories=None, pages=None, dump=False, detail=False, threshold_date=None, products=None, review_pages=None, qanda_pages=None, no_listing=False, num_workers=None, worker_pages=None):
    global my_proxy, session
    global headers, cookies
    global last_product_detail
    global cache
    global use_multithreading
    global USE_DB
    global listing_templates, listing_categories

    if pages is None:
        pages = [10000 for _ in listing_templates] # Keeping a big number
    else:
        if isinstance(pages, int):
            if pages <= 0:
                raise ValueError("pages must be a positive integer")
            pages = [10000 for _ in listing_templates]

    server_url = 'https://www.amazon.in'
    
    if my_proxy is not None:
        try:
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
        for category, category_template, num_pages in zip(listing_categories, listing_templates, pages):
            fetch_category(category, category_template.substitute(PAGE_NUM=1), num_pages, change, server_url=server_url, no_listing=no_listing, detail=detail)
    else:
        no_sub = False
        if num_workers is None or not isinstance(num_workers, int):
            num_workers = max(1, min(32, len(listing_categories)))

        if len(listing_categories) == 1:
            # Only one category. Split it into pages
            if num_workers == 1:
                num_workers = 5
            logger.info(f"Only one category. Splitting work into {num_workers} threads")
            
            categories = [listing_categories[0] for _ in range(1, num_workers+1)]
            listing_categories = categories

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
        
        try:
            if concurrent_jobs == True and detail == True:
                num_workers *= 2
        except:
            pass
        
        logger.info(f"Have {len(listing_categories)} categories. Splitting work into {num_workers} threads")

        # TODO: https://stackoverflow.com/questions/56733397/how-i-can-get-new-ip-from-tor-every-requests-in-threads
        # Separate proxy object per thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Start the load operations and mark each future with its URL
            if no_sub == False:
                future_to_category = {executor.submit(fetch_category, category, category_template.substitute(PAGE_NUM=1), num_pages, change, server_url, no_listing, detail): category for category, category_template, num_pages in zip(listing_categories, listing_templates, pages)}
            else:
                future_to_category = {executor.submit(fetch_category, category, category_template, num_pages, change, server_url, no_listing, detail): category for category, category_template, num_pages in zip(listing_categories, listing_templates, pages)}
            
            try:
                if concurrent_jobs == True:
                    if detail == True:
                        # Add pure listing jobs too
                        future_to_category[executor.submit(fetch_category, category, category_template.substitute(PAGE_NUM=1), num_pages, change, server_url, True, False)] = f"{category}_listing"
            except:
                pass    
            
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

    return final_results



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--categories', help='List of all categories (comma separated)', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('--listing', help='Scraping the category listing', default=False, action='store_true')
    parser.add_argument('--detail', help='Scraping individual product details', default=False, action='store_true')
    parser.add_argument('-n', '--number', help='Number of Individual Product Details per category to fetch', type=int, default=0)
    parser.add_argument('--pages', help='Number of pages to scrape the listing details', type=lambda s: [int(item.strip()) for item in s.split(',')], default=1)
    parser.add_argument('--num_products', help='Number of products per category to scrape the listing details', type=lambda s: [int(item.strip()) for item in s.split(',')], default=None)
    parser.add_argument('--review_pages', help='Number of pages to scrape the reviews per product', type=int, default=100) # 100 pages Reviews (1000 reviews)
    parser.add_argument('--qanda_pages', help='Number of pages to scrape the qandas per product', type=int, default=10) # 10 pages QandA (100 QandAs)
    parser.add_argument('--dump', help='Flag for dumping the Product Listing Results for each category', default=False, action='store_true')
    parser.add_argument('-i', '--ids', help='List of all product_ids to scrape product details', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('--date', help='Threshold Limit for scraping Product Reviews', type=lambda s: datetime.strptime(s, '%Y-%m-%d'))
    parser.add_argument('--config', help='A config file for the options', type=str)
    parser.add_argument('--tor', help='To use Tor vs Public Proxies', default=False, action='store_true')
    parser.add_argument('--override', help='To scape using existing filters at utils.py', default=False, action='store_true')
    parser.add_argument('--no_listing', help='To specify if listing is needed while scraping details', default=False, action='store_true')
    parser.add_argument('--concurrent_jobs', help='To specify if listing + details need to be done', default=False, action='store_true')
    parser.add_argument('--num_workers', help='To specify number of worker threads', type=int, default=0)
    parser.add_argument('--worker_pages', help='Page per worker thread for product detail', type=lambda s: [int(item.strip()) for item in s.split(',')], default=None)

    args = parser.parse_args()

    categories = args.categories
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
    no_listing = args.no_listing
    concurrent_jobs = args.concurrent_jobs
    num_workers = args.num_workers
    worker_pages = args.worker_pages

    if num_workers <= 0:
        num_workers = None

    no_scrape = False

    # store the original SIGINT handler
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)

    try:
        if config is not None:
            # Iterate thru args
            for arg in vars(args):
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
                if arg not in ('config', 'number',) and getattr(args, arg) not in (None, False):
                    raise ValueError("--config file is already specified")
            
            option = None
            categories = []
            product_ids = []
            pages = []
            no_scrape = False # For scraping Listing

            options = ["Listing", "Details"]
            with open(f"{config}", "r") as f:
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
            setattr(my_proxy, 'use_tor', use_tor)
            if use_tor == False:
                my_proxy.proxy_list = my_proxy.get_proxy_list()
                my_proxy.switch_proxy()
        else:
            if use_tor == True:
                raise ValueError("Tor service is not available. Please start it")
            else:
                my_proxy = my_proxy = proxy.Proxy(OS=OS, use_tor=use_tor)
        
        logger.info(f"no_listing is {no_listing}")

        if categories is not None:
            if listing == True:
                if num_products is not None and isinstance(num_products, list):
                    assert len(num_products) == len(categories)
                
                if override == False:
                    results = scrape_category_listing(categories, pages=pages, dump=dump, detail=detail, threshold_date=threshold_date, products=num_products, review_pages=review_pages, qanda_pages=qanda_pages, no_listing=no_listing)
                else:
                    # Override
                    if isinstance(pages, list):
                        results = scrape_template_listing(categories=None, pages=pages, dump=dump, detail=detail, threshold_date=threshold_date, products=num_products, review_pages=review_pages, qanda_pages=qanda_pages, no_listing=no_listing, num_workers=num_workers, worker_pages=worker_pages)
                    else:
                        results = scrape_template_listing(categories=None, pages=None, dump=dump, detail=detail, threshold_date=threshold_date, products=num_products, review_pages=review_pages, qanda_pages=qanda_pages, no_listing=no_listing, num_workers=num_workers, worker_pages=worker_pages)
                """
                if detail == True:
                    for category in categories:
                        curr_item = 0
                        curr_page = 1

                        while curr_item < num_items:
                            if curr_page in results[category]:
                                for title in results[category][curr_page]:
                                    if results[category][curr_page][title]['product_url'] is not None:
                                        product_url = results[category][curr_page][title]['product_url']
                                        try:
                                            product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
                                        except Exception as ex:
                                            logger.critical(f"{ex}")
                                            logger.warning(f"Could not scrape details of Product - URL = {product_url}")
                                            logger.newline()
                                        curr_item += 1
                                        if curr_item == num_items:
                                            break
                            else:
                                break
                            curr_page += 1
                """
            else:
                for category in categories:
                    if product_ids is None:
                        with open(f'dumps/{category}.pkl', 'rb') as f:
                            results = pickle.load(f)
                        curr_item = 0
                        curr_page = 1

                        while curr_item < num_items:
                            if curr_page in results[category]:
                                for title in results[category][curr_page]:
                                    if results[category][curr_page][title]['product_url'] is not None:
                                        product_url = results[category][curr_page][title]['product_url']
                                        try:
                                            product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
                                        except Exception as ex:
                                            logger.critical(f"{ex}")
                                            logger.warning(f"Could not scrape details of Product - URL = {product_url}")
                                            logger.newline()
                                        curr_item += 1
                                        if curr_item == num_items:
                                            break
                            else:
                                break
                            curr_page += 1
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

                            jobs.append([category, product_url, review_pages, qanda_pages, threshold_date, None, None, True])
                            ids.append(product_id)
                            
                            if use_multithreading == False:
                                try: 
                                    product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
                                except Exception as ex:
                                    logger.critical(f"{ex}")
                                    logger.warning(f"Could not scrape details of Product ID {product_id} - URL = {product_url}")
                                    logger.newline()
                        
                        if use_multithreading == True:
                            num_jobs = len(jobs)
                            for idx, _job in enumerate(jobs[::num_workers]):
                                if terminate == True:
                                    logger.info("Terminating....")
                                batch_size = min(num_jobs - idx, num_workers)
                                logger.info(f"Now going for batch from idx {idx}with batch size {batch_size}...")
                                time.sleep(5)
                                with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
                                    future_to_category = dict()
                                    for i, job in enumerate(jobs[idx: idx + batch_size]):
                                        product_id = ids[idx + i]
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
                    product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
                except Exception as ex:
                    logger.critical(f"{ex}")
                    logger.warning(f"Could not scrape details of Product ID {product_id} - URL = {product_url}")
                    logger.newline()
    finally:
        db_session.close()
        #Session.remove()
        logger.info("Closed DB connections!")
