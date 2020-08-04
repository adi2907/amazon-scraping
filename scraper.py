import argparse
import json
import os
import pickle
import random
import sqlite3
import sys
import time
from collections import OrderedDict
from datetime import datetime
from string import Template

import requests
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

import db_manager
import parse_data
import proxy
from utils import create_logger, customer_reviews_template, url_template

logger = create_logger('scraper')

headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

cookies = dict()

try:
    OS = config('OS')
except UndefinedValueError:
    OS = 'Windows'

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
    DB_NAME = config('DB_NAME')
    DB_SERVER = config('DB_SERVER')
    DB_TYPE = config('DB_TYPE')
    engine = db_manager.Database(dbtype=DB_TYPE, username=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, server=DB_SERVER).db_engine
except UndefinedValueError:
    DB_TYPE = 'sqlite'
    engine = db_manager.Database(dbtype=DB_TYPE).db_engine
    logger.warning("Using the default db.sqlite Database")
    logger.newline()


Session = sessionmaker(bind=engine)

db_session = Session()


def scrape_category_listing(categories, pages=None, dump=False):
    global my_proxy, session
    global headers, cookies
    # session = requests.Session()

    if pages is None:
        pages = [10000 for _ in categories] # Keeping a big number
    else:
        if isinstance(pages, int):
            if pages <= 0:
                raise ValueError("pages must be a positive integer")
            pages = [pages for _ in categories]

    server_url = 'https://amazon.in'
    
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
        my_proxy.cookies = cookies
    
    time.sleep(10)

    final_results = dict()

    change = False

    for category, num_pages in zip(categories, pages):
        logger.info(f"Now at category {category}, with num_pages {num_pages}")
        final_results[category] = dict()
        base_url = url_template.substitute(category=category)
        
        if my_proxy is not None:
            if change == True:
                change = False
                my_proxy.change_identity()
                time.sleep(random.randint(2, 5))
            
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
        
        time.sleep(5)
        curr_page = 1
        curr_url = base_url

        while curr_page <= num_pages:
            time.sleep(6)
            html = response.content
            soup = BeautifulSoup(html, 'html.parser')
                        
            product_info = parse_data.get_product_info(soup)

            final_results[category][curr_page] = product_info
            
            page_element = soup.find("ul", class_="a-pagination")
            
            if page_element is None:
                if my_proxy is None:
                    response = session.get(base_url, headers=headers, cookies=cookies)
                else:
                    response = my_proxy.get(base_url)
                
                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}
                
                logger.warning(f"Curr Page = {curr_page}. Pagination Element is None")

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
            curr_url = server_url + page_url

            time.sleep(5)
            curr_page += 1

            # Dump the results of this page to the DB
            page_results = dict()
            page_results[category] = final_results[category]
            db_manager.insert_product_listing(db_session, page_results)
            # Delete the previous page results
            if category in final_results and curr_page - 1 in final_results[category]:
                del final_results[category][curr_page - 1]
            
            logger.info(f"Finished Scraping Listing Page {curr_page - 1} of {category}")
        
        # Dump the category results
        results = dict()
        results[category] = final_results[category]
        
        if dump == True:
            if not os.path.exists(os.path.join(os.getcwd(), 'dumps')):
                os.mkdir(os.path.join(os.getcwd(), 'dumps'))
            
            with open(f'dumps/{category}.pkl', 'wb') as f:
                pickle.dump(results, f)
        
        # Insert to the DB
        db_manager.insert_product_listing(db_session, results)

        logger.info(f"Finished Scraping the LAST page {curr_page} of {category}")

        time.sleep(4)

        change = True
    return final_results


def scrape_product_detail(category, product_url, review_pages=None, qanda_pages=None, threshold_date=None):
    global my_proxy, session
    global headers, cookies
    # session = requests.Session()
    server_url = 'https://amazon.in'

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

    if my_proxy is None:
        response = session.get(server_url + product_url, headers=headers, cookies=cookies)
    else:
        response = my_proxy.get(server_url + product_url, product_url=product_url)
    
    if hasattr(response, 'cookies'):
        cookies = {**cookies, **dict(response.cookies)}
    
    time.sleep(10)

    final_results = dict()

    time.sleep(3)
    html = response.content
        
    product_id = parse_data.get_product_id(product_url)
    
    soup = BeautifulSoup(html, 'html.parser')

    # Get the product details
    details = parse_data.get_product_data(soup)
    details['product_id'] = product_id # Add the product ID
    
    # Check if the product is sponsored
    sponsored = parse_data.is_sponsored(product_url)

    # Insert to the DB
    db_manager.insert_product_details(db_session, details, is_sponsored=sponsored)
    
    time.sleep(4)
    
    # Get the qanda for this product
    if 'customer_lazy' in details and details['customer_lazy'] == True:
        qanda_url = details['customer_qa']
        curr = 0
        while qanda_url is not None:
            if my_proxy is None:
                response = session.get(qanda_url, headers={**headers, 'referer': server_url + product_url}, cookies=cookies)
            else:
                response = my_proxy.get(qanda_url, referer=server_url + product_url, product_url=product_url)
            
            if hasattr(response, 'cookies'):
                cookies = {**cookies, **dict(response.cookies)}
            assert response.status_code == 200
            
            time.sleep(5)
            html = response.content
            soup = BeautifulSoup(html, 'html.parser')
            qanda, next_url = parse_data.get_qanda(soup)
            
            # Insert to the DB
            db_manager.insert_product_qanda(db_session, qanda, product_id=product_id)
            
            if next_url is not None:
                logger.info(f"QandA: Going to Page {curr}")
                qanda_url = server_url + next_url
                curr += 1
                if qanda_pages is not None and curr == qanda_pages:
                    logger.info(f"QandA (Current Page = {curr}) - Finished last page. Going to Reviews now...")
                    logger.newline()
                    break
            else:
                logger.info(f"QandA (Current Page = {curr}) - Next Page is None. Going to Reviews now...")
                logger.newline()
                break
    
    # Get the customer reviews
    if details is not None and 'reviews_url' in details:
        reviews_url = details['reviews_url']
        prev_url = product_url
        curr = 0
        first_request = True
        while reviews_url is not None:
            if reviews_url is not None and product_url is not None:
                if my_proxy is None:
                    response = session.get(server_url + reviews_url, headers={**headers, 'referer': server_url + prev_url}, cookies=cookies)
                else:
                    if curr == 0 and first_request == False:
                        response = my_proxy.get(server_url + reviews_url, referer=server_url + prev_url, product_url=product_url, ref_count='constant')
                    else:
                        response = my_proxy.get(server_url + reviews_url, referer=server_url + prev_url, product_url=product_url)
               
                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}
                
                if response.status_code != 200:
                    logger.error(f"Review Page - Got code {response.status_code}")
                    logger.error(f"Content = {response.content}")

                assert response.status_code == 200
                time.sleep(5)
                
                html = response.content
                soup = BeautifulSoup(html, 'html.parser')

                reviews, next_url = parse_data.get_customer_reviews(soup)
                
                # Insert the reviews to the DB
                db_manager.insert_product_reviews(db_session, reviews, product_id=product_id)
                
                #with open(f'dumps/dump_{product_id}_reviews.pkl', 'wb') as f:
                #	pickle.dump(reviews, f)
                
                if first_request == True:
                    # First Request
                    first_request = False
                    response = my_proxy.get(server_url + reviews_url, referer=server_url + prev_url, product_url=product_url, ref_count='constant')
                    assert response.status_code == 200

                    time.sleep(random.randint(4, 5) + random.uniform(0, 1))

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
                                    logger.info(f"Reviews (Current Page = {curr}) - Date Limit Exceeded.")
                                    logger.newline()
                                    limit = True
                                    break
                        if limit == True:
                            break
                
                if next_url is not None:
                    prev_url = reviews_url
                    reviews_url = next_url
                    curr += 1
                    if review_pages is not None and curr == review_pages:
                        logger.info(f"Reviews (Current Page = {curr}) - Finished last page.")
                        logger.newline()
                        break
                    logger.info(f"Reviews: Going to Page {curr}")
                else:
                    logger.info(f"Reviews (Current Page = {curr}). Next Page is None. Finished Scraping Reviews for this product")
                    break
    
    time.sleep(3)

    return final_results



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--categories', help='List of all categories (comma separated)', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('--listing', help='Scraping the category listing', default=False, action='store_true')
    parser.add_argument('--detail', help='Scraping individual product details', default=False, action='store_true')
    parser.add_argument('-n', '--number', help='Number of Individual Product Details per category to fetch', type=int, default=0)
    parser.add_argument('--pages', help='Number of pages to scrape the listing details', type=lambda s: [int(item.strip()) for item in s.split(',')], default=1)
    parser.add_argument('--review_pages', help='Number of pages to scrape the reviews per product', type=int)
    parser.add_argument('--qanda_pages', help='Number of pages to scrape the qandas per product', type=int)
    parser.add_argument('--dump', help='Flag for dumping the Product Listing Results for each category', default=False, action='store_true')
    parser.add_argument('-i', '--ids', help='List of all product_ids to scrape product details', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('--date', help='Threshold Limit for scraping Product Reviews', type=lambda s: datetime.strptime(s, '%Y-%m-%d'))
    parser.add_argument('--config', help='A config file for the options', type=str)

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

    if config is not None:
        # Iterate thru args
        for arg in vars(args):
            if arg == 'pages' and getattr(args, arg) == 1:
                continue
            if arg not in ('config', 'number',) and getattr(args, arg) not in (None, False):
                raise ValueError("--config file is already specified")
        
        option = None
        categories = []
        product_ids = []
        pages = []

        options = ["Listing", "Details"]
        with open(f"{config}", "r") as f:
            for line in f:
                line = line.strip()
                if len(line) >= 2:
                    if line[0] == '#':
                        # Comment
                        text = line.split()[1]
                        if text in options:
                            option = text
                if option == 'Listing':
                    # Product Listing
                    if len(line) > 0 and line[0] != '#':
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
            assert len(pages) == len(categories)

    #if categories is not None and product_ids is not None:
    #    raise ValueError("Both --categories and --ids cannot be provided")

    if categories is not None:
        if listing == True:
            results = scrape_category_listing(categories, pages=pages, dump=dump)
            if detail == True:
                for category in categories:
                    curr_item = 0
                    curr_page = 1

                    while curr_item < num_items:
                        if curr_page in results[category]:
                            for title in results[category][curr_page]:
                                if results[category][curr_page][title]['product_url'] is not None:
                                    product_url = results[category][curr_page][title]['product_url']
                                    product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
                                    curr_item += 1
                                    if curr_item == num_items:
                                        break
                        else:
                            break
                        curr_page += 1
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
                                    product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
                                    curr_item += 1
                                    if curr_item == num_items:
                                        break
                        else:
                            break
                        curr_page += 1
                else:
                    # Product ids are also there
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
                        product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)

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
            product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=threshold_date)
