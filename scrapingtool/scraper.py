import random
import argparse
from sqlalchemy.sql.operators import is_
import parse_data
import time
from copy import deepcopy
from datetime import datetime, timedelta


import requests
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from sqlalchemy import asc, desc, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlitedict import SqliteDict

import db_manager
import parse_data
import proxy
from utils import (category_to_domain, create_logger,
                   customer_reviews_template, domain_map, domain_to_db,
                   listing_categories, listing_templates, qanda_template,
                   subcategory_map, url_template)

logger = create_logger('scraper')

error_logger = create_logger('errors')

headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

cookies = dict()
today = datetime.today().strftime("%d-%m-%y")

try:
    USE_PROXY = config('USE_PROXY', cast=bool)
except UndefinedValueError:
    USE_PROXY = True

logger.info(f"USE_PROXY = {USE_PROXY}")


try:
    OS = config('OS')
except UndefinedValueError:
    OS = 'Linux'

try:
    cache_file = config('cache_file')
except UndefinedValueError:
    cache_file = 'cache.sqlite3'

logger.info(f"Using Sqlite3 Cache File = {cache_file}")


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

db_session = Session()

def scrape_product_detail(product_url,category=None,threshold_date=None, listing_url=None):
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
    global cache_file

    # set default domain
    domain = category_to_domain[category] if category is not None else 'amazon.in'
    
    server_url = 'https://www.' + domain if not product_url.startswith('http') else ""

    product_id = parse_data.get_product_id(product_url)

    # Get DB credentials and connect to DB
    connection_params = db_manager.get_credentials()
    _engine,SessionFactory = db_manager.connect_to_db(domain_to_db[domain], connection_params)

    reviews_completed = False
    qanda_completed = False
    detail_completed = False
    
    #Assign threshold_date if none
    if threshold_date is None:
        threshold_date = datetime.now() - timedelta(days=90)
        
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
        
        time.sleep(10) 

        final_results = dict()

        time.sleep(3)
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
            time.sleep(random.randint(5, 10))
            
    details['product_id'] = product_id # Add the product ID
    details['duplicate_set'] = duplicate_set
    
    # Check if the product is sponsored
    sponsored = parse_data.is_sponsored(product_url)
    
    # Validate some important fields
    important_fields = ['product_title', 'product_details', 'reviews_url', 'customer_qa']
    empty_fields = []
    for field in important_fields:
        if details.get(field) in [None, "", {}, []]:
            empty_fields.append(field)
    
    if empty_fields != []:
        msg = ','.join([field for field in empty_fields])
        logger.critical(f"{msg} fields are missing in product details" )
    
    # Insert to the DB
    try:
        with db_manager.session_scope(SessionFactory) as db_session:
            status = db_manager.insert_product_details(db_session, details, is_sponsored=sponsored)
            # if Database insertion fails, store in cache to examine later
            if status == True:
                detail_completed = True
            else:
                logger.critical(f"Couldn't insert product details for {product_id}")
    except:
        logger.critical(f"Couldn't insert product details for {product_id}")
    
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
            threshold_date = last_qanda_date + timedelta(days=1) if last_qanda_date > threshold_date else threshold_date
        
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
            threshold_date = last_review_date + timedelta(days=1) if last_review_date > threshold_date else threshold_date
        
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
# 3. Add unique QandA where date exceeded last qanda date

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

        time.sleep(5)
        
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
                is_completed = True
                break
            rand = random.randint(4, 17)
            time.sleep(rand)
            logger.info(f"QandA: Going to Page {curr}")
        else:
            is_completed = True
            break
    
    return is_completed

#TODO: Need to handle following usecases
# 1. Not able to scrape all reviews till a date - retry mechanism or delete all DB entries in case not successful
# 2. Only one page of review
# 3. Add unique reviews where date exceeded last review date


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

        time.sleep(5)
        
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
                is_completed=True
                break
            rand = random.randint(4, 17)
            time.sleep(rand)
            logger.info(f"Reviews: Going to Page {curr+1}")
        else:
            is_completed = True
            break
   
    return is_completed

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--categories', help='List of all categories (comma separated)', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('--url', help='Scrape the product detail for the url', type=str, default=None)
    args = parser.parse_args()
    
        
    if args.categories:
        for category in args.categories:             
            # Get product urls for categories with detail_completed = None or done a week prior
            product_urls = db_manager.fetch_product_urls_unscrapped_details(db_session,category,'ProductListing')
            for product_url in product_urls:
                pd_id = product_url.split('/')[3]
                
                # Don't scrape products dormant for more than 2 months
                last_review_inDB = db_manager.get_last_review_date(db_session,pd_id)
                
                # If no review exists or last review is more than 2 months old
                if last_review_inDB is None or last_review_inDB < datetime.now() - timedelta(days=60):
                    # if product detail has been scrapped earlier
                    if db_manager.get_detail_scrapped_date(db_session,pd_id) is not None:
                        logger.info(f"Product {pd_id} has been dormant for more than 2 months")
                        continue
                
                logger.info(f"Scraping product details for product_id {pd_id}")
                try:
                    product_detail_results = scrape_product_detail(product_url,category)
                except Exception as ex:
                    logger.critical(f"{ex}")
                    logger.warning(f"Could not scrape details of Product - URL = {product_url}")
                    my_proxy.switch_proxy()
                    continue
    if args.url:
        scrape_product_detail(args.url)
    