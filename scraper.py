import argparse
import os
import pickle
import random
import sqlite3
import sys
import time
from string import Template

import requests
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from sqlalchemy.orm import sessionmaker

import db_manager
import parse_data
import proxy

headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

url_template = Template('https://www.amazon.in/s?k=$category&ref=nb_sb_noss_2')

customer_reviews_template = Template('https://www.amazon.in/review/widgets/average-customer-review/popover/ref=acr_search__popover?ie=UTF8&asin=$PID&ref=acr_search__popover&contextId=search')

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
    my_proxy = None

# Dump Directory
#if not os.path.exists(os.path.join(os.getcwd(), 'dumps')):
#    os.mkdir(os.path.join(os.getcwd(), 'dumps'))

# Database Session setup
try:
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_NAME = config('DB_NAME')
    DB_SERVER = config('DB_SERVER')
    DB_TYPE = config('DB_TYPE')
    engine = db_manager.Database(dbtype=DB_TYPE, username=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, server=DB_SERVER)
except UndefinedValueError:
    DB_TYPE = 'sqlite'
    engine = db_manager.Database(dbtype=DB_TYPE).db_engine

db_manager.create_tables(engine)

Session = sessionmaker(bind=engine)

db_session = Session()


def goto_product_listing(category):
    global my_proxy
    if my_proxy is None:
        return
    
    my_proxy.change_identity()
    server_url = 'https://amazon.in'
    
    response = my_proxy.get(server_url)
    assert response.status_code == 200

    time.sleep(random.randint(4, 7))

    listing_url = url_template.substitute(category=category)
    response = my_proxy.get(listing_url)
    assert response.status_code == 200

    time.sleep(random.randint(3, 6))


def scrape_category_listing(categories, num_pages=None, dump=False):
    global my_proxy, session
    # session = requests.Session()

    if num_pages is None:
        num_pages = 10000 # Keeping a big number
    else:
        if not isinstance(num_pages, int) or num_pages <= 0:
            raise ValueError("num_pages must be a positive integer or None (for all pages)")

    server_url = 'https://amazon.in'
    
    if my_proxy is not None:
        try:
            response = my_proxy.get(server_url)
        except requests.exceptions.ConnectionError:
            print('Warning: No Proxy available via Tor relay. Mode = Normal')
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

    for category in categories:
        final_results[category] = dict()
        base_url = url_template.substitute(category=category)
        
        if my_proxy is not None:
            response = my_proxy.get(base_url)
        else:
            response = session.get(base_url, headers=headers, cookies=cookies)
        
        if response.status_code != 200:
            print(response.content)
            raise ValueError(f'Error: Got code {response.status_code}')
        
        if hasattr(response, 'cookies'):
            cookies = {**cookies, **dict(response.cookies)}
        
        time.sleep(5)
        curr_page = 1
        curr_url = base_url

        while curr_page <= num_pages:
            time.sleep(3)
            html = response.content
            soup = BeautifulSoup(html, 'html.parser')
            
            #if not os.path.exists(os.path.join(os.getcwd(), 'data', f'{category}')):
            #    os.mkdir(os.path.join(os.getcwd(), 'data', f'{category}'))
                        
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
                time.sleep(3)
                break
            
            page_url = next_page.find("a").attrs['href']

            if my_proxy is None:       
                response = session.get(server_url + page_url, headers={**headers, 'referer': curr_url}, cookies=cookies)
            else:
                response = my_proxy.get(server_url + page_url, referer=curr_url)
            
            if hasattr(response, 'cookies'):
                cookies = {**cookies, **dict(response.cookies)}
            curr_url = server_url + page_url
            time.sleep(5)
            curr_page += 1
        
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

        time.sleep(4)
    return final_results


def scrape_product_detail(category, product_url, review_pages=None, qanda_pages=None):
    global my_proxy, session
    # session = requests.Session()
    server_url = 'https://amazon.in'
    
    if my_proxy is None:
        response = session.get(server_url, headers=headers)
    else:
        response = my_proxy.get(server_url)
    
    assert response.status_code == 200
    cookies = dict(response.cookies)
    time.sleep(3)

    if my_proxy is None:
        response = session.get(server_url + product_url, headers=headers, cookies=cookies)
    else:
        response = my_proxy.get(server_url + product_url)
    
    if hasattr(response, 'cookies'):
        cookies = {**cookies, **dict(response.cookies)}
    
    time.sleep(10)

    final_results = dict()

    time.sleep(3)
    html = response.content
    
    #if not os.path.exists(os.path.join(os.getcwd(), 'data', f'{category}')):
    #    os.mkdir(os.path.join(os.getcwd(), 'data', f'{category}'))
        
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
                response = my_proxy.get(qanda_url, referer=server_url + product_url)
            
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
                print(f"QandA: Going to Page {curr}")
                qanda_url = server_url + next_url
                curr += 1
                if qanda_pages is not None and curr == qanda_pages:
                    break
            else:
                break
    
    # Get the customer reviews
    if 'customer_reviews' in details and details['customer_reviews'] is not None and 'reviews_url' in details['customer_reviews']:
        reviews_url = details['customer_reviews']['reviews_url']
        curr = 0
        while reviews_url is not None:
            if reviews_url is not None and product_url is not None:
                if my_proxy is None:
                    response = session.get(server_url + reviews_url, headers={**headers, 'referer': server_url + product_url}, cookies=cookies)
                else:
                    response = my_proxy.get(server_url + reviews_url, referer=server_url + product_url)
                
                if hasattr(response, 'cookies'):
                    cookies = {**cookies, **dict(response.cookies)}
                assert response.status_code == 200
                time.sleep(5)
                html = response.content
                soup = BeautifulSoup(html, 'html.parser')
                reviews, next_url = parse_data.get_customer_reviews(soup)
                
                # Insert the reviews to the DB
                db_manager.insert_product_reviews(db_session, reviews, product_id=product_id)
                
                #with open(f'dumps/dump_{product_id}_reviews.pkl', 'wb') as f:
                #	pickle.dump(reviews, f)
                if next_url is not None:
                    reviews_url = server_url + next_url
                    curr += 1
                    if review_pages is not None and curr == review_pages:
                        break
                    print(f"Reviews: Going to Page {curr}")
                else:
                    print("Finished Scraping Reviews for this product")
                    break
    
    time.sleep(3)

    return final_results



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--categories', help='List of all categories (comma separated)', type=lambda s: [item.strip() for item in s.split(',')])
    parser.add_argument('--listing', help='Scraping the category listing', default=False, action='store_true')
    parser.add_argument('--detail', help='Scraping individual product details', default=False, action='store_true')
    parser.add_argument('-n', '--number', help='Number of Individual Product Details per category to fetch', type=int, default=0)
    parser.add_argument('--pages', help='Number of pages to scrape the listing details per category', type=int, default=1)
    parser.add_argument('--review_pages', help='Number of pages to scrape the reviews per product', type=int)
    parser.add_argument('--qanda_pages', help='Number of pages to scrape the qandas per product', type=int)
    parser.add_argument('--dump', help='Flag for dumping the Product Listing Results for each category', default=False, action='store_true')

    args = parser.parse_args()

    categories = args.categories
    listing = args.listing
    detail = args.detail
    num_items = args.number
    num_pages = args.pages
    review_pages = args.review_pages
    qanda_pages = args.qanda_pages
    dump = args.dump

    MAX_ITEMS = random.randint(3, 7)

    if categories is not None:
        if listing == True:
            results = scrape_category_listing(categories, num_pages=num_pages, dump=dump)
            if detail == True:
                for category in categories:
                    curr_item = 0
                    curr_page = 1

                    # Reference Count for reset
                    reference = 0

                    while curr_item < num_items:
                        if curr_page in results[category]:
                            for title in results[category][curr_page]:
                                if results[category][curr_page][title]['product_url'] is not None:
                                    product_url = results[category][curr_page][title]['product_url']
                                    
                                    if reference > 0 and reference == MAX_ITEMS:
                                        # Change Identity
                                        goto_product_listing(category)
                                        reference = 0
                                        MAX_ITEMS = random.randint(2, 8)
                                    
                                    product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages)
                                    curr_item += 1
                                    reference += 1
                                    if curr_item == num_items:
                                        break
                        else:
                            break
                        curr_page += 1
        else:
            for category in categories:
                with open(f'dumps/{category}.pkl', 'rb') as f:
                    results = pickle.load(f)
                curr_item = 0
                curr_page = 1

                # Reference Count for reset
                reference = 0

                while curr_item < num_items:
                    if curr_page in results[category]:
                        for title in results[category][curr_page]:
                            if results[category][curr_page][title]['product_url'] is not None:
                                product_url = results[category][curr_page][title]['product_url']

                                if curr_item > 0 and curr_item == MAX_ITEMS:
                                    # Change Identity
                                    goto_product_listing(category)
                                    reference = 0
                                    MAX_ITEMS = random.randint(2, 8)

                                product_detail_results = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages)
                                curr_item += 1
                                reference += 1
                                if curr_item == num_items:
                                    break
                    else:
                        break
                    curr_page += 1
