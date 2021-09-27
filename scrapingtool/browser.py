import glob
import os
import time
import traceback
import math
from datetime import datetime
from string import Template

import lxml
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webelement import WebElement
from sqlitedict import SqliteDict
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager

import db_manager
import parse_data
from subcategories import subcategory_dict
from utils import (category_to_domain, create_logger, domain_map, domain_to_db,
                   is_lambda, listing_categories, listing_templates)

logger = create_logger('browser')

today = datetime.today().strftime("%d-%m-%y")
PRODUCTS_PER_PAGE = 25
connection_params = db_manager.get_credentials()

def run_category(browser='Firefox'):
    options = Options()
    options.headless = True
    if browser == 'Chrome':
        # Use chrome
        driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=options)
    elif browser == 'Firefox':
        # Set it to Firefox
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)
    
    from sqlalchemy.orm import sessionmaker
    
    try:
        for domain in domain_map:
            logger.info(f"Domain: {domain}")
            curr = 1
            
            try: #Initialize DB ses
                engine, Session = db_manager.connect_to_db(domain_to_db[domain], connection_params)
                engine.execute(f"UPDATE ProductListing SET is_active = False")
            except Exception as ex:
                traceback.print_exc()
                logger.critical(f"Error during initiation session: {ex}")

            try:
                prev_url = ''

                for category, base_url in domain_map[domain].items():
                    url = base_url

                    logger.info(f"Scraping category {category} with URL {url}")
                    
                    # page number
                    curr = 1

                    print(f"GET URL {url}")
                    driver.get(url)
                    while True:
                        if url == prev_url:
                            logger.warning(f"Got the same URL {url}. Skipping the rest...")
                            break
                        print(f"At Page Number {curr}")
                        print("Sleeping...")
                        time.sleep(10) # Wait for some time to load

                        html = driver.page_source.encode('utf-8', errors='ignore')

                        try:
                            captcha = driver.find_element_by_id("captchacharacters")
                            print("Fuck. Captcha")
                            time.sleep(10)
                            driver.get(url)
                            continue
                        except:
                            pass
                        
                        # Extract page contents and write to DB
                        try:
                            soup = BeautifulSoup(html, 'lxml')
                            product_info, _ = parse_data.get_product_info(soup)
                            
                            page_results = dict()
                            page_results[category] = dict()
                            page_results[category][curr] = product_info
                            
                            with db_manager.session_scope(Session) as _session:
                                status = db_manager.insert_product_listing(_session, page_results, domain=domain)

                            if not status:
                                logger.warning(f"Error while inserting LISTING Page {curr} of category - {category}")

                            with db_manager.session_scope(Session) as _session:
                                status = db_manager.insert_daily_product_listing(_session, page_results)

                            if not status:
                                logger.warning(f"Error while inserting DAILY LISTING Page {curr} of category - {category}")


                        except Exception as ex:
                            traceback.print_exc()
                            logger.info(f"Exception during storing daily listing: {ex}")
                
                        time.sleep(5)

                        # Find link of next page, if "Next" link is not enabled, then quit
                        try:
                            element = driver.find_element_by_css_selector("a[class='s-pagination-item s-pagination-next s-pagination-button s-pagination-separator']")
                            #element = driver.find_element_by_class_name('s-pagination-item s-pagination-next s-pagination-button s-pagination-separator')
                            if element.is_enabled() == False:
                                print("link  disabled")
                                # Check if number of pages correspond to total elements
                                total_products,_ = parse_data.get_total_products_number(soup)
                                if curr != math.ceil(total_products/PRODUCTS_PER_PAGE):
                                    logger.warning(f"{category} category: No of items mismatch")
                                break
                        except Exception as ex: #Link not found
                            print(ex)
                            
                            # template_url = listing_templates[category]
                            # url = template_url.substitute(PAGE_NUM=curr)
                            # total_products,curr_listing = parse_data.get_total_products_number(soup)
                            
                            # if curr_listing>total_products:
                            #     logger.info(f"Current listing {curr_listing} exceeds total products {total_products}. Quitting")
                            #     if curr != math.ceil(total_products/PRODUCTS_PER_PAGE):
                            #         logger.warning(f"{category} category: No of items mismatch")
                            #     break
                            
                            # curr+=1
                            # continue
                        # Click on next link
                        
                        tmp = url
                        #Child link of this element
                        try:
                            url = element.get_attribute("href")
                        except:
                            print("Next page url not found")
                            total_products = parse_data.get_total_products_number(soup)
                            if curr != math.ceil(total_products/PRODUCTS_PER_PAGE):
                                logger.warning(f"{category} category: No of items mismatch")
                            break
                        
                        print(f"URL is {url}")
                        curr += 1
                        alpha = 1000 #Changes scroll height for all pages
                        while alpha <= 5000:
                            try:
                                actions = ActionChains(driver)
                                driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight-{alpha});")
                                time.sleep(5)
                                #driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                actions.move_to_element(element)
                                time.sleep(2)
                                actions.click(element).perform()
                                time.sleep(5)
                                break
                            except Exception as ex:
                                print(ex)
                                print(f"Alpha is {alpha}. Now incrementing")
                                alpha += 500
                                time.sleep(1)

                        print("Went to the next URL")
                        prev_url = tmp
                    
                                
                    print("Sleeping...")
                    time.sleep(2)
                
            except Exception as ex:
                print(ex)
            
            finally:
                logger.info(f"Updating duplicate indices...")
                # Update set indexes
                try:
                    with db_manager.session_scope(Session) as _session:
                        db_manager.update_duplicate_set(_session, table='ProductListing', insert=True)
                        logger.info("Updated indexes!")
                except Exception as ex:
                    logger.critical(f"Error when updating listing indexes: {ex}")
                
                try:
                    db_manager.close_all_db_connections(engine, Session)
                except Exception as ex:
                    logger.critical(f"Error when trying to close all sessions: {ex}")
    finally:
        driver.quit()


def run_subcategory(browser='Firefox'):

    options = Options()
    options.headless = True
    if browser == 'Chrome':
        # Use chrome
        driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=options)
    elif browser == 'Firefox':
        # Set it to Firefox
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

    url = ''

    curr = 1

    try:
        for category in subcategory_dict:
            for subcategory in subcategory_dict[category]:
                for subcategory_name in subcategory_dict[category][subcategory]:
                    value = subcategory_dict[category][subcategory][subcategory_name]
                    url = None
                    
                    if isinstance(value, str):
                        # Url
                        url = value
                    else:
                        # None
                        continue

                    if url is None:
                        continue
                    
                    for filename in glob.glob(f"dumps/{category}_{subcategory_name}_*"):
                        if os.path.exists(filename):
                            os.remove(filename)
                    
                    curr = 1
                          
                    while True:
                        print(f"GET URL {url}")
                        driver.get(url)
                        
                        print(f"At Page Number {curr}")
                        print("Sleeping...")
                        time.sleep(12) # Wait for some time to load

                        html = driver.page_source.encode('utf-8', errors='ignore')
                        soup = BeautifulSoup(html, 'lxml')
                        with open(f'dumps/{category}_{subcategory_name}_{curr}.html', 'wb') as f:
                            f.write(html)

                        print("Written html. Sleeping...")
                        time.sleep(6)
                        
                        
                        # Find link of next page, if "Next" link is not enabled, then quit
                        try:
                            #element = driver.find_element_by_css_selector(".a-pagination .a-last")
                            element = driver.find_element_by_css_selector("a[class='s-pagination-item s-pagination-next s-pagination-button s-pagination-separator']")
                            if element.is_enabled() == False:
                                total_products,_ = parse_data.get_total_products_number(soup)
                                if curr != math.ceil(total_products/PRODUCTS_PER_PAGE):
                                    logger.warning(f"{subcategory} subcategory: No of items mismatch")
                                break
                        except Exception as ex: #Link not found
                            print(ex)
                            total_products,_ = parse_data.get_total_products_number(soup)
                            if curr != math.ceil(total_products/PRODUCTS_PER_PAGE):
                                logger.warning(f"{subcategory} subcategory: No of items mismatch")
                            print("Next page not found. Quitting...")
                            break

                        
                        #Child link of this element
                        # try:
                        #     e = element.find_element_by_tag_name("a")
                        # except:
                        #     print("Tag element not found")
                        #     total_products,_ = parse_data.get_total_products_number(soup)
                        #     if curr != math.ceil(total_products/PRODUCTS_PER_PAGE):
                        #         logger.warning(f"{subcategory} subcategory: No of items mismatch")
                        #     break
                        
                        url = element.get_attribute("href")
                        
                        if url is not None:           
                            print(f"URL is {url}")
                            curr += 1
                            alpha = 1000 #Changes scroll height for all pages
                            while alpha <= 5000:
                                try:
                                    actions = ActionChains(driver)
                                    driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight-{alpha});")
                                    time.sleep(5)
                                    actions.move_to_element(element)
                                    time.sleep(2)
                                    actions.click(element).perform()
                                    time.sleep(5)
                                    break
                                except Exception as ex:
                                    print(ex)
                                    print(f"Alpha is {alpha}. Now incrementing")
                                    alpha += 500
                                    time.sleep(1)

                            print("Went to the next URL")
                                
                        print("Sleeping...")
                        time.sleep(2)
        driver.quit()
    except Exception as ex:
        print(ex)
        driver.quit()



if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == 'subcategory':
            print("Subcategory")
            run_subcategory()
        elif sys.argv[1] == 'category':
            print("Category")
            run_category()
        else:
            print("Invalid argument")
    else:
        print("Should have one argument")
