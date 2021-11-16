import time
import math
from datetime import datetime
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.firefox.options import Options
from webdriver_manager.firefox import GeckoDriverManager
import glob
import os

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
    driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

    try:
        domain = config("DOMAIN")
        logger.info(f"Domain: {domain}")
        curr = 1

        try: #Initialize DB ses
            engine, Session = db_manager.connect_to_db(domain_to_db[domain], connection_params)
        except Exception as ex:
            logger.critical(f"Error during initiation session: {ex}")

        try:
            for category, base_url in domain_map[domain].items():
                url = base_url

                logger.info(f"Scraping category {category} with URL {url}")
                print(f"GET URL {url}")
                driver.get(url)
                scrape_listing(category, url, driver, domain)
                print("Sleeping...")
                time.sleep(2)

        except Exception as ex:
            print(ex)

        finally:
            try:
                db_manager.close_all_db_connections(engine, Session)
            except Exception as ex:
                logger.critical(f"Error when trying to close all sessions: {ex}")
    finally:
        driver.quit()


def scrape_listing(category, url, driver, domain):
    curr = 0

    while True:
        if url is None:
            logger.warning(f"No next URL. Skipping the rest...")
            break

        curr += 1
        print(f"At Page Number {curr}")
        print("Sleeping...")

        time.sleep(10) # Wait for some time to load

        html = driver.page_source.encode('utf-8', errors='ignore')

        # Extract listing page contents and write to DB
        try:
            soup = BeautifulSoup(html, 'lxml')
            product_info, _ = parse_data.get_product_info(soup)
            print(product_info)
            page_results = dict()
            page_results[category] = dict()
            page_results[category][curr] = product_info

            with db_manager.session_scope(Session) as _session:
                status = True
                print(page_results)
                # status = db_manager.insert_product_listing(_session, page_results, domain=domain)

            if not status:
                logger.warning(f"Error while inserting LISTING Page {curr} of category - {category}")


        except Exception as ex:
            logger.info(f"Exception during storing listing: {ex}")

        time.sleep(5)

        if domain == "amazon.in":
            url = click_next_url_amazon(url, curr, soup, driver)
        elif domain == "flipkart.com":
            url = click_next_url_flipkart(url, curr, soup, driver)


def click_next_url_amazon(url, curr, soup, driver):
    products_per_page = 40
     # Find link of next page, if "Next" link is not enabled, then quit
    try:
        element = driver.find_element_by_css_selector(".a-pagination .a-last")

        if element.is_enabled() == False:
            print("link  disabled")
            # Check if number of pages correspond to total elements
            total_products,_ = parse_data.get_total_products_number(soup)
            if curr != math.ceil(total_products/products_per_page):
                logger.warning(f"{category} category: No of items mismatch")
            return None

    except Exception as ex: #Link not found
        print(ex)

    try:
        e = element.find_element_by_tag_name("a")
    except:
        print("Tag element not found")
        return None

    url = e.get_attribute("href")
    scroll_and_click(driver, url)
    return url

def click_next_url_flipkart(url, curr, soup, driver):
    next_element = soup.find("a", class_="_1LKTO3", text="Next")

    # There is no next page
    if next_element is None:
        return None

    # If we're on page 25, which we identify by a highlighted page number,
    # we've hit the flipkart max limit and won't be able to get the next page.
    if soup.find("a", class_="_2Kfbh8", text="25"):
        return None

    elements = driver.find_elements_by_class_name("_1LKTO3")
    e = elements[-1] # Get the last element which is the Next page button

    url = e.get_attribute("href")
    scroll_and_click(driver, url)
    return url

def scroll_and_click(driver, url):
    print(f"URL is {url}")
    alpha = 1000 #Changes scroll height for all pages
    while alpha <= 5000:
        try:
            actions = ActionChains(driver)
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight-{alpha});")
            time.sleep(5)
            #driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            actions.move_to_element(element)
            time.sleep(2)
            print(f"Clicking URL {url}")
            actions.click(element).perform()
            time.sleep(5)
            break
        except Exception as ex:
            print(ex)
            print(f"Alpha is {alpha}. Now incrementing")
            alpha += 500
            time.sleep(1)

    print("Went to the next URL")

def run_subcategory(browser='Firefox'):

    options = Options()
    options.headless = True
    driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

    try:
        for category in subcategory_dict:
            for subcategory in subcategory_dict[category]:
                for subcategory_name in subcategory_dict[category][subcategory]:
                    value = subcategory_dict[category][subcategory][subcategory_name]
                    url = None

                    if isinstance(value, str):
                        url = value
                    else:
                        # None
                        continue

                    for filename in glob.glob(f"dumps/{category}_{db_manager.get_marketplace_prefix()}{subcategory_name}_*"):
                        if os.path.exists(filename):
                            os.remove(filename)

                    curr = 0

                    while True:

                        if url is None:
                            logger.warning(f"No next URL. Skipping the rest...")
                            break

                        curr += 1
                        print(f"GET URL {url}")
                        driver.get(url)

                        print(f"At Page Number {curr}")
                        print("Sleeping...")
                        time.sleep(12) # Wait for some time to load

                        html = driver.page_source.encode('utf-8', errors='ignore')
                        soup = BeautifulSoup(html, 'lxml')
                        with open(f'dumps/{category}_{db_manager.get_marketplace_prefix()}{subcategory_name}_{curr}.html', 'wb') as f:
                            f.write(html)

                        print("Written html. Sleeping...")
                        time.sleep(6)

                        if domain == "amazon.in":
                            url = click_next_url_amazon(url, curr, soup, driver)
                        elif domain == "flipkart.com":
                            url = click_next_url_flipkart(url, curr, soup, driver)

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
