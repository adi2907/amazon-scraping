import glob
import os
import time
import traceback
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

            server_url = f'https://www.{domain}'
            
            try:
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
                    
                    curr = 1

                    print(f"GET URL {url}")
                    driver.get(url)
                    flag = False
                    while True:
                        if url == prev_url:
                            logger.warning(f"Got the same URL {url}. Skipping the rest...")
                            break
                        print(f"At Page Number {curr}")
                        print("Sleeping...")
                        time.sleep(12) # Wait for some time to load

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

                            # If the "Link" was disabled in the last iteration, quit the program
                            if flag == True:
                                break

                        except Exception as ex:
                            traceback.print_exc()
                            logger.info(f"Exception during storing daily listing: {ex}")

                    
                        with open(f'dumps/listing_{category}_{curr}.html', 'wb') as f:
                            f.write(html)
                

                        print("Written html. Sleeping...")
                        time.sleep(2)

                        # Find link of next page, if "Next" link is not enabled, then quit
                        try:
                            element = driver.find_element_by_css_selector(".a-pagination .a-last")
                            if element.is_enabled() == False:
                                flag = True
                        except Exception as ex: #Link not found
                            print(ex)
                            print("Next page not found. Quitting...")
                            break

                        # Click on next link
                        try:
                            tmp = url
                            #Child link of this element
                            try:
                                e = element.find_element_by_tag_name("a")
                            except:
                                flag = True
                                print("Tag element not found")
                            
                            url = e.get_attribute("href")
                            print(url)

                            if url is not None:
                                if not url.startswith(server_url):
                                    url = server_url + url
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
                                
                        except Exception as ex:
                            print(ex)
                            continue

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
    #from pyvirtualdisplay import Display
    #display = Display(visible=0, size=(800, 800))
    #display.start()

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
            server_url = 'https://' + category_to_domain[category]
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

                    print(f"GET URL {url}")
                    driver.get(url)

                    while True:

                        print(f"At Page Number {curr}")
                        print("Sleeping...")
                        time.sleep(12) # Wait for some time to load

                        html = driver.page_source.encode('utf-8', errors='ignore')

                        try:
                            captcha = driver.find_element_by_id("captchacharacters")
                            print("Fuck. Captcha")
                            time.sleep(10)
                            driver.get(url)
                            continue
                        except:
                            pass
                
                        with open(f'dumps/{category}_{subcategory_name}_{curr}.html', 'wb') as f:
                            f.write(html)

                        print("Written html. Sleeping...")
                        time.sleep(2)

                        flag = True

                        try:
                            elements = driver.find_elements_by_class_name("a-last")
                        except:
                            print("Next page not found. Quitting...")
                            break

                        for element in elements:
                            try:
                                #url = element.get_attribute("href")
                                e = driver.find_element_by_css_selector(".a-last > a:nth-child(1)")
                                url = e.get_attribute("href")
                                print(url)
                                if url is not None:
                                    if not url.startswith(server_url):
                                        url = server_url + url
                                    print(f"URL is {url}")
                                    curr += 1
                                    alpha = 1000
                                    while alpha <= 5000:
                                        try:
                                            actions = ActionChains(driver)
                                            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight-{alpha});")
                                            time.sleep(5)
                                            actions.move_to_element(e)
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
                                    flag = False
                                    break
                            except Exception as ex:
                                print(ex)
                                continue

                        if flag == True:
                            break

                        print("Sleeping...")
                        time.sleep(2)
        driver.quit()
    except Exception as ex:
        print(ex)
        driver.quit()


def insert_category_to_db(category, domain='all'):
    # Deprecated function: No longer needed
    DUMP_DIR = os.path.join(os.getcwd(), 'dumps')
    if not os.path.exists(DUMP_DIR):
        raise ValueError("Dump Directory not present")

    from sqlalchemy.orm import sessionmaker

    if category != 'all':
        raise ValueError(f"Need to provide category = all")

    for _domain in domain_map:
        if domain == 'all':
            pass
        else:
            if _domain != domain:
                continue
        try:
            try:
                engine, Session = db_manager.connect_to_db(domain_to_db[_domain], connection_params)
            except Exception as ex:
                traceback.print_exc()
                logger.critical(f"Error during initiation session: {ex}")

            for category, _ in domain_map[domain].items():

                files = sorted(glob.glob(os.path.join(DUMP_DIR, f"listing_{category}_*")), key=lambda x: int(x.split('.')[0].split('_')[-1]))

                final_results = dict()

                final_results[category] = dict()

                for idx, filename in enumerate(files):
                    with open(filename, 'rb') as f:
                        html = f.read()

                    soup = BeautifulSoup(html, 'lxml')
                    product_info, _ = parse_data.get_product_info(soup)

                    final_results[category][idx + 1] = product_info

                    page_results = dict()
                    page_results[category] = dict()
                    page_results[category][idx + 1] = final_results[category][idx + 1]

                    with db_manager.session_scope(Session) as session:
                        status = db_manager.insert_product_listing(session, page_results, domain=_domain)

                    if not status:
                        print(f"Error while inserting Page {idx + 1} of category - {category}")
                        continue
        finally:
            try:
                db_manager.close_all_db_connections(engine, Session)
            except Exception as ex:
                logger.critical(f"Error when trying to close all sessions: {ex}")


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == 'subcategory':
            print("Subcategory")
            run_subcategory()
        elif sys.argv[1] == 'category':
            print("Category")
            run_category()
        elif sys.argv[1] == 'listing':
            if len(sys.argv) == 4:
                category = sys.argv[2]
                domain = sys.argv[3]
                print("Inserting listing")
                if category == 'all' and domain == 'all':
                    insert_category_to_db('all', domain='all')
                else:
                    insert_category_to_db(category, domain)
            else:
                print("Need to specify category, domain")
        else:
            print("Invalid argument")
    else:
        print("Should have one argument")
