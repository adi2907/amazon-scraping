import glob
import os
import time
from copy import deepcopy
from string import Template

import lxml
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.firefox.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager

import db_manager
import parse_data
from utils import listing_categories, listing_templates

ENTRY_URL = "https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_thirteen_browse-bin%3A2753043031&dc&qid=1599327930&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_2"

category = "refrigerator"
subcategory = "double door"

#ENTRY_URL = "https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_thirteen_browse-bin%3A2753039031%7C2753045031&dc&qid=1599327851&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_2"

#category = "refrigerator"
#subcategory = "multi door"

# wireless -> Last URL https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_n_feature_six_browse-bin%3A15564047031%7C15564048031&dc&page=36&fst=as%3Aoff&qid=1599907737&rnid=15564019031&ref=sr_pg_35


def run_category(browser='Firefox'):
    options = Options()
    options.headless = True
    if browser == 'Chrome':
        # Use chrome
        driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=options)
    elif browser == 'Firefox':
        # Set it to Firefox
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

    curr = 1

    server_url = 'https://www.amazon.in'
    
    try:

        assert len(listing_categories) == len(listing_templates)

        for category, template in zip(listing_categories, listing_templates):
            url = template.substitute(PAGE_NUM=1)
            
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

                with open(f'dumps/listing_{category}_{curr}.html', 'wb') as f:
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
        
    except Exception as ex:
        print(ex)
    
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

    url = ENTRY_URL

    curr = 1

    server_url = 'https://www.amazon.in'

    category_map = {"washing machine": [["fully automatic", "https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_sixteen_browse-bin%3A2753055031&dc&qid=1599329326&rnid=2753054031&ref=sr_nr_p_n_feature_sixteen_browse-bin_1"], ["semi automatic", "https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_sixteen_browse-bin%3A2753056031&dc&qid=1599329355&rnid=2753054031&ref=sr_nr_p_n_feature_sixteen_browse-bin_2"], ["top load", "https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_fifteen_browse-bin%3A2753052031&dc&qid=1599329495&rnid=2753051031&ref=sr_nr_p_n_feature_fifteen_browse-bin_2"], ["front load", "https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_fifteen_browse-bin%3A2753053031&dc&qid=1599329490&rnid=2753051031&ref=sr_nr_p_n_feature_fifteen_browse-bin_1"]]}

    #category_map = {"refrigerator": [["single door", "https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_thirteen_browse-bin%3A2753044031&dc&qid=1599984176&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_3"]]}
    
    #category_map = {"refrigerator": [["multi door", "https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_thirteen_browse-bin%3A2753039031%7C2753045031&dc&qid=1599327851&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_2"], ["direct cool", "https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_eleven_browse-bin%3A2753030031&dc&qid=1599329198&rnid=2753029031&ref=sr_nr_p_n_feature_eleven_browse-bin_1"], ["frost free", "https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_eleven_browse-bin%3A2753031031&dc&qid=1599329267&rnid=2753029031&ref=sr_nr_p_n_feature_eleven_browse-bin_1"]]}

    try:
        for category in category_map:
            for item in category_map[category]:
                subcategory, url = item[0], item[1]
                
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

                    with open(f'dumps/{category}_{subcategory}_{curr}.html', 'wb') as f:
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


def insert_category_to_db(category):
    DUMP_DIR = os.path.join(os.getcwd(), 'dumps')
    if not os.path.exists(DUMP_DIR):
        raise ValueError("Dump Directory not present")

    from sqlalchemy.orm import sessionmaker

    Session = sessionmaker(bind=db_manager.engine, autocommit=False, autoflush=True)
    session = Session()

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
        page_results[category][idx] = final_results[category][idx]

        status = db_manager.insert_product_listing(session, page_results)

        if not status:
            print(f"Error while inserting Page {idx} of category - {category}")
            continue


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
            if len(sys.argv) == 3:
                category = sys.argv[2]
                print("Inserting listing")
                insert_category_to_db(category)
            else:
                print("Need to specify category")
        else:
            print("Invalid argument")
    else:
        print("Should have one argument")
