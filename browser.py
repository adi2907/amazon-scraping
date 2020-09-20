from selenium import webdriver
from selenium.webdriver import ActionChains
from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.firefox.options import Options
import time
import os



ENTRY_URL = "https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_thirteen_browse-bin%3A2753043031&dc&qid=1599327930&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_2"

category = "refrigerator"
subcategory = "double door"

#ENTRY_URL = "https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_thirteen_browse-bin%3A2753039031%7C2753045031&dc&qid=1599327851&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_2"

#category = "refrigerator"
#subcategory = "multi door"

# wireless -> Last URL https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_n_feature_six_browse-bin%3A15564047031%7C15564048031&dc&page=36&fst=as%3Aoff&qid=1599907737&rnid=15564019031&ref=sr_pg_35

def run(browser='Firefox'):
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

if __name__ == '__main__':
    run()
