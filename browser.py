from selenium import webdriver
from selenium.webdriver import ActionChains
from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.firefox.options import Options
import time
import os


#ENTRY_URL = 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_n_feature_six_browse-bin%3A15564046031&dc&fst=as%3Aoff&qid=1599898509&rnid=15564019031&ref=sr_pg_1'

ENTRY_URL = "https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_n_feature_six_browse-bin%3A15564047031%7C15564048031&dc&fst=as%3Aoff&qid=1599295118&rnid=15564019031&ref=sr_nr_p_n_feature_six_browse-bin_1"

category = "headphones"
subcategory = "wireless"

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

    print(f"GET URL {url}")
    driver.get(url)

    while True:

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
            driver.quit()
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
                    actions = ActionChains(driver)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight-1300);")
                    time.sleep(5)
                    actions.move_to_element(e)
                    time.sleep(2)
                    actions.click(element).perform()
                    time.sleep(5)
                    print("Went to the next URL")
                    flag = False
                    break
            except Exception as ex:
                print(ex)
                continue

        if flag == True:
            driver.quit()
            break

        print("Sleeping...")
        time.sleep(2)


if __name__ == '__main__':
    run()
