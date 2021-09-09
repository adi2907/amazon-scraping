from logging import error
import requests
import concurrent.futures
import os

#get the list of free proxies
def getProxies():
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    proxy_file = os.path.join(__location__, 'proxy_list.txt')
    if os.path.exists(proxy_file):
        proxy_list = []
        with open(proxy_file, 'r') as f:
            for line in f:
                proxy_list.append(line.strip())
        #proxy_list = [f"socks5h://{ip}" for ip in proxy_list]
        
        return proxy_list
    else:
        print("unable to get proxy list file")

def extract(proxy):
    #this was for when we took a list into the function, without conc futures.
    #proxy = random.choice(proxylist)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'}
    #headers['referer']='www.google.com'
    try:
        r = requests.get('https://www.amazon.in/Crompton-Hill-1200mm-Ceiling-Brown/dp/B015H0AKTS/ref=sr_1_1?crid=1TGIH58I2LW9I&dchild=1&keywords=ceiling+fan&m=AT95IG9ONZD7S&qid=1630471596&refinements=p_6%3AAT95IG9ONZD7S%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031&rnid=1318483031&s=kitchen&sprefix=ceili%2Caps%2C380&sr=1-1', headers=headers, proxies={'http' : proxy,'https': proxy}, timeout=5)
        #r = requests.get('https://www.amazon.in',headers=headers,timeout =5)
        print(r.status_code," ",proxy)
    except Exception as ex:
        print (ex)
    return proxy

proxylist = getProxies()
print(len(proxylist))

#check them all with futures super quick
with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(extract, proxylist)
