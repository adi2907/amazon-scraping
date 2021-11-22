import os
import random
import requests
import itertools
import time
from decouple import config


from utils import (create_logger)
logger = create_logger(__name__)


class Proxy():
    """Our own Proxy Class 
    """
    def __init__(self,OS='Linux', use_proxy=True):
        domain = config("DOMAIN")
        self.server_url = f"https://www.{domain}"
        self.use_proxy = use_proxy
        self.proxies = {
                'http': None,
                'https': None,
            }
        if self.use_proxy == True:
            self.proxy_list = self.get_proxy_list()
            ip = self.switch_proxy()
        print("Proxy ip is "+ip)
        
        if OS == 'Linux':
            self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        else:
            # Windows
            self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0"
        
        self.headers = dict()
        host = f"www.{domain}"
        self.headers = {"Accept-Encoding":"gzip, deflate, br", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5", "Connection":"keep-alive", "DNT": "1", "Host": host, "Upgrade-Insecure-Requests":"1", "User-Agent": self.user_agent}
        self.headers['Sec-Fetch-Dest'] = 'document'
        self.headers['Sec-Fetch-Mode'] = 'navigate'
        self.headers['Sec-Fetch-User'] = '?1'
        self.headers['TE'] = 'Trailers'
        
        self.user_agent_choices = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0",
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36",
            "Mozilla/5.0 (X11; Linux i686; rv:78.0) Gecko/20100101 Firefox/78.0",
            "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux i686; rv:78.0) Gecko/20100101 Firefox/78.0",
        ]
    
    def get(self,url,referrer_url=""):       
        if referrer_url:
                self.headers['referer'] =  referrer_url

        response = requests.get(url,headers = self.headers,proxies = self.proxies)
        if response.status_code == 200:
            if hasattr(response, 'cookies'):
                self.cookies = {**(self.cookies), **dict(response.cookies)}
        else:
            logger.critical(f"Error received for proxy{self.get_ip}. Deleting it")
            with open("proxy_list.txt", "r") as f:
                lines = f.readlines()
            with open("proxy_list.txt", "w") as f:
                for line in lines:
                    if line.strip("\n") != self.proxies['http']:
                        f.write(line)
        return response
            
    def get_proxy_list(self) -> list:
        """Fetches the list of active proxies
        """
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        proxy_file = os.path.join(__location__, 'proxy_list.txt')
        if os.path.exists(proxy_file):
            proxy_list = []
            with open(proxy_file, 'r') as f:
                for line in f:
                    proxy_list.append(line.strip())
            return proxy_list
        else:
            logger.critical(f"Proxy list file not found")
        
    
    def switch_proxy(self):
        if self.use_proxy == False:
            return

        if len(self.proxy_list) < 2:
            raise ValueError(f"Proxy List must have atleast 2 elements")

        proxy = self.proxies['https']
        
        new_proxy = random.choice(self.proxy_list)

        while proxy == new_proxy:
            new_proxy = random.choice(self.proxy_list)

        self.proxies = {
            'http': f'{new_proxy}',
            'https': f'{new_proxy}',
        }
        # Reset session cookies
        self.cookies = dict()

        urls = ['https://ident.me','http://myip.dnsomatic.com','https://checkip.amazonaws.com']
        retries = 0
        limit = 10
        for url in itertools.cycle(urls):
            try:
                response = requests.get(url, proxies=self.proxies)
            except requests.exceptions.ConnectionError:
                if retries == limit:
                    break
                print("Connection Error when changing IP. Trying with another URL")
                retries += 1
                time.sleep(5)
                continue
            if response.status_code == 200:
                ip = response.text.strip()
                return ip
            else:
                continue
        raise ValueError("Proxy change could not be verified")  
    

    def get_ip(self, override=False) -> str:
        """Fetches the IP Address of the machine
        Raises:
            ValueError: If the IP Address couldn't be fetched

        Returns:
            str: An IP Address
        """
        urls = ['https://ident.me','http://myip.dnsomatic.com','https://checkip.amazonaws.com']
        retries = 0
        limit = 50
        for url in itertools.cycle(urls):
            try:
                response = requests.get(url, proxies=self.proxies)
            except requests.exceptions.ConnectionError:
                if retries == limit:
                    break
                print("Connection Error when changing IP. Trying with another URL")
                retries += 1
                time.sleep(5)
                continue
            if response.status_code == 200:
                ip = response.text.strip()
                return ip
            else:
                continue
        raise ValueError("Couldn't get the external IP Address. Please Check the URLs") 

    def change_identity(self):
        """Method which will change both the IP address (in case using proxy) as well as the user agent
        """

        if self.use_proxy == True:
            self.switch_proxy()
        new_userAgent = random.choice(self.user_agent_choices)
        while new_userAgent == self.user_agent:
            new_userAgent = random.choice(self.proxy_list)
        
        self.user_agent = new_userAgent
    
    def test_proxy_url(self,url,proxy):
        self.proxies = {
            'http': f'{proxy}',
            'https': f'{proxy}',
        }
        print(self.get_ip())
        self.get(url)

    def get_current_proxy(self):
        return self.proxies['http']
    
if __name__ == '__main__':
    proxy = Proxy()
    # print(proxy.get_proxy_list())
    print(proxy.get_ip())
    print(proxy.get("https://www.amazon.in/KECHAODA-K9-Kechaoda-Red-Black/dp/B00IP9974I/ref=sr_1_141?dchild=1&keywords=smartphone&m=A14CZOWI0VEHLG&qid=1627466863&refinements=p_72%3A1318478031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031&rnid=3837712031&s=electronics&sr=1-141"))

    # # Failure test case
    # print(proxy.get("https://www.apple.com/%"))
    # print(proxy.get_ip())

    #print(proxy.test_proxy_url('https://www.amazon.in/KECHAODA-K9-Kechaoda-Red-Black/dp/B00IP9974I/ref=sr_1_141?dchild=1&keywords=smartphone&m=A14CZOWI0VEHLG&qid=1627466863&refinements=p_72%3A1318478031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031&rnid=3837712031&s=electronics&sr=1-141','185.222.172.4'))
