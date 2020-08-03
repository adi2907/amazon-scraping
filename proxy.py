import random
import socket
import time
from collections import OrderedDict
from functools import wraps
from string import Template

import requests
import socks
from bs4 import BeautifulSoup
from decouple import UndefinedValueError, config
from stem import Signal
from stem.control import Controller

from utils import create_logger, customer_reviews_template, url_template

logger = create_logger(__name__)

try:
    TOR_PASSWORD = config('TOR_PASSWORD')
except UndefinedValueError:
    TOR_PASSWORD = None


control_port = 9051
BACKOFF_DURATION = 20


class Retry():
    """A class which will ensure that falied requests are send again
    """
    @classmethod
    def retry(cls, predicate, deadline:int =None):
        """This decorator method uses exponential backoff to re-send requests

        Args:
            predicate: A predicate, which will ensure that backoff is done only after we catch specific exception(s)
            deadline (int, optional): [description]. Defaults to None. This is the maximum backoff limit.

        Raises:
            TimeoutError: When the backoff exceeds the limit
        """
        if deadline is None:
            deadline = BACKOFF_DURATION
        
        @wraps(cls)
        def wrapper1(func):
            @wraps(func)
            def wrapper2(self, *args, **kwargs):
                for _ in range(20):
                    try:
                        return func(self, *args, **kwargs)
                    except Exception as ex:
                        if (predicate(ex) == True):
                            logger.error(
                                "Exception: occured in {}. Now trying exponential backoff. Current backoff = {}".format(func.__qualname__, self.backoff),
                                exc_info=ex,
                            )
                            logger.newline()
                            
                            # Now backoff and try again
                            self.backoff = 2*self.backoff
                            if self.backoff > deadline:
                                raise TimeoutError("Maximum Backoff Exceeded")
                            self.delay = self.backoff
                            self.penalty = max(2, self.penalty+1)
                        else:
                            raise
                raise TimeoutError("Maximum Loop Limit Exceeded during backoff")
            return wrapper2
        return wrapper1

    @classmethod
    def if_exception_type(cls, *exception_types):
        def if_exception_type_predicate(exception):
            """Bound predicate for checking an exception type."""
            return isinstance(exception, exception_types)
        return if_exception_type_predicate


class Proxy():
    """Our own Proxy Class which will use Tor relays to keep shifting between IP addresses
    """

    @staticmethod
    def generate_count(start=2, end=6):
        count = random.randint(start, end)
        return count
    

    def __init__(self, proxy_port=9050, control_port=9051, OS='Windows'):
        self.proxy_port = proxy_port
        self.control_port = control_port
        self.proxies = {
            'http': f'socks5h://127.0.0.1:{self.proxy_port}',
            'https': f'socks5h://127.0.0.1:{self.proxy_port}',
        }
        if OS == 'Windows':
            self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0"
        else:
            # Linux
            self.user_agent = "Mozilla/5.0 (X11; Linux i686; rv:78.0) Gecko/20100101 Firefox/78.0"
        
        self.user_agent_choices = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/80.0",
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36",
            "Mozilla/5.0 (X11; Linux i686; rv:78.0) Gecko/20100101 Firefox/78.0",
            "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15",
        ]
        self.reset()
        self.reference_count = random.randint(2, 4)
    

    def reset(self):
        """Resets the state of the proxy
        """
        self.session = requests.Session()
        self.cookies = dict()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.user_agent = random.choice(self.user_agent_choices)
        self.delay = 0
        self.penalty = 0
        self.ip_address = None
        self.max_retries = 3
        self.reference_count = self.generate_count()
        self.delay = 0
        self.penalty = 0
        self.backoff = 1
        self._BACKOFF_DURATION = 20
    

    def get_ip(self) -> str:
        """Fetches the IP Address of the machine

        Raises:
            ValueError: If the IP Address couldn't be fetched

        Returns:
            str: An IP Address
        """
        urls = ["https://ident.me", "http://myip.dnsomatic.com", "https://checkip.amazonaws.com"]
        for url in urls:
            response = requests.get(url, proxies=self.proxies)
            if response.status_code == 200:
                ip = response.text.strip()
                return ip
            else:
                continue
        raise ValueError("Couldn't get the external IP Address. Please Check the URLs")


    def change_identity(self):
        """Method which will change both the IP address as well as the user agent
        """
        # Reset the state of the proxy
        self.reset()
        
        curr = 0
        while curr <= self.max_retries:
            # Now change the IP via the Tor Relay Controller
            with Controller.from_port(port = self.control_port) as controller:
                controller.authenticate(password = TOR_PASSWORD)
                controller.signal(Signal.NEWNYM) # type: ignore
            
            # Now let's find out the new IP, if this worked correctly
            ip = self.get_ip()
            
            if ip is not None:
                if self.ip_address is None:
                    self.ip_address = ip
                    logger.info(f"IP Address is: {self.ip_address}")
                    break
                else:
                    # Let's compare
                    if ip != self.ip_address:
                        self.ip_address = ip
                        logger.info(f"New IP Address is: {self.ip_address}")
                        break
                    else:
                        curr += 1
                        if curr < self.max_retries:
                            logger.warning("Error during changing the IP Address. Retrying...")
                            time.sleep(5)
                        else:
                            raise TimeoutError("Maximum Retries Exceeded. Couldn't change the IP Address")
            else:
                curr += 1
                if curr < self.max_retries:
                    logger.warning("Error during changing the IP Address. Retrying...")
                    time.sleep(5)
                else:
                    raise TimeoutError("Maximum Retries Exceeded. Couldn't change the IP Address")
            


    def make_request(self, request_type, url, **kwargs):
        """Performs a request using a session object. This handles cookies, along with referral urls.
        """
        if 'proxies' not in kwargs:
            if 'no_proxy' not in kwargs or kwargs['no_proxy'] == True:
                pass
            else:
                kwargs['proxies'] = self.proxies
        
        if 'headers' not in kwargs or 'User-Agent' not in kwargs['headers']:
            # Provide a random user agent
            if url.startswith('https://amazon'):
                # Amazon specific headers
                headers = {"Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Connection":"close", "DNT": "1", "Upgrade-Insecure-Requests":"1", "User-Agent": self.user_agent}
                headers = OrderedDict(headers)
            else:
                headers = {"User-Agent": self.user_agent, "Accept-Encoding":"gzip, deflate"}
                headers = OrderedDict(headers)
            kwargs['headers'] = headers
        
        if 'referer' in kwargs:
            kwargs['headers']['referer'] = kwargs['referer']
            del kwargs['referer']

        if 'post_headers' in kwargs:
            if request_type == 'post':
                kwargs['headers'] = OrderedDict({**(kwargs['headers']), **(kwargs['post_headers'])})
            del kwargs['post_headers']
        
        # Some requests may not need cookies at all (For example, the first request)
        if 'cookies' not in kwargs and ('no_cookies' not in kwargs or kwargs['no_cookies'] == False):
            # Don't EVER send empty cookies unless explicitly mentioned
            if 'empty_cookies' not in kwargs and self.cookies != {}:
                kwargs['cookies'] = self.cookies
            else:
                if 'empty_cookies' in kwargs and kwargs['empty_cookies'] == True:
                    if self.cookies == {}:
                        kwargs['cookies'] = self.cookies
                    del kwargs['empty_cookies']
        
        if 'no_cookies' in kwargs:
            del kwargs['no_cookies']
        
        if 'ref_count' in kwargs:
            if kwargs['ref_count'] == 'constant':
                # We won't change reference_count
                const = True
            else:
                const = False
            del kwargs['ref_count']
        else:
            const = False
        
        if 'product_url' in kwargs:
            product_url = kwargs['product_url']
            del kwargs['product_url']
        else:
            product_url = None

        # Now make the request and decrement the reference count
        if hasattr(requests, request_type):
            response = getattr(self.session, request_type)(url, **kwargs)
            if hasattr(response, 'cookies'):
                self.cookies = {**(self.cookies), **dict(response.cookies)}
            
            if const == False:
                self.reference_count -= 1
                if self.reference_count <= 0:
                    # Change the identity and set it again
                    time.sleep(random.randint(3, 5) + self.delay + random.uniform(0, 2))

                    if hasattr(self, 'category') and url.startswith('https://amazon'):
                        self.goto_product_listing(getattr(self, 'category'), product_url=product_url)
                    else:
                        self.change_identity()
                        self.reference_count = max(2, self.generate_count(2, 6) - self.penalty)
            else:
                # Keep ref count constant
                pass

            return response
        else:
            raise ValueError(f"Invalid Request Type: {request_type}")
    
    
    # Reference Material: https://cloud.google.com/iot/docs/how-tos/exponential-backoff
    @Retry.retry(predicate=Retry.if_exception_type(AssertionError), deadline=BACKOFF_DURATION)
    def get(self, url, **kwargs):
        return self.make_request('get', url, **kwargs)
    

    def post(self, url, **kwargs):
        return self.make_request('post', url, **kwargs)

    
    def goto_product_listing(self, category, product_url=None):
        self.change_identity()
        self.reference_count = self.generate_count(2, 6)

        server_url = 'https://amazon.in'

        # Increase ref count before request. Don't want to keep looping!
        self.reference_count += 1
        response = self.get(server_url)
        assert response.status_code == 200

        time.sleep(random.randint(4, 7) + self.delay + random.uniform(0, 1))

        listing_url = url_template.substitute(category=category)
        
        # Increase ref count before request. Don't want to keep looping!
        self.reference_count += 1
        response = self.get(listing_url, referer=server_url)
        assert response.status_code == 200
        
        time.sleep(random.randint(3, 6) + self.delay + random.uniform(0, 1))

        if product_url is not None:
            # Go to the product detail page
            self.reference_count += 3
            response = self.get(server_url + product_url, referer=listing_url)
            assert response.status_code == 200

            time.sleep(random.randint(2, 5) + self.delay + random.uniform(0, 1))



def test_proxy(proxy: Proxy, change: bool = False) -> None:
    """A method which tests if the proxy service using Tor is working
    """

    response = proxy.make_request('get', 'https://check.torproject.org')

    html = response.content
    soup = BeautifulSoup(html, 'html.parser')
    
    status = soup('title')[0].get_text().strip()
    assert 'Congratulations.' == status.split()[0]

    ip_text = soup.find("div", class_ = 'content').p.text.strip()
    old_ip_address = ip_text.split()[-1]
    
    logger.warning(f"Old (Current) IP: {old_ip_address}")

    if change == True:
        proxy.change_identity()

        response = proxy.make_request('get', 'https://check.torproject.org')

        html = response.content
        soup = BeautifulSoup(html, 'html.parser')
        status = soup('title')[0].get_text().strip()
        assert 'Congratulations.' == status.split()[0]

        ip_text = soup.find("div", class_ = 'content').p.text.strip()
        new_ip_address = ip_text.split()[-1]

        assert old_ip_address != new_ip_address

        logger.warning(f"New (Current) IP: {new_ip_address}")


if __name__ == '__main__':
    proxy = Proxy(proxy_port=9050, control_port=9051)
    print(proxy.get_ip())
    #proxy.change_identity()
    #print(proxy.get_ip())
