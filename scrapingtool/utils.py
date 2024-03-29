import logging
import logging.handlers
import os
import sys
import json
import types
from string import Template

try:
    import dramatiq
    from taskqueue.broker import Broker
    from decouple import config
except:
    from decouple import config

url_template = Template('https://www.amazon.in/s?k=$category&ref=nb_sb_noss_2')

customer_reviews_template = Template('https://www.amazon.in/review/widgets/average-customer-review/popover/ref=acr_search__popover?ie=UTF8&asin=$PID&ref=acr_search__popover&contextId=search')

qanda_template = Template('https://www.amazon.in/ask/questions/asin/$PID/$PAGE/ref=ask_dp_iaw_ql_hza?sort=$SORT_TYPE')

# Not needed anymore
listing_templates = {
            "smartphones": Template("https://www.amazon.in/s?k=smartphone&i=electronics&rh=n%3A1805560031%2Cp_72%3A1318478031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_36%3A400000-&dc&page=$PAGE_NUM&qid=1630905465&rnid=1318502031&ref=sr_pg_$PAGE_NUM"),
            "headphones": Template("https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031&dc&page=$PAGE_NUM&fst=as%3Aoff&qid=1630905511&rnid=3837712031&ref=sr_pg_$PAGE_NUM"),
            "ceiling fan": Template("https://www.amazon.in/s?k=ceiling+fan&i=kitchen&bbn=4369221031&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_6%3AA1X5VLS1GXL2LN%7CAT95IG9ONZD7S%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031&dc&page=$PAGE_NUM&crid=1TGIH58I2LW9I&qid=1630905549&rnid=1318474031&sprefix=ceili%2Caps%2C380&ref=sr_pg_$PAGE_NUM"),
            "refrigerator": Template("https://www.amazon.in/s?k=refrigerator&i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S&dc&page=$PAGE_NUM&qid=1630905592&rnid=1318474031&ref=sr_pg_$PAGE_NUM"),
            "washing machine": Template("https://www.amazon.in/s?k=washing+machine&i=kitchen&bbn=1380369031&rh=n%3A1380263031%2Cn%3A1380369031%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_6%3AA3VI3FOOSYHJSV%7CAT95IG9ONZD7S&dc&page=$PAGE_NUM&qid=1630905632&rnid=1318474031&ref=sr_pg_$PAGE_NUM"),
            "hair color": Template("https://www.amazon.in/s?k=hair+color&i=beauty&rh=n%3A1355016031%2Cn%3A1374305031%2Cn%3A1374336031%2Cn%3A1374309031%2Cp_6%3AAT95IG9ONZD7S%2Cp_72%3A1318477031&dc&page=$PAGE_NUM&qid=1630905683&rnid=1318475031&ref=sr_pg_$PAGE_NUM")
        }

old_listing_templates = [
    Template('https://www.amazon.in/s?k=headphones&i=electronics&rh=n%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_72%3A1318478031&dc&page=$PAGE_NUM&qid=1597664105&rnid=1318475031&ref=sr_pg_$PAGE_NUM'),
]

#Todo: Find a way to figure out the root path
#root_dir = os.path.join(os.path.abspath(".."))
root_dir = os.getcwd()
#root_dir = os.path.dirname(os.path.abspath(__file__))
print(root_dir)
# If categories.json exists in currnet or parent directory
if not os.path.exists(os.path.join(root_dir,'categories.json')):
    raise ValueError(f"categories.json file not found")


# All the categories to be scraped (Listing + Details)
with open(os.path.join(root_dir, 'categories.json'), 'r') as f:
    json_info = json.load(f)

if 'categories' not in json_info or 'domains' not in json_info:
    raise ValueError(f"categories field and domains field must be present in categories.json")

listing_categories = []
category_to_domain = {}
domain_map = {}
domain_to_db = {}

for domain in json_info['categories']:
    domain_map[domain] = {}
    for category in json_info['categories'][domain]:
        listing_categories.append(category)
        category_to_domain[category] = domain
        domain_map[domain][category] = json_info['categories'][domain][category]

for domain in json_info['domains']:
    domain_to_db[domain] = json_info['domains'][domain]

# Not needed anymore
subcategory_map = {
    'headphones': {
    'wired': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_n_feature_six_browse-bin%3A15564046031&dc&fst=as%3Aoff&qid=1599294897&rnid=15564019031&ref=sr_nr_p_n_feature_six_browse-bin_1',
    'wireless': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_n_feature_six_browse-bin%3A15564047031%7C15564048031&dc&fst=as%3Aoff&qid=1599295118&rnid=15564019031&ref=sr_nr_p_n_feature_six_browse-bin_1',
    'tws': '',
    'price': ['<500', '500-1000', '1000-2000', '2000-3000', '3000-5000', '>5000']
    }
}

def setup_broker():
    try:
        broker_type = config('BROKER_TYPE')
        connection_params = {'host': config('REDIS_SERVER_HOST'), 'port': config('REDIS_SERVER_PORT'), 'db': config('REDIS_SERVER_DATABASE'), 'password': config('REDIS_SERVER_PASSWORD')}
    except:
        broker_type = None
        connection_params = {}

    broker = Broker(broker_type=broker_type, connection_params=connection_params)
    return broker


def is_lambda(v):
  LAMBDA = lambda:0
  return isinstance(v, type(LAMBDA)) and v.__name__ == LAMBDA.__name__


def to_http(url, use_tor=False):
    if use_tor == True:
        return url
    if url.startswith("https://"):
        url = url.replace("https://", "http://")
    return url


def add_newlines(self: logging.Logger, num_newlines=1) -> None:
    """Add newlines to a logger object

    Args:
        num_newlines (int, optional): Number of new lines. Defaults to 1.
    """
    self.removeHandler(self.base_handler)
    self.addHandler(self.newline_handler)

    # Main code comes here
    for _ in range(num_newlines):
        self.info('')

    self.removeHandler(self.newline_handler)
    self.addHandler(self.base_handler)


def create_logger(app_name: str) -> logging.Logger:
    """Creates the logger for the current application

    Args:
        app_name (str): The name of the application

    Returns:
        logging.Logger: A logger object for that application
    """
    if not os.path.exists(os.path.join(os.getcwd(), 'logs')):
        os.mkdir(os.path.join(os.getcwd(), 'logs'))

    app_logfile = os.path.join(os.getcwd(), 'logs', f'{app_name}.log')

    logger = logging.getLogger(f"{app_name}-logger")
    logger.setLevel(logging.DEBUG)

    # handler = logging.FileHandler(filename=app_logfile, mode='a')
    handler = logging.handlers.RotatingFileHandler(filename=app_logfile, mode='a', maxBytes=20000, backupCount=10)
    handler.setLevel(logging.DEBUG)

    # Set the formatter
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    # Set it as the base handler
    logger.base_handler = handler

    # Also add a newline handler to switch to later
    newline_handler = logging.FileHandler(filename=app_logfile, mode='a')
    newline_handler.setLevel(logging.DEBUG)
    newline_handler.setFormatter(logging.Formatter(fmt='')) # Must be an empty format
    
    logger.newline_handler = newline_handler

    # Also add the provision for a newline handler using a custom method attribute
    logger.newline = types.MethodType(add_newlines, logger)

    # Also add a StreamHandler for printing to stderr
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)

    return logger


# Create the logger for the app
logger = create_logger(__name__)


# We can log any unhandled exceptions using the logger!
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    logger.newline()

# This will capture any uncaught exception
sys.excepthook = handle_exception
