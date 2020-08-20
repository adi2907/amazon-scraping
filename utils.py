import logging
import logging.handlers
import os
import sys
import types
from string import Template

url_template = Template('https://www.amazon.in/s?k=$category&ref=nb_sb_noss_2')

customer_reviews_template = Template('https://www.amazon.in/review/widgets/average-customer-review/popover/ref=acr_search__popover?ie=UTF8&asin=$PID&ref=acr_search__popover&contextId=search')

qanda_template = Template('https://www.amazon.in/ask/questions/asin/$PID/$PAGE/ref=ask_dp_iaw_ql_hza')


# List of all the necessary listing page URLS (after applying filters)
old_listing_templates = [
    Template('https://www.amazon.in/s?k=smartphones&i=electronics&rh=n%3A1805560031%2Cp_89%3AApple%7CNokia%7COnePlus%7COppo%7CPanasonic%7CRedmi%7CSamsung%7CVivo%2Cp_6%3AA14CZOWI0VEHLG&dc&page=$PAGE_NUM&qid=1597662424&rnid=1318474031&ref=sr_pg_$PAGE_NUM'),
    Template('https://www.amazon.in/s?k=headphones&i=electronics&rh=n%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_72%3A1318478031&dc&page=$PAGE_NUM&qid=1597664105&rnid=1318475031&ref=sr_pg_$PAGE_NUM'),
    Template('https://www.amazon.in/s?k=ceiling+fan&i=kitchen&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_6%3AAT95IG9ONZD7S%2Cp_72%3A1318478031&dc&page=$PAGE_NUM&crid=1TGIH58I2LW9I&qid=1597664762&rnid=1318475031&sprefix=ceili%2Caps%2C380&ref=sr_pg_$PAGE_NUM'),
    Template('https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S&dc&page=$PAGE_NUM&qid=1597665403&rnid=1318474031&ref=sr_pg_$PAGE_NUM'),
    Template('https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380263031%2Cn%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S&dc&page=$PAGE_NUM&qid=1597665517&rnid=1318474031&ref=sr_pg_$PAGE_NUM'),
]

listing_templates = [
    Template('https://www.amazon.in/s?k=headphones&i=electronics&rh=n%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_72%3A1318478031&dc&page=$PAGE_NUM&qid=1597664105&rnid=1318475031&ref=sr_pg_$PAGE_NUM'),
]

#listing_categories = ['smartphones', 'headphones', 'ceiling fan', 'refrigerator', 'washing machine']
listing_categories = ['headphones']


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
    handler = logging.handlers.RotatingFileHandler(filename=app_logfile, mode='a', maxBytes=5000, backupCount=5)
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
