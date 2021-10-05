import parse_data
import time
import requests
from bs4 import BeautifulSoup
import db_manager
import proxy
from utils import (category_to_domain, create_logger,
                   customer_reviews_template, domain_map, domain_to_db,
                   listing_categories, listing_templates, qanda_template,
                   subcategory_map, url_template)
from decouple import UndefinedValueError, config
from sqlalchemy.orm import sessionmaker

headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

# Start the requests session
session = requests.Session()

# Use a proxy if possible
my_proxy = proxy.Proxy(OS='Linux', use_proxy=True) 
# Database Session setup
try:
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_PORT = config('DB_PORT')
    DB_NAME = config('DB_NAME')
    DB_SERVER = config('DB_SERVER')
    DB_TYPE = config('DB_TYPE')
    engine = db_manager.Database(dbtype=DB_TYPE, username=DB_USER, password=DB_PASSWORD, port=DB_PORT, dbname=DB_NAME, server=DB_SERVER).db_engine
except UndefinedValueError:
    DB_TYPE = 'sqlite'
    engine = db_manager.Database(dbtype=DB_TYPE).db_engine
    
Session = sessionmaker(bind=engine)

db_session = Session()