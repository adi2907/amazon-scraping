# Represents all the Models used to create our scraper
import argparse
import datetime
import glob
import json
import os
import pickle
import re
from contextlib import contextmanager
from unicodedata import normalize
from bs4 import BeautifulSoup
import parse_data
import requests
import time


import pymysql
from decouple import UndefinedValueError, config
from pytz import timezone
from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKey, Integer,
                        MetaData, String, Table, Text, desc,create_engine, exc,func,and_,or_)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, relationship, sessionmaker
from sqlalchemy.orm.exc import FlushError, NoResultFound
from sqlalchemy.sql.expression import except_all, null
from sqlitedict import SqliteDict

import tokenize_titles
from subcategories import subcategory_dict
from utils import create_logger, subcategory_map, listing_categories
from sp_api.base import SellingApiException,Marketplaces
from sp_api.base.reportTypes import ReportType
from sp_api.api import Catalog




# This is required for integration with MySQL and Python
pymysql.install_as_MySQLdb()

# Create the logger
logger = create_logger(__name__)

productlisting_logger = create_logger('productlisting')
productdetails_logger = create_logger('productdetails')
qanda_logger = create_logger('qanda')
reviews_logger = create_logger('reviews')

# Our Database Schema
tables = {
    'ProductListing': {
        'product_id': 'TEXT(16) PRIMARY KEY',
        'category': 'TEXT(100)',
        'title': 'LONGTEXT',
        'domain': 'TEXT(60)',
        'product_url': 'LONGTEXT',
        'avg_rating': 'FLOAT',
        'total_ratings': 'INTEGER',
        'price': 'FLOAT',
        'old_price': 'FLOAT',
        'secondary_information': 'LONGTEXT',
        'image': 'TEXT(1000)',
        'short_title': 'TEXT(100)',
        'duplicate_set': 'TEXT(100)',
        'date_completed': 'DATETIME',
        'brand': 'TEXT(100)',
        'model': 'TEXT(100)',
        'detail_completed': 'DATETIME',
        },
    'ProductDetails': {
        'product_id': 'TEXT(16) PRIMARY KEY',
        'product_title': 'LONGTEXT',
        'byline_info': 'LONGTEXT',
        'num_reviews': 'INTEGER',
        'answered_questions': 'TEXT(100)',
        'curr_price': 'FLOAT',
        'features': 'LONGTEXT',
        'offers': 'LONGTEXT',
        'description': 'LONGTEXT',
        'product_details': 'LONGTEXT',
        'featurewise_reviews': 'LONGTEXT',
        'customer_qa': 'LONGTEXT',
        'histogram': 'LONGTEXT',
        'reviews_url': 'LONGTEXT',
        'created_on': 'DATETIME',
        'subcategories': 'LONGTEXT',
        'brand': 'TEXT(100)',
        'model': 'TEXT(100)',
        'date_completed': 'DATETIME',
        'duplicate_set': 'TEXT(100)',
        'product_overview': 'LONGTEXT',
        'related_products':'LONGTEXT',
    },
    'SponsoredProductDetails': {
        'product_id': 'TEXT(16) PRIMARY KEY',
        'product_title': 'LONGTEXT',
        'byline_info': 'LONGTEXT',
        'num_reviews': 'INTEGER',
        'answered_questions': 'TEXT(100)',
        'curr_price': 'FLOAT',
        'features': 'LONGTEXT',
        'offers': 'LONGTEXT',
        'description': 'LONGTEXT',
        'product_details': 'LONGTEXT',
        'featurewise_reviews': 'LONGTEXT',
        'customer_qa': 'LONGTEXT',
        'customer_lazy': 'INTEGER',
        'histogram': 'LONGTEXT',
        'reviews_url': 'LONGTEXT',
        'created_on': 'DATETIME',
        'subcategories': 'LONGTEXT',
        'is_sponsored': 'BOOLEAN',
    },
    'QandA': {
        'id': 'INTEGER PRIMARY KEY',
        'product_id': 'TEXT(16)',
        'question': 'LONGTEXT',
        'answer': 'LONGTEXT',
        'date': 'DATETIME',
        'page_num': 'INTEGER',
        'is_duplicate': 'BOOLEAN',
        'duplicate_set': 'INTEGER',
        '_product_id': ['FOREIGN KEY', 'REFERENCES ProductListing (product_id)'],
    },
    'Reviews': {
        'id': 'INTEGER PRIMARY KEY',
        'product_id': 'TEXT(16)',
        'rating': 'FLOAT',
        'review_date': 'DATETIME',
        'country': 'TEXT(40)',
        'title': 'TEXT(1000)',
        'body': 'LONGTEXT',
        'product_info': 'LONGTEXT',
        'verified_purchase': 'INTEGER',
        'helpful_votes': 'INTEGER',
        'page_num': 'INTEGER',
        'is_duplicate': 'BOOLEAN',
        'duplicate_set': 'INTEGER',
        '_product_id': ['FOREIGN KEY', 'REFERENCES ProductListing (product_id)'],
    },
    'DailyProductListing': {
        'id': 'INTEGER PRIMARY KEY',
        'product_id': 'TEXT(16)',
        'category': 'TEXT(100)',
        'serial_no': 'INTEGER',
        'avg_rating': 'FLOAT',
        'total_ratings': 'INTEGER',
        'price': 'FLOAT',
        'old_price': 'FLOAT',
        'date': 'DATETIME',
    },
    'SentimentAnalysis': {
        'id': 'INTEGER PRIMARY KEY',
        'product_id': 'TEXT(16)',
        'positive_sentiments': 'LONGTEXT',
        'negative_sentiments': 'LONGTEXT',
        '_product_id': ['FOREIGN KEY', 'REFERENCES ProductListing (product_id)'],
    },
    'SentimentBreakdown': {
        'product_id': 'TEXT(16) PRIMARY KEY',
        'sentiments': 'LONGTEXT',
    },
}

field_map = {
    'INTEGER': Integer,
    'TEXT': String,
    'FLOAT': Float,
    'DATETIME': DateTime,
    'BOOLEAN': Boolean,
    'LONGTEXT': Text,
}


class Database():
    db_file = 'db.sqlite'
    DB_ENGINE = {
        'sqlite': f'sqlite:///{db_file}',
    }

    # Main DB Connection Ref Obj
    db_engine = None
    def __init__(self, dbtype='sqlite', username='', password='', port=3306, dbname='', server=''):
        dbtype = dbtype.lower()
        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=dbname)
            self.db_engine = create_engine(engine_url)
        elif dbtype.startswith('mysql'):
            # mysql+pymysql also supported
            engine_url = f'{dbtype}://{username}:{password}@{server}:{port}/{dbname}'
            self.db_engine = create_engine(engine_url)
            self.db_engine.connect()
            self.db_engine.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
            self.db_engine.execute(f"USE {dbname}")
        else:
            raise ValueError("DBType is not found in DB_ENGINE")


# Database Session setup
try:
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_NAME = config('DB_NAME')
    DB_SERVER = config('DB_SERVER')
    DB_TYPE = config('DB_TYPE')
    engine = Database(dbtype=DB_TYPE, username=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, server=DB_SERVER).db_engine
except UndefinedValueError:
    DB_TYPE = 'sqlite'
    engine = Database(dbtype=DB_TYPE).db_engine

# And the metadata
metadata = MetaData(bind=engine)


try:
    DEVELOPMENT = config('DEVELOPMENT', cast=bool)
except:
    DEVELOPMENT = False


def get_credentials():
    from decouple import config
    try:
        if config('DB_TYPE') == 'sqlite':
            connection_params = {
                'dbtype': config('DB_TYPE'),
            }
            return connection_params
    except:
        pass

    try:
        connection_params = {
            'dbtype': config('DB_TYPE'),
            'username': config('DB_USER'),
            'password': config('DB_PASSWORD'),
            'server': config('DB_SERVER'),
        }
     
    except:
        connection_params = None
    return connection_params


def connect_to_db(db_name, connection_params):
    engine = Database(dbname=db_name, **(connection_params)).db_engine
    SessionFactory = sessionmaker(bind=engine, autocommit=False, autoflush=True)
    return engine, SessionFactory


@contextmanager
def session_scope(sessionmaker=None):
    if sessionmaker is None:
        SessionFactory = sessionmaker(bind=engine, autocommit=False, autoflush=True)
        session = SessionFactory()
    else:
        session = sessionmaker()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def apply_schema(cls):
    # Refer https://stackoverflow.com/a/2575016
    table = tables[cls.__name__]
    columns = []

    _fk_field = {} # Assume one FK per table
    for field in table:
        if isinstance(table[field], list) and 'FOREIGN KEY' in table[field]:
            _fk_field = {'field': field[1:], 'parent': table[field][1].split()[1], 'fk_field': table[field][1].split()[2][1:-1]}

    for field in table:
        is_foreign_key = False

        if 'field' in _fk_field and field == _fk_field['field']:
            is_foreign_key = True
            parent = _fk_field['parent']
            fk_field = _fk_field['fk_field']

        if isinstance(table[field], list) and 'FOREIGN KEY' in table[field]:
            continue

        datatype = table[field].split()[0]
        
        pattern = r'(.+)\ *\(([0-9]+)\)$'
        match = re.match(pattern, datatype)
        
        if match is not None:
            datatype = match.groups()[0]
            size = int(match.groups()[1])
        else:
            size = None

        datatype = field_map[datatype]

        if datatype == String:
            if size is not None:
                datatype = String(size)
            else:
                datatype = String(None)
        elif datatype == Text:
            if size is not None:
                datatype = Text(size)
            else:
                datatype = Text()
        
        args = [field, datatype]
        kwargs = dict()

        if 'PRIMARY KEY' in table[field]:
            kwargs['primary_key'] = True
        if is_foreign_key == True:
            # Set the relationship attribute on the parent class
            relation = relationship(cls.__name__)
            setattr(globals()[parent], cls.__name__.lower(), relation)

            fk_args = [parent + '.' + fk_field]
            fk = ForeignKey(*fk_args)
            args.append(fk)

        column = Column(*args, **kwargs)
        columns.append(column)

    table = Table(cls.__name__, metadata, *(column for column in columns))
    metadata.create_all()
    mapper(cls, table)
    return cls


@apply_schema
class ProductListing():
    pass


@apply_schema
class ProductDetails():
    pass


@apply_schema
class SponsoredProductDetails():
    pass


@apply_schema
class QandA():
    pass


@apply_schema
class Reviews():
    pass


@apply_schema
class DailyProductListing():
    pass


@apply_schema
class SentimentAnalysis():
    pass


@apply_schema
class SentimentBreakdown():
    pass


table_map = {
    'ProductListing': ProductListing,
    'ProductDetails': ProductDetails,
    'SponsoredProductDetails': SponsoredProductDetails,
    'QandA': QandA,
    'Reviews': Reviews,
    'DailyProductListing': DailyProductListing,
    'SentimentAnalysis': SentimentAnalysis,
    'SentimentBreakdown': SentimentBreakdown,
}

# Credentials for Amazon API
credentials=dict(
    refresh_token='Atzr|IwEBIMjdGchHxslnhs965zD6ZOprd5y48jqQW-_YryyVZGp7mvpJLJIEyH3d7vV3DYm-vFj5NzpqykvgmttDxhFZk336qOdZ-ADU8SmzlaJVbGv70Ks-8tvTr37sA7RZ46xHakDcf3zeMRj0BKb68YGRrziXvWpD2hdIiDLjeKXdRC9FM4502nO4sHNzTRj-rxEB1Br9GWyA2pelV4LgQ322AnLcV1DKx2OC0zF_WTMasQPq3VLRarl0-hx07BcBpRkKggH6pI0eFg3gvtzVvTr1GB_y0NUPEj73RbXyc43q5OuH39GiFtYEBP3zLCc6puzkJYc',
    lwa_app_id='amzn1.application-oa2-client.f995345f18ec49f79df4c4187d68ef23',
    lwa_client_secret='59c490020431165a15450dd3461bc057963c132f8613a699e927456ec37ac378',
    aws_secret_key='z1dBpqlEUoZmgSCpIQKgCGkzidBFhRCNkn8E1DTg',
    aws_access_key='AKIAV2GQNFTIXV2GKZH6',
    role_arn='arn:aws:iam::399869029585:role/sellerAPI',
)
    
def get_short_title(product_title):
    if product_title is None:
        return product_title
    
    if product_title.startswith('(Renewed)'):
        product_title = product_title[9:].strip()
    
    return tokenize_titles.remove_stop_words(product_title)


def insert_product_listing(session, data, table='ProductListing', domain='amazon.in'):
    """
    Insert data as a row into ProductListing table

    Args:
        data = Nested dictionary with listing details e.g. data[category][curr_page][title]['is_duplicate']
    """
    row = dict()
    row['domain'] = domain
    for category in data:
        row['category'] = category
        for page_num in data[category]:
            for title in data[category][page_num]:
                row['title'] = title
                try:
                    row['short_title'] = get_short_title(title)
                except:
                    row['short_title'] = None
                
                if row['short_title'] is None:
                    row['brand'] = None
                else:
                    try:
                        row['brand'] = row['short_title'].split()[0]
                    except:
                        row['brand'] = None
                
                value = data[category][page_num][title]
                for key in value:
                    if value[key] is not None:
                        if key == 'avg_rating':
                            row[key] = float(value[key].split()[0])
                        elif key == 'total_ratings':
                            row[key] = int(value[key].replace(',', '').replace('.', ''))
                        elif key in ('price', 'old_price'):
                            row[key] = float(value[key].replace(',', ''))
                        else:
                            row[key] = value[key]
                    else:
                        row[key] = value[key]
                try:
                    if row['product_id'] is not None:
                        obj = table_map[table]()
                        [setattr(obj, key, value) for key, value in row.items() if hasattr(obj, key)]
                        # Update the date
                        date = datetime.datetime.now(timezone('Asia/Kolkata'))#.date()
                        if hasattr(obj, 'date_completed'):
                            setattr(obj, 'date_completed', date)
                        session.add(obj)
                        session.commit()
                        continue
                except (exc.IntegrityError, FlushError,):
                    session.rollback()
                    result = session.query(table_map[table]).filter(ProductListing.product_id == row['product_id']).first()
                    if result is None:
                        continue
                    else:
                        update_fields = [field for field in tables[table] if field != "product_id"]
                        short_title = getattr(result, 'short_title')
                        for field in update_fields:
                            if field in row:
                                setattr(result, field, row[field])

                        # Update the date
                        date = datetime.datetime.now(timezone('Asia/Kolkata'))#.date()
                        if hasattr(result, 'date_completed'):
                            setattr(result, 'date_completed', date)
                        if short_title is not None:
                            setattr(result, 'short_title', short_title)
                        try:
                            session.commit()
                            continue
                        except:
                            session.rollback()
                            logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
                            logger.newline()
                            continue
                except Exception as ex:
                    session.rollback()
                    productlisting_logger.critical(f"{row['product_id']}-> Exception: {ex}")
                    logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
                    logger.newline()
                    continue
    return True


def insert_daily_product_listing(session, data, table='DailyProductListing'):
    """
    Insert data as a row into DailyProductListing table

    Args:
        data = Nested dictionary with listing details e.g. data[category][curr_page][title]['is_duplicate']
    """
    row = dict()
    for category in data:
        for page_num in data[category]:
            for title in data[category][page_num]:
                value = data[category][page_num][title]
                for key in value:
                    if value[key] is not None:
                        if key == 'avg_rating':
                            row[key] = float(value[key].split()[0])
                        elif key == 'total_ratings':
                            row[key] = int(value[key].replace(',', '').replace('.', ''))
                        elif key in ('price', 'old_price'):
                            row[key] = float(value[key].replace(',', ''))
                        else:
                            row[key] = value[key]
                    else:
                        row[key] = value[key]
                try:
                    if row['product_id'] is not None:
                        row['date'] = datetime.datetime.now(timezone('Asia/Kolkata'))#.date()
                        row['category'] = category
                        obj = table_map[table]()
                        [setattr(obj, key, value) for key, value in row.items() if hasattr(obj, key)]
                        session.add(obj)
                        session.commit()
                        continue
                except exc.IntegrityError:
                    session.rollback()
                    result = session.query(table_map[table]).filter(ProductListing.product_id == row['product_id']).first()
                    if result is None:
                        pass
                    else:
                        update_fields = [field for field in tables[table] if hasattr(result, field) and getattr(result, field) in (None, {}, [], "", "{}", "[]")]
                        for field in update_fields:
                            if field in row:
                                setattr(result, field, row[field])
                        # Update the date
                        date = datetime.datetime.now(timezone('Asia/Kolkata'))#.date()
                        setattr(result, 'date', date)
                        try:
                            session.commit()
                            continue
                        except:
                            session.rollback()
                            logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
                            logger.newline()
                            continue
                except Exception as ex:
                    session.rollback()
                    productlisting_logger.critical(f"{row['product_id']} -> Exception: {ex}")
                    logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
                    logger.newline()
                    continue
    return True


def insert_product_details(session, data, table='ProductDetails', is_sponsored=False):
    row = {key: (data[key] if not (isinstance(data[key], list) or isinstance(data[key], dict)) else json.dumps(data[key])) for key in data}
    for field in row:
        if row[field] is not None:
            if field == 'num_reviews':
                row[field] = int(row[field].split()[0].replace(',', '').replace('.', ''))
            elif field in ('curr_price'):
                row[field] = float(row[field].replace(',', ''))
    row['created_on'] = datetime.datetime.now()
    try:
        obj = table_map[table]()
        [setattr(obj, key, value) for key, value in row.items() if hasattr(obj, key)]
        session.add(obj)
        session.commit()
        return True
    except (exc.IntegrityError, FlushError):
        session.rollback()
        result = session.query(table_map[table]).filter(ProductDetails.product_id == row['product_id']).first()
        update_fields = (field for field in tables[table] if field in row and row[field] not in (None, {}, [], "", "{}", "[]"))
        for field in update_fields:
            if field in row:
                setattr(result, field, row[field])
        try:
            session.commit()
            return True
        except Exception as ex:
            session.rollback()
            productdetails_logger.critical(f"{row['product_id']} -> Exception: {ex}")
            logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
            logger.newline()
            return False
    except Exception as ex:
        session.rollback()
        productdetails_logger.critical(f"{row['product_id']} -> Exception: {ex}")
        logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
        logger.newline()
        return False


def insert_product_qanda(session, qandas, product_id, table='QandA', duplicate_set=None):
    for pair in qandas:
        row = {key: (value if not isinstance(value, list) and not isinstance(value, dict) else json.dumps(value)) for key, value in pair.items()}
        # Add product id
        row['product_id'] = product_id
        row['duplicate_set'] = duplicate_set
        row['is_duplicate'] = False
        obj = table_map[table]()
        [setattr(obj, key, val) for key, val in row.items()]
        session.add(obj)
    # TODO: Change this later outside the loop
    try:
        session.commit()
        return True
    except Exception as ex:
        session.rollback()
        qanda_logger.critical(f"{product_id} -> Exception: {ex}")
        logger.warning(f" For Product {product_id}, there is an error with the data.")
        logger.newline()
        return False


def insert_product_reviews(session, reviews, product_id, table='Reviews', duplicate_set=None):
    for review in reviews['reviews']:
        row = dict()
        # Add product id
        row['product_id'] = product_id
        row['rating'] = float(review['rating'].split()[0])
        row['review_date'] = review['review_date']
        row['country'] = review['country']
        row['title'] = review['title']
        row['body'] = review['body']
        if isinstance(review['product_info'], list) or isinstance(review['product_info'], dict):
            row['product_info'] = json.dumps(review['product_info'])
        else:
            row['product_info'] = json.dumps(review['product_info'])
        row['verified_purchase'] = review['verified_purchase']
        row['helpful_votes'] = review['helpful_votes']
        row['is_duplicate'] = False
        row['duplicate_set'] = duplicate_set
        obj = table_map[table]()
        [setattr(obj, key, val) for key, val in row.items()]
        session.add(obj)
    # TODO: Change this later outisde the loop
    try:
        session.commit()
        return True
    except Exception as ex:
        session.rollback()
        reviews_logger.critical(f"{product_id} -> Exception: {ex}")
        logger.warning(f"For Product {product_id}, there is an error with the data.")
        logger.newline()
        return False


def insert_sentiment_breakdown(db_name, counts=None, table='SentimentBreakdown', filename=None):
    if counts is None:
        if filename is None:
            raise ValueError(f"Need to provide filename for sentiment_counts_category.pkl")
        with open(filename, 'rb') as f:
            counts = pickle.load(f)
    
    credentials = get_credentials()
    _, Session = connect_to_db(db_name, credentials)
    
    with session_scope(Session) as session:
        for product_id in counts:
            sentiment_summary = counts[product_id]
            obj = table_map[table]()
            obj.product_id = product_id
            obj.sentiments = json.dumps(sentiment_summary)
            session.add(obj)
    
    logger.info(f"Inserted Sentiment Breakdown summary for all products!")


def insert_sentiment_reviews(db_name, db_df=None, table='SentimentAnalysis', filename=None):
    import pandas as pd

    if db_df is None:
        if filename is None:
            raise ValueError(f"Need to provide filename for sentiment_db_category.csv")
        db_df  = pd.read_csv(filename, sep=",", encoding="utf-8", header=0, usecols=["id", "product_id", "positive_sentiments", "negative_sentiments"])
    
    credentials = get_credentials()
    engine, _ = connect_to_db(db_name, credentials)

    db_df.to_sql(table, engine, method='multi', index=False, if_exists='append')
    
    logger.info(f"Inserted Sentiment analysis reviews for all products!")


def query_table(session, table, query='all', filter_cond=None):
    if query == 'all':
        if filter_cond is None:
            try:
                instance = session.query(table_map[table]).all()
                return instance
            except:
                return None
        # TODO: How does this work? Not used anywhere
        elif isinstance(filter_cond, list):
            # Filter IN
            filter_type = filter_cond[0]
            assert filter_type in ['in']
            try:
                if filter_type == 'in':
                    assert len(filter_cond) == 3
                    column = filter_cond[1] 
                    choices = filter_cond[2]
                    instance = session.query(table_map[table]).filter(getattr(table_map[table], column).in_(choices)).all()
                    return instance
            except:
                return None
        else:
            # Filter Condition MUST be a dict
            assert isinstance(filter_cond, dict)
            try:
                instance = session.query(table_map[table]).filter_by(**filter_cond).all()
                return instance
            except:
                return None
    
    elif query == 'one':
        if filter_cond is None:
            try:
                instance = session.query(table_map[table]).one()
                return instance
            except:
                return None
        else:
            # Filter Condition MUST be a dict
            assert isinstance(filter_cond, dict)
            try:
                instance = session.query(table_map[table]).filter_by(**filter_cond).one()
                return instance
            except:
                return None
    
    else:
        return None


def fetch_product_ids(session, table, categories):
    result = []
    if not isinstance(categories, list):
        categories = [categories]
    for category in categories:
        try:
            instances = session.query(table_map[table]).filter_by(category=category).all()
            result.extend([getattr(instance, 'product_id') for instance in instances])
        except:
            result.extend([])
    return result

# Fetch unscrapped product urls - based on category
def fetch_product_urls_unscrapped_details(session,category,table="ProductListing"):
    result = []
   
    if category is None:
        raise ValueError("Category can't be empty")
    try:
        tbl =  table_map[table]

        # Get max date for each duplicate_set
        maxdates = session.query(
            func.max(tbl.detail_completed),
            tbl.product_url,
            tbl.duplicate_set
        ).group_by(tbl.duplicate_set).filter(and_(ProductListing.category == category)).all()
        
        # Add all list entries unless the last scrapped date (detail_scrapped) is less than 1 week old       
        for maxdate in maxdates:
            if(isinstance(maxdate[0],datetime.datetime)):
                if ((datetime.datetime.today() - maxdate[0]).days<15):
                    continue
            result.append(maxdate[1]) #Domain + product_url
    except Exception as ex:
        logger.error("Exception in fetching product ids"+ex)
    print("# of ids to be scrapped are "+ str(len(result)))
    
    return result

def get_last_review_date(session,product_id,table="Reviews"):
    try:
        obj = session.query(table_map[table]).filter(Reviews.product_id == product_id).order_by(desc(Reviews.review_date)).first()
        if obj is not None:
            return obj.review_date
        else:
            return None
    except Exception as ex: # No row was found
        logger.error("Exception in getting review date "+ex)
        return None
    
def get_last_qanda_date(session,product_id,table="QandA"):
    try:
        obj = session.query(table_map[table]).filter(QandA.product_id == product_id).order_by(desc(QandA.date)).first()
        if obj is not None:
            return obj.date
        else:
            return None
    except Exception as ex:
        logger.error("Exception in getting QandA date "+ex) # No row was found
        return None
    
def get_detail_scrapped_date(session,product_id,table="ProductDetails"):
    try:
        obj = session.query(table_map[table]).filter(ProductDetails.product_id == product_id).one_or_none()
        if obj is not None:
            return obj.date_completed
        else:
            return None
    except Exception as ex:
        logger.error("Exception in getting Product Details date "+ex) 
        return None
    

def update_brands_and_models(session, table='ProductDetails'):
    instances = session.query(table_map[table]).all()
    for instance in instances:
        if instance.brand is not None and instance.model is not None:
            continue
        brand = None
        _model = None
        if instance.product_details not in (None, {}, "{}"):
            # Get the brand
            details = json.loads(instance.product_details)
            key = 'Technical Details' if 'Technical Details' in details else 'Product Details'
            if key in details:
                if 'Brand' in details[key]:
                    brand = details[key]['Brand']
                elif 'Brand Name' in details[key]:
                    brand = details[key]['Brand Name']
                elif 'Manufacturer' in details[key]:
                    brand = details[key]['Manufacturer']
                
                if 'Model' in details[key]:
                    _model = details[key]['Model']
                elif 'Item model name' in details[key]:
                    _model = details[key]['Item model name']
            else:
                # Get it from byline_info
                byline_info = json.loads(instance.byline_info)
                if byline_info not in (None, {}, "{}", "") and 'info' in byline_info:
                    brand = byline_info['info']
                    if brand.startswith("Visit the "):
                        brand = brand.replace("Visit the ", "")
                        if brand.strip()[-1] == 'store':
                            brand = brand.replace(' store', '')
        
        if brand is not None and _model is not None:
            # Update
            setattr(instance, 'brand', brand)
            setattr(instance, 'model', _model)
            try:
                session.commit()
            except Exception as ex:
                session.rollback()
                print(ex)


def assign_subcategories(session, category, table='ProductDetails'):

    DUMP_DIR = os.path.join(os.getcwd(), 'dumps')

    if not os.path.exists(DUMP_DIR):
        return

    def insert_subcategory(session, instance, subcategory, subcategory_type=None, subcategory_list=[]):
        if instance.subcategories in ([], None):
            instance.subcategories = json.dumps([subcategory])
        else:
            subcategories = json.loads(instance.subcategories)
            if subcategory_type == 'Price':
                for ncat in subcategory_list:
                    if ncat in subcategories and ncat != subcategory:
                        subcategories.remove(ncat)
            if subcategory in subcategories:
                return
            subcategories.append(subcategory)
            instance.subcategories = json.dumps(subcategories)
        
        try:
            session.commit()
            logger.info(f'Updated subcategories for {subcategory}')
        except Exception as ex:
            session.rollback()
            logger.critical(f"Exception during commiting: {ex}")

    
    def process_subcategory_html(subcategory, filename, subcategory_type=None, subcategory_list=[]):
        with open(filename, 'rb') as f:
            html = f.read()

        soup = BeautifulSoup(html, 'lxml')
        product_info, _ = parse_data.get_product_info(soup)

        for title in product_info:
            product_id = product_info[title]['product_id']
            if product_id is None:
                continue
            print(product_id, title)
            obj = query_table(session, 'ProductDetails', 'one', filter_cond=({'product_id': product_id}))
            if obj is not None:
                insert_subcategory(session, obj, subcategory, subcategory_type, subcategory_list)
        head, name = os.path.split(filename)
        os.rename(filename, os.path.join(DUMP_DIR, f"archived_{name}"))


    for category in subcategory_dict:
        queryset = session.query(ProductListing).filter(ProductListing.category == category)
        pids = dict()
        for obj in queryset:
            pids[obj.product_id] = obj.price
        
        for _subcategory in subcategory_dict[category]:
            for subcategory_name in subcategory_dict[category][_subcategory]:
                value = subcategory_dict[category][_subcategory][subcategory_name]
                
                # if subcategory is a url, parse the dumps files to assign subcategories
                if isinstance(value, str):
                    # Parse the html for the subcategory
                    files = glob.glob(f"{DUMP_DIR}/{category}_{subcategory_name}_*")
                    if _subcategory == 'Price':
                        subcategory_list = list(subcategory_dict[category][_subcategory].values())
                    else:
                        subcategory_list = []
                    for filename in files:
                        process_subcategory_html(subcategory_name, filename, subcategory_type=_subcategory, subcategory_list=subcategory_list)
                
                # if subcategory is a lambda
                elif isinstance(value, dict) and 'predicate' in value and 'field' in value:
                    # Use the predicate
                    field = value['field']
                    predicate = value['predicate']
                    for pid in pids:
                        instance = session.query(ProductDetails).filter(ProductDetails.product_id == pid).first()
                        if instance is None:
                            continue
                        instance_field = getattr(instance, field)
                        result = predicate(instance_field)
                        if result == True:
                            if _subcategory == 'Price':
                                subcategory_list = list(subcategory_dict[category][_subcategory].values())
                            else:
                                subcategory_list = []
                            insert_subcategory(session, instance, subcategory_name, subcategory_type=_subcategory, subcategory_list=subcategory_list)
                else:
                    # None
                    continue

def insert_short_titles(session):
    def foo(product_title):
        if product_title is None:
            return product_title
        
        if product_title.startswith('(Renewed)'):
            product_title = product_title[9:].strip()
        
        result = product_title.lower()
        # Order matters
        DELIMITERS = ['tws', 'true', 'wired', 'wireless', 'in-ear', 'in ear', 'on-ear', 'on ear'] + ['with', '[', '{', '(', ',']
        slen = len(result)
        fin = result
        temp = fin
        for delim in DELIMITERS:
            if result.startswith(delim):
                result = result[len(delim):].strip()
            bar = result.split(delim)
            if len(bar) == 1:
                # Empty
                continue
            short_title = bar[0].strip()
            
            if len(short_title) < slen:
                temp = fin
                fin = short_title.strip()
                slen = len(short_title)
        
        fin = fin.strip()
        if len(fin) == 0:
            print(f"For title {product_title}, len = 0")
        if len(fin.split()) <= 1:
            # Let's take the second shortest one instead, as fin is too short
            if len(temp.split()) <= 1:
                pass
            else:
                fin = temp.strip()
        if len(fin) > 0 and fin[-1] in [',', '.', ':', '-']:
            fin = fin[:-1]
        return fin
    
    queryset = session.query(ProductListing).all()

    for obj in queryset:
        if hasattr(obj, 'short_title'):
            setattr(obj, 'short_title', foo(obj.title))
    
    try:
        session.commit()
        logger.info("Updated short_title field!")
    except:
        session.rollback()
        logger.critical(f"Error during updating short_title field")


def index_qandas(engine, table='QandA'):
    engine.execute('UPDATE %s as t1 JOIN (SELECT product_id, duplicate_set FROM ProductListing) as t2 SET t1.duplicate_set = t2.duplicate_set WHERE t1.product_id = t2.product_id' % (table))

def index_reviews(engine, table='Reviews'):
    engine.execute('UPDATE %s as t1 JOIN (SELECT product_id, duplicate_set FROM ProductListing) as t2 SET t1.duplicate_set = t2.duplicate_set WHERE t1.product_id = t2.product_id' % (table))

def update_product_duplicates(session,product_id):
    try:
        # get feature summary and update in ProductDetails table
        data = get_duplicate_products(product_id)
        # Add all details to listing table
        listing_obj = session.query(ProductListing).filter(ProductListing.product_id==product_id).one()
        listing_obj.duplicate_set = data['parent_asin']
        if data['brand']:
            listing_obj.brand = data['brand'] 
        if data['model']:
            listing_obj.model = data['model']  
        session.flush()
        
        details_obj = session.query(ProductDetails).filter(ProductDetails.product_id==product_id).first()
        if details_obj is not None:
            details_obj.duplicate_set = data['parent_asin']
            details_obj.related_products = json.dumps(data['related_products'])
            if data['brand']:
                details_obj.brand = data['brand'] 
            if data['model']:
                details_obj.model = data['model']  
        session.flush() 
        # Commit the session
        session.commit()
    except Exception as ex:
        print(ex)

def update_duplicate_sets(session,update_all='False'): 
    # Get all product ids from ProductDetails, update_all is string value from command line
    if update_all=='True':
        product_list = [product.product_id for product in session.query(ProductListing).all()]
    else:
        product_list = [product.product_id for product in session.query(ProductListing).filter(or_(ProductListing.duplicate_set==None,ProductListing.duplicate_set=="")).all()]
    
    print("# of products to be updated "+str(len(product_list)))
    
    for product_id in product_list:
        time.sleep(2)
        print("Update product id "+product_id)
        update_product_duplicates(session,product_id)
    
def get_duplicate_products(product_id):
    try:    
        child_data = Catalog(marketplace=Marketplaces.IN,credentials=credentials).get_item(product_id).payload
    except SellingApiException as ex:
        print(ex)
        
    results = {}
    # Get brand and define model. Model is not picked here since it may be including variant info
    brand = ""
    model = ""
    title = ""
    parent_asin=""
    related_products = []
    # Get parent asin
    if len(child_data['Relationships']) >0:
        parent_asin = child_data['Relationships'][0]['Identifiers']['MarketplaceASIN']['ASIN']
        # Get all related products
        try:
            parent_data = Catalog(marketplace=Marketplaces.IN,credentials=credentials).get_item(parent_asin).payload
        except SellingApiException as ex:
            print(ex)
            
        if len(parent_data['Relationships'])>0:
            related_products = []
            #Take keys from any row
            keys = parent_data['Relationships'][0].keys()
            for row in parent_data['Relationships']:
                dict ={}
                dict['product_id']=row['Identifiers']['MarketplaceASIN']['ASIN']
                for key in keys:
                    if key =='Identifiers':
                        continue
                    dict[key] = row[key]
                related_products.append(dict)
        
        # Get brand, model and title
        if 'Brand' in parent_data['AttributeSets'][0].keys():
            brand = parent_data['AttributeSets'][0]['Brand']
        if 'Model' in parent_data['AttributeSets'][0].keys():
            model = parent_data['AttributeSets'][0]['Model']
        if 'Title' in parent_data['AttributeSets'][0].keys():
            title = parent_data['AttributeSets'][0]['Title']
                
   
    # if brand and model not available in parent data, then take it from child
    if not model and 'Model' in child_data['AttributeSets'][0].keys():
        model = child_data['AttributeSets'][0]['Model']     
    if not brand and 'Brand' in child_data['AttributeSets'][0].keys():
        brand = child_data['AttributeSets'][0]['Brand']
    
        
    results['brand'] = brand.lower()
    results['model'] = model.lower()
    results['title'] = title.lower()
    results['parent_asin'] = parent_asin
    results['related_products'] = related_products
    return results
                    

def update_featurewise_reviews(session,product_id=None,update_all=False):
    
    if product_id is not None:
        try:
            # get feature summary and update in ProductDetails table
            data = json.dumps(get_featurewise_reviews(product_id))
            obj = session.query(ProductDetails).filter(ProductDetails.product_id==product_id).one()
            obj.featurewise_reviews = data
            session.commit()
        except Exception as ex:
            print(ex)
        
    else:
        # Get all product ids from ProductDetails, update_all is string value from command line
        if update_all=='True':
            product_list = [product.product_id for product in session.query(ProductDetails).all()]
        else:
            product_list = [product.product_id for product in session.query(ProductDetails).filter(or_(ProductDetails.featurewise_reviews==None,ProductDetails.featurewise_reviews=='{}')).all()]
        print(len(product_list))
        for product_id in product_list:
            try:
                data = json.dumps(get_featurewise_reviews(product_id))
                time.sleep(2)
                obj = session.query(ProductDetails).filter(ProductDetails.product_id==product_id).one()
                obj.featurewise_reviews = data
                session.commit()
            except Exception as ex:
                print(ex) 
                

def get_featurewise_reviews(product_id):
    # General request headers for fetching featurewise_reviews
    url='https://www.amazon.in/hz/reviews-render/ajax/lazy-widgets/stream'
    
    headers = {
    'authority': 'www.amazon.in',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    'rtt': '100',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
    'accept': 'text/html,*/*',
    'cache-control': 'no-cache',
    'x-requested-with': 'XMLHttpRequest',
    'downlink': '9.75',
    'ect': '4g',
    'origin': 'https://www.amazon.in',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }
    
    params = (
            ('asin', product_id),
            ('language', 'en_IN'),
            ('lazyWidget', 'cr-summarization-attributes'),
            )
    
    proxies = { # Hard coded for now
    'http': 'http://192.126.131.174:8800',
    'https': 'http://192.126.131.174:8800',
    }
    
    response = requests.get('https://www.amazon.in/hz/reviews-render/ajax/lazy-widgets/stream', headers=headers, params=params,proxies=proxies)
    assert response.status_code == 200
    # Parse the response
    soup = BeautifulSoup(response.text,'lxml')
    
    txt = soup.text 
    '''Output like: '["update",".cr-lazy-widget.cr-summarization-attributes","\\n      
    \\n\\n  By featurePicture quality4.44.4Sound quality4.44.4Low light4.24.2
    Fingerprint reader4.24.2Camera quality4.04.0Battery life3.33.3See more\\n\\n\\n  "]\n&&&\n'''
    data = {}
    
    if "by feature" in txt.lower():    
        new_str = txt[txt.find("By feature")+len("By feature"):txt.find("See more")] #Output like: 'Picture quality4.44.4Sound quality4.44.4Low light4.24.2Fingerprint reader4.24.2Camera quality4.04.0Battery life3.33.3'
        
        # Split with delimiter like 4.3
        res = re.split('(\d.\d)', new_str) 

        '''Output like: ['Picture quality', '4.4', '', '4.4', 'Sound quality', '4.4', '', '4.4', 
        'Low light', '4.2', '', '4.2', 'Fingerprint reader', '4.2', '', '4.2', 
        'Camera quality', '4.0', '', '4.0', 'Battery life', '3.3', '', '3.3', '']'''
        
        list_str = [i for i in res if i] # remove blank strings Output like  ['Picture quality', '4.4', '4.4', 'Sound quality', '4.4', '4.4', 'Low light', '4.2', '4.2', 'Fingerprint reader', '4.2', '4.2', 'Camera quality', '4.0', '4.0', 'Battery life', '3.3', '3.3']
        
        # Finally remove every 3rd element in the list since they are repeating
        del list_str[2::3]
        
        # If we don't have See more in feature-reviews, need to remove the last element since output is like list_str=['Battery life', '4.1', 'Fingerprint reader', '4.0', 'Value for money', '3.8', '\\n\\n\\n  "]\n&&&']
        if "See more" not in txt:
            list_str.pop(-1)
            
        # Change list to dictionary, every odd element is key and every even element is value
        key_list=list_str[::2]
        val_list=list_str[1::2]
        for index in range(len(key_list)):
            data[key_list[index]]=val_list[index]
            
    return data
    
    
def close_all_db_connections(engine, SessionFactory):
    SessionFactory.close_all()
    engine.dispose()
    logger.info(f"Closed all DB connections!")


if __name__ == '__main__':
    # Start a session using the existing engine
    parser = argparse.ArgumentParser()
    # parser.add_argument('--index_duplicate_sets', help='Index Duplicate Sets', default=False, action='store_true')
    # parser.add_argument('--update_duplicate_sets', help='Update Duplicate Sets', default=False, action='store_true')
    parser.add_argument('--index_qandas', help='Index Q and A', default=False, action='store_true')
    parser.add_argument('--index_reviews', help='Index Reviews', default=False, action='store_true')
    parser.add_argument('--update_listing_alerts', help='Update Listing Alerts', default=False, action='store_true')
    parser.add_argument('--assign_subcategories', help='Assign Subcategories', default=False, action='store_true')
    parser.add_argument('--close_all_db_connections', help='Forcibly close all DB connections', default=False, action='store_true')
    parser.add_argument('--get_last_review_date', help='Get the date of the last review given the product ID', default=None, type=str)
    
    parser.add_argument('--insert_sentiment_breakdown', help='Inserts the sentiment summary to the DB', default=False, action='store_true')
    parser.add_argument('--insert_sentiment_reviews', help='Inserts the sentiment reviews to the DB', default=False, action='store_true')
    parser.add_argument('--filename', help='Filename', default=None, type=str)
    parser.add_argument('--featurewise_review', help='Update all features or only non existing featurewise reviews, value True or False', default=None, type=str)
    parser.add_argument('--update_feature_product', help='Product_id for feature update', default=None, type=str)
    parser.add_argument('--update_duplicate_sets', help='Update all duplicate sets or only non existing duplicate sets in ProductListing and ProductDetails tables, value True or False', default=None, type=str)
    
    
    args = parser.parse_args()
    
    _assign_subcategories = args.assign_subcategories
    _close_all_db_connections = args.close_all_db_connections
    _get_last_review_date = args.get_last_review_date

    _insert_sentiment_breakdown = args.insert_sentiment_breakdown
    _insert_sentiment_reviews = args.insert_sentiment_reviews
    _feature_product = args.update_feature_product
    _featurewise_review = args.featurewise_review
    _update_duplicates = args.update_duplicate_sets


    filename = args.filename

    from sqlalchemy import desc
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=True)

    session = Session()

    if _get_last_review_date is not None:
        get_last_review_date(session,_get_last_review_date)
    if _assign_subcategories == True:
        for category in subcategory_dict:
            assign_subcategories(session, category, table='ProductDetails')
    if _close_all_db_connections == True:
        close_all_db_connections(engine, Session)
    if _insert_sentiment_breakdown == True:
        insert_sentiment_breakdown(config('DB_NAME'), filename=filename)
    if _insert_sentiment_reviews == True:
        insert_sentiment_reviews(config('DB_NAME'), filename=filename)
    if _feature_product:
        update_featurewise_reviews(session,product_id=_feature_product)
    if _featurewise_review:
        update_featurewise_reviews(session,update_all=_featurewise_review)  
    if _update_duplicates:
        update_duplicate_sets(session,update_all=_update_duplicates)
        
    exit(0)
   
    