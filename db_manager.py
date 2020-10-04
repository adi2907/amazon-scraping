# Represents all the Models used to create our scraper

import os
import glob

import json
import pickle
import re
import sqlite3
import datetime

import pymysql
from decouple import UndefinedValueError, config
from pytz import timezone
from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKey, Integer,
                        MetaData, String, Table, Text, create_engine, exc)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, relationship, sessionmaker
from sqlalchemy.orm.exc import NoResultFound, FlushError
from sqlitedict import SqliteDict

from utils import create_logger

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
        'product_url': 'LONGTEXT',
        'avg_rating': 'FLOAT',
        'total_ratings': 'INTEGER',
        'price': 'INTEGER',
        'old_price': 'INTEGER',
        'secondary_information': 'LONGTEXT',
        'image': 'TEXT(1000)',
        'is_duplicate': 'BOOLEAN',
        'short_title': 'TEXT(100)',
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
        'customer_lazy': 'INTEGER',
        'histogram': 'LONGTEXT',
        'reviews_url': 'LONGTEXT',
        'created_on': 'DATETIME',
        'subcategories': 'LONGTEXT',
        'is_sponsored': 'BOOLEAN',
        'completed': 'BOOLEAN',
        'brand': 'TEXT(100)',
        'model': 'TEXT(100)',
        'date_completed': 'DATETIME',
        'is_duplicate': 'BOOLEAN',
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
        '_product_id': ['FOREIGN KEY', 'REFERENCES ProductListing (product_id)'],
    },
    'DailyProductListing': {
        'id': 'INTEGER PRIMARY KEY',
        'product_id': 'TEXT(16)',
        'category': 'TEXT(100)',
        'serial_no': 'INTEGER',
        'avg_rating': 'FLOAT',
        'total_ratings': 'INTEGER',
        'price': 'INTEGER',
        'old_price': 'INTEGER',
        'date': 'DATETIME',
        #'_product_id': ['FOREIGN KEY', 'REFERENCES ProductListing (product_id)'],
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


table_map = {
    'ProductListing': ProductListing,
    'ProductDetails': ProductDetails,
    'SponsoredProductDetails': SponsoredProductDetails,
    'QandA': QandA,
    'Reviews': Reviews,
    'DailyProductListing': DailyProductListing,
}


def insert_product_listing(session, data, table='ProductListing'):
    row = dict()
    for category in data:
        row['category'] = category
        for page_num in data[category]:
            for title in data[category][page_num]:
                row['title'] = title
                value = data[category][page_num][title]
                for key in value:
                    if value[key] is not None:
                        if key == 'avg_rating':
                            row[key] = float(value[key].split()[0])
                        elif key == 'total_ratings':
                            row[key] = int(value[key].replace(',', '').replace('.', ''))
                        elif key in ('price', 'old_price'):
                            row[key] = int(value[key][1:].replace(',', '').replace('.', ''))
                        else:
                            row[key] = value[key]
                    else:
                        row[key] = value[key]
                try:
                    if row['product_id'] is not None:
                        obj = table_map[table]()
                        row['is_duplicate'] = None
                        [setattr(obj, key, value) for key, value in row.items() if hasattr(obj, key)]
                        session.add(obj)
                        session.commit()
                        return True
                except (exc.IntegrityError, FlushError,):
                    session.rollback()
                    result = session.query(table_map[table]).filter_by(product_id=row['product_id']).first()
                    if result is None:
                        return True
                    else:
                        update_fields = (field for field in tables[table] if field != "product_id")
                        temp = getattr(result, 'is_duplicate')
                        for field in update_fields:
                            setattr(result, field, row[field])
                        setattr(result, 'is_duplicate', temp)
                        try:
                            session.commit()
                            return True
                        except:
                            session.rollback()
                            logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
                            logger.newline()
                            return False
                except Exception as ex:
                    session.rollback()
                    productlisting_logger.critical(f"{row['product_id']}-> Exception: {ex}")
                    logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
                    logger.newline()
                    return False


def insert_daily_product_listing(session, data, table='DailyProductListing'):
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
                            row[key] = int(value[key][1:].replace(',', '').replace('.', ''))
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
                        return True
                except exc.IntegrityError:
                    session.rollback()
                    result = session.query(table_map[table]).filter_by(product_id=row['product_id']).first()
                    if result is None:
                        pass
                    else:
                        update_fields = (field for field in tables[table] if hasattr(result, field) and getattr(result, field) in (None, {}, [], "", "{}", "[]"))
                        for field in update_fields:
                            setattr(result, field, row[field])
                        # Update the date
                        date = datetime.datetime.now(timezone('Asia/Kolkata'))#.date()
                        setattr(result, 'date', date)
                        try:
                            session.commit()
                            return True
                        except:
                            session.rollback()
                            logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
                            logger.newline()
                            return False
                except Exception as ex:
                    session.rollback()
                    productlisting_logger.critical(f"{row['product_id']} -> Exception: {ex}")
                    logger.warning(f"For Product {row['product_id']}, there is an error with the data.")
                    logger.newline()
                    return False


def insert_product_details(session, data, table='ProductDetails', is_sponsored=False):
    row = {key: (data[key] if not (isinstance(data[key], list) or isinstance(data[key], dict)) else json.dumps(data[key])) for key in data}
    for field in row:
        if row[field] is not None:
            if field == 'num_reviews':
                row[field] = int(row[field].split()[0].replace(',', '').replace('.', ''))
            elif field in ('curr_price'):
                row[field] = float(row[field].replace(',', ''))
    row['created_on'] = datetime.datetime.now()
    row['is_sponsored'] = is_sponsored
    try:
        obj = table_map[table]()
        [setattr(obj, key, value) for key, value in row.items()]
        session.add(obj)
        session.commit()
        return True
    except (exc.IntegrityError, FlushError):
        session.rollback()
        result = session.query(table_map[table]).filter_by(product_id=row['product_id']).first()
        update_fields = (field for field in tables[table] if hasattr(result, field) and getattr(result, field) in (None, {}, [], "", "{}", "[]"))
        for field in update_fields:
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


def insert_product_qanda(session, qanda, product_id, table='QandA'):
    for pair in qanda:
        row = {key: (value if not isinstance(value, list) and not isinstance(value, dict) else json.dumps(value)) for key, value in pair.items()}
        # Add product id
        row['product_id'] = product_id
        obj = table_map[table]()
        [setattr(obj, key, val) for key, val in row.items()]
        session.add(obj)
    # TODO: Change this later outisde the loop
    try:
        session.commit()
        return True
    except Exception as ex:
        session.rollback()
        qanda_logger.critical(f"{product_id} -> Exception: {ex}")
        logger.warning(f" For Product {product_id}, there is an error with the data.")
        logger.newline()
        return False


def insert_product_reviews(session, reviews, product_id, table='Reviews'):
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
        row['page_num'] = review['page_num']
        row['is_duplicate'] = False
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


def query_table(session, table, query='all', filter_cond=None):
    if query == 'all':
        if filter_cond is None:
            try:
                instance = session.query(table_map[table]).all()
                return instance
            except:
                return None
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


def add_column(engine, table_name: str, column: Column):
    column_name = column.compile(dialect=engine.dialect)
    column_type = column.type.compile(engine.dialect)
    engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % (table_name, column_name, column_type))


def alter_column(engine, table_name: str, column_name: str, new_name: str, data_type: str):
    engine.execute('ALTER TABLE %s CHANGE COLUMN %s %s %s' % (table_name, column_name, new_name, data_type))


def dump_listing_from_cache(session, category, cache_file='cache.sqlite3'):
    with SqliteDict(cache_file) as cache:
        today = datetime.datetime.today().strftime("%d-%m-%y")
        for page in range(1, 100):
            key = f"LISTING_{category}_PAGE_{page}_{today}"
            value = cache.get(key)
            if value is None:
                continue
            try:
                status = insert_product_listing(session, value, table='ProductListing')
                if status == False:
                    logger.warning(f"Status = False for category: {category}, page: {page}")
                else:
                    logger.info(f"Category: {category}, Inserted Page: {page}")
            except Exception as ex:
                print(ex)


def dump_from_cache(session, category, cache_file='cache.sqlite3'):
    with SqliteDict(cache_file, autocommit=True) as cache:
        key = f"DETAILS_SET_{category}"
        if key in cache:
            _set = cache[key]
            for product_id in _set:
                logger.info(f"Dumping Product ID {product_id}")
                
                qanda_counter = 0
                reviews_counter = 0

                qanda_errors = f"ERRORS_QANDA_{product_id}"
                reviews_errors = f"ERRORS_REVIEWS_{product_id}"

                if qanda_errors not in cache:
                    cache[qanda_errors] = set()
                
                _q = cache[qanda_errors]
                
                if reviews_errors not in cache:
                    cache[reviews_errors] = set()
                
                _r = cache[reviews_errors]

                for page_num in range(10+1):
                    key = f"QANDA_{product_id}_{page_num}"
                    if key not in cache:
                        continue

                    data = cache[key]

                    if len(data) == 0 or 'page_num' not in data[0]:
                        continue

                    status = insert_product_qanda(session, data, product_id)
                    
                    if status == False:
                        logger.error(f"QandA: Error during dumping PAGE-{page_num} from cache for Product ID {product_id}")
                        _q.add(page_num)
                        break
                    else:
                        qanda_counter += 1
                        del cache[key]
                
                cache[qanda_errors] = _q
                
                for page_num in range(100+1):
                    key = f"REVIEWS_{product_id}_{page_num}"
                    if key not in cache:
                        continue
                    
                    data = cache[key]

                    if len(data) == 0 or 'page_num' not in data['reviews'][0]:
                        continue

                    status = insert_product_reviews(session, data, product_id)

                    if status == False:
                        logger.error(f"Reviews: Error during dumping PAGE - {page_num} from cache for Product ID {product_id}")
                        _r.add(page_num)
                        break
                    else:
                        reviews_counter += 1
                        del cache[key]
                
                cache[reviews_errors] = _r

                logger.info(f"For PRODUCT ID {product_id}, dumped {qanda_counter} QandA pages, and {reviews_counter} Review Pages")


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


def update_completed(session, table='ProductDetails'):
    from sqlalchemy import func

    instances = session.query(table_map[table]).all()
    count = 0
    for instance in instances:
        flag = True
        if instance.completed is None or instance.completed == False:
            if instance.num_reviews <= 1000:
                num_reviews = instance.num_reviews
                nr = -1
                with SqliteDict('cache.sqlite3') as mydict:
                    if f"NUM_REVIEWS_{instance.product_id}" in mydict:
                        nr = mydict[f"NUM_REVIEWS_{instance.product_id}"]
                        if not isinstance(nr, int):
                            nr = 1000
                    else:
                        flag = False
                if nr > -1:
                    num_reviews = nr
                if flag == False:
                    continue
                num_reviews_not_none = 0
                num_reviews_none = session.query(table_map['Reviews']).filter(Reviews.product_id == instance.product_id, Reviews.page_num == None).count()
                if num_reviews_none == 0:
                    num_reviews_not_none = session.query(table_map['Reviews']).filter(Reviews.product_id == instance.product_id, Reviews.page_num != None).count()
                
                if num_reviews_none >= round(int(0.85 * num_reviews)) or num_reviews_not_none >= round(int(0.85 * num_reviews)):
                    logger.info(f"{count} - ID {instance.product_id}: Marking as complete...")
                    instance.completed = True
                    try:
                        session.commit()
                        count += 1
                    except Exception as ex:
                        session.rollback()
                        print(ex)
            else:
                # Gt than 1000
                num_reviews = 1000
                nr = -1
                with SqliteDict('cache.sqlite3') as mydict:
                    if f"NUM_REVIEWS_{instance.product_id}" in mydict:
                        nr = mydict[f"NUM_REVIEWS_{instance.product_id}"]
                        if not isinstance(nr, int):
                            nr = 1000
                if nr > -1:
                    num_reviews = nr
                num_reviews_not_none = 0
                num_reviews_none = session.query(table_map['Reviews']).filter(Reviews.product_id == instance.product_id, Reviews.page_num == None).count()
                if num_reviews_none == 0:
                    num_reviews_not_none = session.query(table_map['Reviews']).filter(Reviews.product_id == instance.product_id, Reviews.page_num != None).count()
                if num_reviews_none >= round(int(0.9 * num_reviews)) or num_reviews_not_none >= round(int(0.9 * num_reviews)):
                    logger.info(f"{count} - ID {instance.product_id}: Marking as complete...")
                    instance.completed = True
                    try:
                        session.commit()
                        count += 1
                    except Exception as ex:
                        session.rollback()
                        print(ex)



def find_incomplete(session, table='ProductDetails'):
    instances = session.query(table_map[table]).all()
    count = 0
    pids = []
    for instance in instances:
        if instance.completed == True:
            if instance.num_reviews >= 1000:
                num_reviews = instance.num_reviews
                num_reviews_not_none = 0
                num_reviews_none = session.query(table_map['Reviews']).filter(Reviews.product_id == instance.product_id, Reviews.page_num == None, Reviews.review_date >= datetime.date(2020, 7, 1)).count()
                if num_reviews_none == 0:
                    num_reviews_not_none = session.query(table_map['Reviews']).filter(Reviews.product_id == instance.product_id, Reviews.page_num != None, Reviews.review_date >= datetime.date(2020, 7, 1)).count()
                
                if max(num_reviews_none, num_reviews_not_none) >= 900 and max(num_reviews_none, num_reviews_not_none) <= 1040:
                    logger.info(f"{count} - ID {instance.product_id}: Incomplete...")
                    pids.append(instance.product_id)
    return pids


def assign_subcategories(session, category, subcategory, table='ProductDetails'):
    import parse_data
    from bs4 import BeautifulSoup

    DUMP_DIR = os.path.join(os.getcwd(), 'dumps')

    if not os.path.exists(DUMP_DIR):
        return

    files = glob.glob(f"{DUMP_DIR}/{category}_{subcategory}_*")
    
    curr = 1

    for filename in files:
        with open(filename, 'r') as f:
            html = f.read()

        soup = BeautifulSoup(html, 'lxml')
        product_info, _ = parse_data.get_product_info(soup)

        for title in product_info:
            product_id = product_info[title]['product_id']
            if product_id is None:
                continue
            print(curr, product_id, title)
            curr += 1
            obj = query_table(session, 'ProductDetails', 'one', filter_cond=({'product_id': product_id}))
            if obj is not None:
                if obj.subcategories in ([], None):
                    obj.subcategories = json.dumps([subcategory])
                else:
                    subcategories = json.loads(obj.subcategories)
                    if subcategory in subcategories:
                        continue
                    subcategories.append(subcategory)
                    obj.subcategories = json.dumps(subcategories)
                try:
                    session.commit()
                except Exception as ex:
                    session.rollback()
                    print(ex)


def update_date(session):
    objs = query_table(session, 'ProductDetails', 'all')
    count = 0
    incorrect = 0
    for obj in objs:
        if obj.date_completed is None:
            continue
        try:
            i = session.query(Reviews).filter(Reviews.product_id == obj.product_id).order_by(desc('review_date')).first()
            if i is not None:
                obj.date_completed = i.review_date
                try:
                    session.commit()
                    count += 1
                    print(count)
                except Exception as ex:
                    session.rollback()
                    print(ex)
            else:
                incorrect += 1
                print(f"Incorrect - {incorrect}: Possible integrityError with PID {obj.product_id}")
        except Exception as ex:
            print(ex)
            continue


def update_product_listing_from_cache(session, category, cache_file='cache.sqlite3'):
    from sqlitedict import SqliteDict

    count = 0

    with SqliteDict(cache_file, autocommit=False) as mydict:
        today = datetime.datetime.today().strftime("%d-%m-%y")

        for page in range(1, 100+1):
            key = f"LISTING_{category}_PAGE_{page}_{today}"
            listing = mydict.get(key)
            if listing is None:
                continue

            # Update ProductListing
            status = insert_product_listing(session, listing)
            if not status:
                logger.warning(f"Error when updating listing details for PAGE {page}")
            else:
                count += 1
    
    logger.info(f"Successfully updated {count} products for category = {category}")


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
        if len(fin) > 0 and fin[-1] in [',']:
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


def mark_duplicates(session, category, table='ProductListing'):
    from sqlalchemy import asc, desc

    _table = table_map[table]
    
    queryset = session.query(_table).filter(_table.category == category, _table.is_duplicate == None, _table.total_ratings != None, _table.price != None, _table.title != None).order_by(asc('title')).order_by(desc('total_ratings')).order_by(desc('price'))

    reviews = {}
    pids = {}
    
    """
    for obj in queryset:
        short_title = ' '.join(word.lower() for word in obj.title.split()[:6])
        if short_title not in reviews:
            reviews[short_title] = session.query(table_map['Reviews']).filter(Reviews.product_id == obj.product_id, Reviews.is_duplicate != True).count()
            pids[obj.product_id] = short_title 
        else:
            db_reviews = session.query(table_map['Reviews']).filter(Reviews.product_id == obj.product_id, Reviews.is_duplicate != True).count()
            if reviews[short_title] >= db_reviews:
                num_reviews = reviews[short_title]
            else:
                num_reviews = db_reviews
                pids[obj.product_id] = short_title
            
            reviews[short_title] = num_reviews
    """
    
    prev = None

    duplicates = set()

    for idx, obj in enumerate(queryset):
        if idx == 0:
            prev = obj
            continue

        
        a = obj.short_title == prev.short_title

        b = ((abs(obj.total_ratings - prev.total_ratings) / max(obj.total_ratings, prev.total_ratings)) < 0.1)
        c = ((abs(obj.price - prev.price) / max(obj.price, prev.price)) < 0.1)

        if ((a & b) | (b & c) | (c & a)):
            # Majority Function
            duplicates.add(prev.product_id)
            duplicates.add(obj.product_id)
            continue

            if prev.product_id not in pids:
                obj.is_duplicate = True
                logger.info(f"Found duplicate - {obj.product_id} with prev id {prev.product_id}")
                try:
                    session.commit()
                except:
                    session.rollback()
                    logger.critical(f"Error during updating duplicate ID for product - {obj.product_id}")
        
        prev = obj
    
    from sqlitedict import SqliteDict

    with SqliteDict('cache.sqlite3', autocommit=True) as mydict:
        mydict[f'DUPLICATE_SET_{category}'] = duplicates


def mark_duplicate_reduced(session, category):
    from sqlitedict import SqliteDict

    with SqliteDict('cache.sqlite3', autocommit=False) as mydict:
        duplicates = mydict.get(f'DUPLICATE_SET_{category}')
    
    if duplicates is None:
        return

    pids = {}
    reviews = {}

    for pid in duplicates:
        obj = session.query(table_map['ProductListing']).filter(ProductListing.product_id == pid).first()
        if obj is None:
            continue

        short_title = obj.short_title
        if short_title not in reviews:
            reviews[short_title] = session.query(table_map['Reviews']).filter(Reviews.product_id == obj.product_id, Reviews.is_duplicate != True).count()
            pids[obj.product_id] = short_title 
        else:
            db_reviews = session.query(table_map['Reviews']).filter(Reviews.product_id == obj.product_id, Reviews.is_duplicate != True).count()
            if reviews[short_title] >= db_reviews:
                num_reviews = reviews[short_title]
            else:
                num_reviews = db_reviews
                pids[obj.product_id] = short_title
            
            reviews[short_title] = num_reviews

    with SqliteDict('cache.sqlite3', autocommit=True) as mydict:
        mydict[f'NON_DUPLICATE_SET_{category}'] = [pid for pid in pids]
    
    return

    count = 0
    
    for pid in duplicates:
        if pid not in pids:
            # Duplicate
            obj = query_table('ProductListing', 'one', filter_cond=({'product_id': pid}))
            setattr(obj, 'is_duplicate', True)
            try:
                session.commit()
                logger.info(f"Marked {pid} as duplicate!")
                count += 1
            except:
                session.rollback()
                logger.critical(f"Error when marking {pid} as duplicate")
    
    logger.info(f"Marked {count} products as duplicate!")



if __name__ == '__main__':
    # Start a session using the existing engine
    from sqlalchemy import desc
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=True)

    session = Session()

    #instances = session.query(ProductDetails).filter(ProductListing.category == "headphones", ProductListing.product_id == ProductDetails.product_id, ProductDetails.completed == None)
    #print(", ".join(obj.product_id for obj in instances[30:50]))
    
    #update_completed(session)
    #for c in ["smartphones", "ceiling fan", "washing machine", "refrigerator"]:
    #    dump_from_cache(session, c, cache_file='cache.sqlite3')
    #update_brands_and_models(session, 'ProductDetails')
    
    """
    count = 0
    objs = query_table(session, 'ProductDetails', 'all')
    instances = session.query(ProductDetails).filter(ProductListing.category == "ceiling fan", ProductListing.product_id == ProductDetails.product_id)
    for obj in instances:
        if obj is not None:
            if obj.curr_price is None:
                continue
            price = int(round(obj.curr_price))
            
            if price < 1500:
                subcategory = "economy"
            elif price >= 4000:
                subcategory = "luxury"
            elif price >= 1500 and price < 2500:
                subcategory = "standard"
            elif price >= 2500 and price < 4000:
                subcategory = "premium"

            subcategories = obj.subcategories
            if subcategories is not None:
                subcategories = json.loads(subcategories)
                if subcategory not in subcategories:
                    subcategories.append(subcategory)
                    obj.subcategories = json.dumps(subcategories)
            else:
                obj.subcategories = json.dumps([subcategory])
            try:
                session.commit()
                count += 1
                print(f"{obj.product_title} {count}")
            except:
                session.rollback()
                print(ex)
    """
    #assign_subcategories(session, 'washing machine', 'fully automatic')

    #print(fetch_product_ids(session, 'ProductListing', 'books'))

    #column = Column('category', String(100))
    #add_column(engine, 'DailyProductListing', column)
    #column = Column('is_duplicate', Boolean())
    #add_column(engine, 'ProductDetails', column)
    #update_date(session)
    #update_product_listing_from_cache(session, "headphones")
    #column = Column('short_title', String(100))
    #add_column(engine, 'ProductListing', column)
    mark_duplicates(session, "headphones")
    mark_duplicate_reduced(session, "headphones")
    exit(0)
    #add_column(engine, 'SponsoredProductDetails', column)
    
    #alter_column(engine, 'ProductDetails', 'categories', 'subcategories', 'Text')
    #alter_column(engine, 'SponsoredProductDetails', 'categories', 'subcategories', 'Text')
    #column = Column('is_sponsored', Boolean(), unique=False, default=False)
    #add_column(engine, 'ProductDetails', column)
    #add_column(engine, 'SponsoredProductDetails', column)

    #obj = query_table(session, 'ProductListing', 'one', filter_cond=({'product_id': '8173711461'}))
    #objs = query_table(session, 'ProductListing', 'all', filter_cond=({'category': 'books'}))
    #objs = query_table(session, 'ProductListing', 'all', filter_cond=['in', 'category', (('books', 'mobile'))])
    #if objs is not None:
    #    for obj in objs: 
    #        print(obj.product_id, obj.title)
    #else:
    #    print("Nothing Found")

    with open('dumps/headphones.pkl', 'rb') as f:
        product_listing = pickle.load(f)

    insert_daily_product_listing(session, product_listing)
    #insert_product_listing(session, product_listing)

    #with open('dumps/dump_B07DJLVJ5M.pkl', 'rb') as f:
    #    product_details = pickle.load(f)

    #insert_product_details(session, product_details, is_sponsored=False)

    #with open('dumps/dump_B07DJLVJ5M_qanda.pkl', 'rb') as f:
    #    qanda = pickle.load(f)

    #insert_product_qanda(session, qanda, product_id='B07DJLVJ5M')
