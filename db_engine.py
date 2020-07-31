import json
import pickle
import sqlite3
from datetime import datetime

from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKey, Integer,
                        String, create_engine, exc)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

import parse_html

tables = {
    'ProductListing': {
        'product_id': 'TEXT PRIMARY KEY',
        'category': 'TEXT',
        'title': 'TEXT',
        'product_url': 'TEXT',
        'avg_rating': 'FLOAT',
        'total_ratings': 'INTEGER',
        'price': 'INTEGER',
        'old_price': 'INTEGER',
        'secondary_information': 'TEXT',
        'image': 'TEXT',
        },
    'ProductDetails': {
        'product_id': 'TEXT PRIMARY KEY',
        'product_title': 'TEXT',
        'byline_info': 'TEXT',
        'num_reviews': 'INTEGER',
        'answered_questions': 'TEXT',
        'curr_price': 'FLOAT',
        'features': 'TEXT',
        'offers': 'TEXT',
        'description': 'TEXT',
        'product_details': 'TEXT',
        'featurewise_reviews': 'TEXT',
        'customer_qa': 'TEXT',
        'customer_lazy': 'INTEGER',
        'customer_reviews': 'TEXT',
        'created_on': 'DATETIME',
        # '_product_id': ['FOREIGN KEY', 'REFERENCES ProductListing (product_id)'],
    },
    'SponsoredProductDetails': {
        'product_id': 'TEXT PRIMARY KEY',
        'product_title': 'TEXT',
        'byline_info': 'TEXT',
        'num_reviews': 'INTEGER',
        'answered_questions': 'TEXT',
        'curr_price': 'FLOAT',
        'features': 'TEXT',
        'offers': 'TEXT',
        'description': 'TEXT',
        'product_details': 'TEXT',
        'featurewise_reviews': 'TEXT',
        'customer_qa': 'TEXT',
        'customer_lazy': 'INTEGER',
        'customer_reviews': 'TEXT',
        'created_on': 'DATETIME',
        # '_product_id': ['FOREIGN KEY', 'REFERENCES ProductListing (product_id)'],
    },
    'QandA': {
        'id': 'INTEGER PRIMARY KEY',
        'product_id': 'TEXT',
        'question': 'TEXT',
        'answer': 'TEXT',
        'date': 'DATETIME',
        '_product_id': ['FOREIGN KEY', 'REFERENCES ProductListing (product_id)'],
    },
    'Reviews': {
        'id': 'INTEGER PRIMARY KEY',
        'product_id': 'TEXT',
        'rating': 'FLOAT',
        'review_date': 'DATETIME',
        'title': 'TEXT',
        'body': 'TEXT',
        'product_info': 'TEXT',
        'verified_purchase': 'INTEGER',
        'helpful_votes': 'INTEGER',
        '_product_id': ['FOREIGN KEY', 'REFERENCES ProductListing (product_id)'],
    }
}

field_map = {
    'INTEGER': 'Integer',
    'TEXT': 'String',
    'FLOAT': 'Float',
    'DATETIME': 'DateTime',
}

Base = declarative_base()

class ProductListing(Base):
    __tablename__ = 'ProductListing'

    product_id = Column(String, primary_key=True)
    category = Column(String)
    title = Column(String)
    product_url = Column(String)
    avg_rating = Column(Float)
    total_ratings = Column(Integer)
    price = Column(Integer)
    old_price = Column(Integer)
    secondary_information = Column(String)
    image = Column(String)
    qanda = relationship('QandA')
    reviews = relationship('Reviews')

    def __repr__(self):
        return f"<ProductListing(product_id={self.product_id}, category={self.category}, title={self.title})>"


class ProductDetails(Base):
    __tablename__ = 'ProductDetails'

    product_id = Column(String, primary_key=True)
    product_title = Column(String)
    byline_info = Column(String)
    num_reviews = Column(Integer)
    answered_questions = Column(String)
    curr_price = Column(Float)
    features = Column(String)
    offers = Column(String)
    description = Column(String)
    product_details = Column(String)
    featurewise_reviews = Column(String)
    customer_qa = Column(String)
    customer_lazy = Column(Boolean)
    customer_reviews = Column(String)
    created_on = Column(DateTime)

    def __repr__(self):
        return f"<ProductDetails(product_id={self.product_id}, product_tile={self.product_title})>"


class SponsoredProductDetails(Base):
    __tablename__ = 'SponsoredProductDetails'

    product_id = Column(String, primary_key=True)
    product_title = Column(String)
    byline_info = Column(String)
    num_reviews = Column(Integer)
    answered_questions = Column(String)
    curr_price = Column(Float)
    features = Column(String)
    offers = Column(String)
    description = Column(String)
    product_details = Column(String)
    featurewise_reviews = Column(String)
    customer_qa = Column(String)
    customer_lazy = Column(Boolean)
    customer_reviews = Column(String)
    created_on = Column(DateTime)

    def __repr__(self):
        return f"<SponsoredProductDetails(product_id={self.product_id}, product_tile={self.product_title})>"


class QandA(Base):
    __tablename__ = 'QandA'

    id = Column(Integer, primary_key=True)
    question = Column(String)
    answer = Column(String)
    date = Column(DateTime)
    product_id = Column(String, ForeignKey('Product Listing.product_id'))


class Reviews(Base):
    __tablename__ = 'Reviews'

    id = Column(Integer, primary_key=True)
    rating = Column(Float)
    review_date = Column(String)
    title = Column(String)
    body = Column(String)
    product_info = Column(String)
    verified_purchase = Column(Boolean)
    helpful_votes = Column(Integer)
    product_id = Column(String, ForeignKey('Product Listing.product_id'))



db_file = 'db.sqlite'
class MyDatabase:
    DB_ENGINE = {
        'sqlite': f'sqlite:///{db_file}'
    }

    # Main DB Connection Ref Obj
    db_engine = None
    def __init__(self, dbtype='sqlite', username='', password='', dbname=''):
        dbtype = dbtype.lower()
        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=dbname)
            self.db_engine = create_engine(engine_url)
        else:
            raise ValueError("DBType is not found in DB_ENGINE")


def create_tables(engine):
    Base.metadata.create_all(engine)


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
                        session.add(ProductListing(**row))
                        session.commit()
                except exc.IntegrityError:
                    session.rollback()
                    result = session.query(ProductListing).filter_by(product_id=row['product_id']).first()
                    update_fields = (field for field in tables[table] if getattr(result, field) is None)
                    for field in update_fields:
                        setattr(result, field, row[field])
                    session.commit()


def insert_product_details(session, data, table='ProductDetails', is_sponsored=False):
    if is_sponsored == True:
        table = 'SponsoredProductDetails'

    row = {key: (data[key] if not (isinstance(data[key], list) or isinstance(data[key], dict)) else json.dumps(data[key])) for key in data}
    for field in row:
        if row[field] is not None:
            if field == 'num_reviews':
                row[field] = int(row[field].split()[0].replace(',', '').replace('.', ''))
            elif field in ('curr_price'):
                row[field] = float(row[field].replace(',', ''))
    row['created_on'] = datetime.now()
    try:
        if is_sponsored == True:
            session.add(SponsoredProductDetails(**row))
        else:
            session.add(ProductDetails(**row))
        session.commit()
    except exc.IntegrityError:
        session.rollback()
        result = session.query(ProductListing).filter_by(product_id=row['product_id']).first()
        update_fields = (field for field in tables[table] if getattr(result, field) is None)
        for field in update_fields:
            setattr(result, field, row[field])
        session.commit()


def insert_product_qanda(session, qanda, product_id, table='QandA'):
    for pair in qanda:
        question, answer = pair['question'], pair['answer']
        row = {'question': question, 'answer': answer, 'product_id': product_id}
        session.add(QandA(**row))
    session.commit()


def insert_product_reviews(session, reviews, product_id, table='Reviews'):
    for review in reviews['reviews']:
        row = dict()
        row['rating'] = float(review['rating'].split()[0])
        row['review_date'] = review['review_date']
        row['title'] = review['title']
        row['body'] = review['body']
        if isinstance(review['product_info'], list) or isinstance(review['product_info'], dict):
            row['product_info'] = json.dumps(review['product_info'])
        else:
            row['product_info'] = json.dumps(review['product_info'])
        row['verified_purchase'] = review['verified_purchase']
        row['helpful_votes'] = review['helpful_votes']
        session.add(Reviews(**row))
    session.commit()


if __name__ == '__main__':
    # Setup the Engine
    engine = MyDatabase().db_engine
    create_tables(engine)
    
    Session = sessionmaker(bind=engine)

    session = Session()

    with open('dumps/mobile.pkl', 'rb') as f:
        product_listing = pickle.load(f)

    insert_product_listing(session, product_listing)
