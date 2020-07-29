import sqlite3
import json
import pickle
from datetime import datetime
import parse_html

db_file = 'db.sqlite'
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


def create_tables(db_file='db.sqlite'):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    for table in tables.keys():
        fieldset = []
        for col, definition in tables[table].items():
            if 'FOREIGN KEY' in definition:
                fieldset.append("FOREIGN KEY ({0}) {1}".format(col[1:], definition[1]))
            else:
                fieldset.append("'{0}' {1}".format(col, definition))

        if len(fieldset) > 0:
            query = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(table, ", ".join(fieldset))
            c.execute(query)
    c.close()


def insert_product_listing(conn, data, table='ProductListing'):
    cursor = conn.cursor()
    fields = ", ".join(field for field in tables[table])
    group = "(" + ", ".join('?' for _ in tables[table]) +  ")"
    query = f"INSERT INTO {table} ({fields}) VALUES {group}"
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
                            row[key] = int(value[key].replace(',', ''))
                        elif key in ('price', 'old_price'):
                            row[key] = int(value[key][1:].replace(',', ''))
                        else:
                            row[key] = value[key]
                    else:
                        row[key] = value[key]
                values = list(row[field] for field in tables[table])
                try:
                    cursor.execute(query, values)
                except sqlite3.IntegrityError:
                    cursor.execute(f"SELECT * from {table} WHERE product_id='{row['product_id']}'")
                    result = cursor.fetchall()[0]
                    update_fields = (field for idx, field in enumerate(tables[table]) if result[idx] is None)
                    params = ", ".join(field + '=?' for field in update_fields)
                    update_query = f"UPDATE {table} SET {params} WHERE product_id='{row['product_id']}'"
                    update_fields = list(values[idx] for idx, _ in enumerate(tables[table]) if result[idx] is None)
                    cursor.execute(update_query, update_fields)
    cursor.close()


def insert_product_details(conn, data, table='ProductDetails', is_sponsored=False):
    cursor = conn.cursor()
    useless_fields = set({'id', '_product_id'})
    
    if is_sponsored == True:
        table = 'SponsoredProductDetails'
    
    fields = ", ".join(field for field in tables[table] if field not in useless_fields)
    group = "(" + ", ".join('?' for field in tables[table] if field not in useless_fields) +  ")"
    query = f"INSERT INTO {table} ({fields}) VALUES {group}"
    row = {key: (data[key] if not (isinstance(data[key], list) or isinstance(data[key], dict)) else json.dumps(data[key])) for key in data}
    for field in row:
        if row[field] is not None:
            if field == 'num_reviews':
                row[field] = int(row[field].split()[0].replace(',', ''))
            elif field in ('curr_price'):
                row[field] = float(row[field].replace(',', ''))
    row['created_on'] = datetime.now()
    values = list(row[field] for field in tables[table] if field not in useless_fields)
    try:
        cursor.execute(query, values)
    except sqlite3.IntegrityError:
        cursor.execute(f"SELECT * from {table} WHERE product_id='{row['product_id']}'")
        result = cursor.fetchall()[0]
        update_fields = (field for idx, field in enumerate(tables[table]) if result[idx] is None)
        params = ", ".join(field + '=?' for field in update_fields)
        update_query = f"UPDATE {table} SET {params} WHERE product_id='{row['product_id']}'"
        update_fields = list(values[idx] for idx, _ in enumerate(tables[table]) if result[idx] is None)
        cursor.execute(update_query, update_fields)
    cursor.close()


def insert_product_qanda(conn, qanda, product_id, table='QandA'):
    cursor = conn.cursor()
    fields = ("product_id", "question", "answer",)
    group = "(" + ", ".join('?' for field in fields) +  ")"
    query = f"INSERT INTO {table} {fields} VALUES {group}"
    for pair in qanda:
        question, answer = pair['question'], pair['answer']
        value = list([product_id, question, answer])
        cursor.execute(query, value)
    cursor.close()


def insert_product_reviews(conn, reviews, product_id, table='Reviews'):
    cursor = conn.cursor()
    fields = ("product_id", "rating", "review_date", "title", "body", 'product_info', 'verified_purchase', 'helpful_votes',)
    group = "(" + ", ".join('?' for field in fields) +  ")"
    query = f"INSERT INTO {table} {fields} VALUES {group}"
    for review in reviews['reviews']:
        rating = float(review['rating'].split()[0])
        review_date = review['review_date']
        title = review['title']
        body = review['body']
        if isinstance(review['product_info'], list) or isinstance(review['product_info'], dict):
            product_info = json.dumps(review['product_info'])
        else:
            product_info = json.dumps(review['product_info'])
        verified_purchase = review['verified_purchase']
        helpful_votes = review['helpful_votes']
        value = list([product_id, rating, review_date, title, body, product_info, verified_purchase, helpful_votes])
        cursor.execute(query, value)
    cursor.close()


if __name__ == '__main__':
    # Setup the DB
    create_tables(db_file='db.sqlite')

    with open('dumps/mobile.pkl', 'rb') as f:
        product_listing = pickle.load(f)

    with open('dumps/dump_B07DJLVJ5M.pkl', 'rb') as f:
        product_details = pickle.load(f)

    with open('dumps/dump_B07DJLVJ5M_qanda.pkl', 'rb') as f:
        qanda = pickle.load(f)
    
    #with open('dumps/dump_B07DJLVJ5M_reviews.pkl', 'rb') as f:
    #    reviews = pickle.load(f)

    with sqlite3.connect(db_file) as conn:
        insert_product_listing(conn, product_listing)
        #insert_product_details(conn, product_details)
        #product_id = product_details['product_id']
        #insert_product_qanda(conn, qanda, product_id=product_id)
        #insert_product_reviews(conn, reviews, product_id=product_id)
