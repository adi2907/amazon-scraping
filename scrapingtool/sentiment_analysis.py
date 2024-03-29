import argparse
import csv
import glob
import os
import pickle
import re
import time
from datetime import datetime

import pandas as pd
import stanza
from decouple import config

import db_manager

DATASET_PATH = os.path.join(os.getcwd(), 'data')
if not os.path.exists(DATASET_PATH):
    os.mkdir(DATASET_PATH)

PARAMETERS_FILE = 'parameters.csv'
REVIEWS_FILE = 'Reviews.csv'
CLEANED_UP_FILE = 'Preprocessed.csv'

OUTPUT_FILE = 'sentiments'


def preprocess_reviews(category):
    df = pd.read_csv(os.path.join(DATASET_PATH, REVIEWS_FILE), sep=",", encoding="utf-8", usecols=["id", "product_id", "title", "body", "category"])
    df['body'] = df['body'].apply(lambda content: re.sub(r'\.\.+', '.', content.replace('\\n', '.').strip())[2:] if isinstance(content, str) else content)
    return df


def load_model(download=False):
    if download == True:
        stanza.download('en')    
    nlp = stanza.Pipeline(lang='en', processors='tokenize,sentiment')
    return nlp

# Returns aspect based sentiment analysis for a review
def aspect_based_sa(nlp, keywords, review, category):
    aspect_dict = {}
    param_list = []
    
    # aspect list for the category
    head_list = [_list[1:] for _list in keywords if _list[0] == category]
    for _list in head_list:
        param_list.append([x for x in _list if x])
    
    # Break review into sentences, assign sentiment to each sentence
    doc = nlp(review)
    for sentence in doc.sentences:
        for _list in param_list:
            for element in _list:
                if element in sentence.text:
                    if sentence.sentiment != 1:
                        aspect = _list[0]
                        aspect_dict[aspect]=(1 if sentence.sentiment>1 else -1)
    return aspect_dict


def preprocess(category):
    # Read parameters file
    with open(PARAMETERS_FILE) as f:
        reader=csv.reader(f)
        data = list(reader)
    return data


def analyse(df, nlp, keywords, category):
    sentiments = []
    idx = 0

    for i in range(0, df.count()['body']):
        if category != 'all' and df['category'][i] != category:
            sentiments.append({'id': i})
            continue

        review = df['body'][idx]
        _id = df['id'][idx]

        if isinstance(df['body'][idx], str):
            sentiments.append({'id': i, **aspect_based_sa(nlp, keywords, review, df['category'][i])})
        else:
            sentiments.append({'id': i})

        if idx % 1000 == 0:
            print(f"{datetime.now()} Idx = {idx}")

        if idx % 1000 == 0:
            print("Dumping to pickle file...")
            with open(os.path.join(DATASET_PATH, OUTPUT_FILE + "_" + str(int(idx / 1000)) + ".pkl"), 'wb') as f:
                pickle.dump(sentiments, f)
            sentiments = []
            print("Sleeping a bit....")
            time.sleep(3)

        idx += 1

    print("Dumping to pickle file...")
    with open(os.path.join(DATASET_PATH, OUTPUT_FILE + "_" + str(int(idx / 1000) + 1) + ".pkl"), 'wb') as f:
        pickle.dump(sentiments, f)
    sentiments = []

# Combine all sentiment analysis pickles into a single pickle file
def aggregate_sentiments_after_script():
    files = sorted(glob.glob(os.path.join(DATASET_PATH, 'sentiments_*.pkl')), key=lambda x: int(x.rsplit('_')[1].split('.')[0]))
    indexed_sentiments = []
    for filename in files:
        with open(filename, 'rb') as f:
            sentiments = pickle.load(f)
        for sentiment in sentiments:
            indexed_sentiments.append(sentiment)    
    with open(os.path.join(DATASET_PATH, 'indexed_sentiments.pkl'), 'wb') as f:
        pickle.dump(indexed_sentiments, f)
    return indexed_sentiments


def construct_indexed_df(reviews_df, indexed_sentiments=None): 
    if indexed_sentiments is None:
        with open(os.path.join(DATASET_PATH, 'indexed_sentiments.pkl'), 'rb') as f:
            indexed_sentiments = pickle.load(f)
    sentiments = []
    for idx, indexed_sentiment in enumerate(indexed_sentiments):
        if indexed_sentiment not in ({}, None,):
            positive_sentiments = {}
            negative_sentiments = {}
            for feature in indexed_sentiment.keys():
                if feature == 'id':
                    continue
                sentiment = indexed_sentiment.get(feature, 0)
                if sentiment > 0:
                    positive_sentiments[feature] = sentiment
                elif sentiment < 0:
                    negative_sentiments[feature] = sentiment
            sentiments.append({'id': reviews_df['id'][idx], 'product_id': reviews_df['product_id'][idx], 'positive_sentiments': positive_sentiments, 'negative_sentiments': negative_sentiments})

    db_df = pd.DataFrame(sentiments)
    db_df.dropna(thresh=1)
    
    sentiments = [{'id': reviews_df['id'][idx], 'product_id': reviews_df['product_id'][idx], **(indexed_sentiment)} for idx, indexed_sentiment in enumerate(indexed_sentiments) if indexed_sentiment not in ({}, None)]
    indexed_df = pd.DataFrame(sentiments)
    indexed_df.dropna(thresh=1)

    return db_df, indexed_df


def get_unique_ids(df):
    id_df = df.drop_duplicates(['product_id'])[['product_id']]
    id_df = id_df.reset_index(drop=True) # Re-index
    return id_df


def get_range_dataframe(indexed_df, product_id, sentiment):
    pos = indexed_df[((indexed_df['product_id'] == product_id) & (indexed_df[sentiment] > 0))].shape[0]
    neg = indexed_df[((indexed_df['product_id'] == product_id) & (indexed_df[sentiment] < 0))].shape[0]
    return pos, neg


def count_ranges(indexed_df, review_df):
    headers = indexed_df.columns.values.tolist()
    # Dataframe of Product Ids
    id_df = review_df.drop_duplicates(['product_id'])[['product_id']]
    id_df = id_df.reset_index(drop=True) # Re-indexing
    counts = {}
    for idx in range(0, id_df['product_id'].count()):
        pid = id_df['product_id'][idx]
        counts[pid] = {}
        for column in headers:
            if column in ('id', 'product_id',):
                continue
            counts[pid][column] = {}
            pos, neg = get_range_dataframe(indexed_df, pid, column)
            counts[pid][column]['positive'] = pos
            counts[pid][column]['negative'] = neg
    return counts


def clean_up_reviews(category):
    df = preprocess_reviews(category)
    df.to_csv(os.path.join(DATASET_PATH, CLEANED_UP_FILE), index=False)


def sentiment_analysis(category):
    df = pd.read_csv(os.path.join(DATASET_PATH, CLEANED_UP_FILE), sep=",", encoding="utf-8")
    keywords = preprocess(category)
    nlp = load_model(download=True)
    # aspect_based_sa(nlp, keywords, 'Very good sound quality', category)
    analyse(df, nlp, keywords, category)


def fetch_category_info(engine, Session, category, start_date, end_date, last_review=False):
    from sqlalchemy import func

    # Fetch start and end date for review analysis
    try:
        tokens = start_date.split('-')
        start_year, start_month, start_day = tokens[0], int(tokens[1]), tokens[2]
        if start_month < 10:
            start_month = '0' + str(start_month)
        else:
            start_month = str(start_month)

        tokens = end_date.split('-')
        end_year, end_month, end_day = tokens[0], int(tokens[1]), tokens[2]
        if end_month < 10:
            end_month = '0' + str(end_month)
        else:
            end_month = str(end_month)
    except Exception as ex:
        print('start_date, end_date must be of form: YYYY-MM-DD')
        raise ex
    
    #Consider reviews from last sentiment analysed review
    if last_review == True:
        # Take max review id from sentiment analysis table (sentiment analysis id and review id are the same)
        with db_manager.session_scope(Session) as session:
            max_id = session.query(func.max(db_manager.SentimentAnalysis.id)).scalar()
        results = pd.read_sql_query(f"SELECT Reviews.id, Reviews.product_id, Reviews.rating, Reviews.review_date, Reviews.helpful_votes, Reviews.title, Reviews.body, Reviews.is_duplicate, Reviews.duplicate_set, ProductListing.category FROM Reviews INNER JOIN ProductListing WHERE (ProductListing.product_id = Reviews.product_id AND Reviews.is_duplicate = False AND Reviews.id > {max_id})", engine)
    else:
        if category == 'all':
            results = pd.read_sql_query(f"SELECT Reviews.id, Reviews.product_id, Reviews.rating, Reviews.review_date, Reviews.helpful_votes, Reviews.title, Reviews.body, Reviews.is_duplicate, Reviews.duplicate_set, ProductListing.category FROM Reviews INNER JOIN ProductListing WHERE (ProductListing.product_id = Reviews.product_id AND Reviews.is_duplicate = False AND Reviews.review_date BETWEEN '{start_year}-{start_month}-{start_day}' AND '{end_year}-{end_month}-{end_day}') ORDER BY Reviews.duplicate_set asc, Reviews.title ASC, Reviews.review_date ASC, Reviews.title asc", engine)
        else:
            results = pd.read_sql_query(f"SELECT Reviews.id, Reviews.product_id, Reviews.rating, Reviews.review_date, Reviews.helpful_votes, Reviews.title, Reviews.body, Reviews.is_duplicate, Reviews.duplicate_set, ProductListing.category FROM Reviews INNER JOIN ProductListing WHERE (ProductListing.category = '{category}' AND ProductListing.product_id = Reviews.product_id AND Reviews.is_duplicate = False AND Reviews.review_date BETWEEN '{start_year}-{start_month}-{start_day}' AND '{end_year}-{end_month}-{end_day}') ORDER BY Reviews.duplicate_set asc, Reviews.title ASC, Reviews.review_date ASC, Reviews.title asc", engine)
    results.to_csv(os.path.join(DATASET_PATH, REVIEWS_FILE), index=False, sep=",")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--category', help='Category for dumping Reviews', type=str, default=None)
    parser.add_argument('-s', '--start_date', help='List the start date', type=str, default=None)
    parser.add_argument('-e', '--end_date', help='List the end_date', type=str, default=None)
    parser.add_argument('-r', '--last_review', help='Whether to take sentiment analysis from the last review', default=False, action='store_true')

    parser.add_argument('--test', help='Testing', default=False, action='store_true')

    args = parser.parse_args()

    category = args.category
    start_date = args.start_date
    end_date = args.end_date
    test = args.test
    last_review = args.last_review

    Session = None

    if test == True:
        credentials = db_manager.get_credentials()
        engine, Session = db_manager.connect_to_db(config('DB_NAME'), credentials)
        results = pd.read_sql_query(f"SELECT * FROM ProductListing", engine)
        results.to_csv(os.path.join(DATASET_PATH, 'test.csv'), index=False, sep=",")
        db_manager.close_all_db_connections(engine, Session)
        exit(0)

    if category is not None:
        if start_date is None or end_date is None:
            pass
            # raise ValueError(f"Need to specify --month and --year")
        else:
            credentials = db_manager.get_credentials()
            engine, Session = db_manager.connect_to_db(config('DB_NAME'), credentials)
            # Fetch
            fetch_category_info(engine, Session, category, start_date, end_date, last_review=last_review)
    else:
        raise ValueError(f"Need to specify --category argument")

    # Pre-process
    clean_up_reviews(category)
    
    # Run the sentiment analysis
    sentiment_analysis(category)
    
    # Read all reviews in reviews dataframe
    review_df = pd.read_csv(os.path.join(DATASET_PATH, REVIEWS_FILE), sep=",", encoding="utf-8", usecols=["id", "product_id", "title", "body", "category"])
    
    # Combine all sentiment analysis pickles into a single pickle file
    indexed_sentiments = aggregate_sentiments_after_script()
    
    
    db_df, indexed_df = construct_indexed_df(review_df, indexed_sentiments)
    db_df.to_csv(os.path.join(DATASET_PATH, f'sentiment_db_{category}.csv'))
    indexed_df.to_csv(os.path.join(DATASET_PATH, f'sentiment_analysis_{category}.csv'))
    counts = count_ranges(indexed_df, review_df)

    with open(os.path.join(DATASET_PATH, f'sentiment_counts_{category}.pkl'), 'wb') as f:
        pickle.dump(counts, f)

    df_count = pd.DataFrame(counts).T
    df_count.to_csv(os.path.join(DATASET_PATH, f'sentiment_counts_{category}.csv'))

    # Finally insert into the DB
    db_manager.insert_sentiment_breakdown(config('DB_NAME'), counts)
    db_manager.insert_sentiment_reviews(config('DB_NAME'), db_df)

    db_manager.close_all_db_connections(engine, Session)
