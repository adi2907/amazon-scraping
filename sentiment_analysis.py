import argparse
import glob
import os
import pickle
import re
import time
from datetime import datetime

import pandas as pd
import spacy
from decouple import config
from spacy.matcher import PhraseMatcher
from transformers import pipeline

try:
    import en_core_web_sm
except ImportError:
    raise ValueError(f"Please download the English CNN model using 'python -m spacy download en_core_web_sm'")

import db_manager

DATASET_PATH = os.getcwd()

PARAMETERS_FILE = 'parameters.csv'
REVIEWS_FILE = 'Reviews.csv'
CLEANED_UP_FILE = 'Preprocessed.csv'

OUTPUT_FILE = 'sentiments'

try:
    sentiment_classifier = pipeline('sentiment-analysis')
    nlp = en_core_web_sm.load()
except:
    print(f"Warning: Please install PyTorch / Tensorflow. Crucial features won't work")

def preprocess_reviews():
    df = pd.read_csv(REVIEWS_FILE, sep=",", encoding="utf-8", usecols=["id", "product_id", "title", "body", "category"])
    df['body'] = df['body'].apply(lambda content: re.sub(r'\.\.+', '.', content.replace('\\n', '.').strip())[2:] if isinstance(content, str) else content)
    return df


def preprocess(category):
  # nlp = spacy.load("en_core_web_sm")
  category_str = category+'.*'
  keyword_dict = pd.read_csv(PARAMETERS_FILE).filter(regex=category_str)

  # Remove the super category from the header
  new_header = keyword_dict.iloc[0]

  keyword_dict=keyword_dict[1:]
  keyword_dict.columns = new_header
  header_list=list(keyword_dict.iloc[0])
  return keyword_dict, header_list


def aspect_based_sa(review, keyword_dict, header_list):
    try:
      #Create Spacy matcher with list of synonmys for aspects
      matcher = PhraseMatcher(nlp.vocab)
      for word in header_list:
        synonym_list = [nlp(text) for text in keyword_dict[word].dropna(axis = 0)]
        matcher.add(word, None, *synonym_list)
      #Break review into sentences and analyse sentiment on each
      absa_dict = {}

      doc = nlp(review)
      for sentence in doc.sents:
        matches = matcher(nlp(sentence.text))
        for match_id, start, end in matches:
            aspect = nlp.vocab.strings[match_id]
            sent_id = sentiment_classifier(sentence.text)[0]
            sentiment = 1 if sent_id['label'] == 'POSITIVE' else -1
            score = sent_id['score']
            if score > 0.7:
              absa_dict[aspect] = sentiment
    except Exception as ex:
        print(ex)
        return {"NaN": True}
    return absa_dict


def analyse(df, keyword_dict, header_list):
    sentiments = []
    idx = 0

    for i in range(0, df.count()['body']):
        if df['category'][i] != "headphones":
            sentiments.append({'id': i})
            continue

        review = df['body'][idx]
        _id = df['id'][idx]
        product_id = df['product_id'][idx]

        if isinstance(df['body'][idx], str):
            sentiments.append({'id': i, **aspect_based_sa(df['body'][idx], keyword_dict, header_list)})
        else:
            sentiments.append({'id': i})

        if idx % 1000 == 0:
            print(f"{datetime.now()} Idx = {idx}")

        if idx % 1000 == 0:
            print("Dumping to pickle file...")
            with open(OUTPUT_FILE + "_" + str(int(idx / 1000)) + ".pkl", 'wb') as f:
                pickle.dump(sentiments, f)
            sentiments = []
            print("Sleeping a bit....")
            time.sleep(3)

        idx += 1

    print("Dumping to pickle file...")
    with open(OUTPUT_FILE + "_" + str(int(idx / 1000) + 1) + ".pkl", 'wb') as f:
        pickle.dump(sentiments, f)
    sentiments = []


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


def construct_indexed_df(reviews_df, indexed_sentiments=None): # From CLEANED_UP file
    if indexed_sentiments is None:
        with open(os.path.join(DATASET_PATH, 'indexed_sentiments.pkl'), 'rb') as f:
            indexed_sentiments = pickle.load(f)
    sentiments = [{'id': reviews_df['id'][idx], 'product_id': reviews_df['product_id'][idx], **(indexed_sentiment)} for idx, indexed_sentiment in enumerate(indexed_sentiments) if indexed_sentiment not in ({}, None)]
    indexed_df = pd.DataFrame(sentiments)
    indexed_df.dropna(thresh=1)
    return indexed_df


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


def clean_up_reviews():
    df = preprocess_reviews()
    df.to_csv(CLEANED_UP_FILE, index=False)


def sentiment_analysis():
    df = pd.read_csv(CLEANED_UP_FILE, sep=",", encoding="utf-8")
    a, b = preprocess("headphones")
    analyse(df, a, b)


def fetch_category_info(engine, category, month, year):
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    
    first_day = '01'
    if month == '02':
        if year % 4 == 0:
            last_day = '29'
        else:
            last_day = '28'
    elif month in ['01', '03', '05', '07', '08', '10', '12']:
        last_day = '31'
    else:
        last_day = '30'
    
    results = pd.read_sql_query(f"SELECT Reviews.id, Reviews.product_id, Reviews.rating, Reviews.review_date, Reviews.helpful_votes, Reviews.title, Reviews.body, Reviews.is_duplicate, Reviews.duplicate_set, ProductListing.category FROM Reviews INNER JOIN ProductListing WHERE (ProductListing.category = 'headphones' AND ProductListing.product_id = Reviews.product_id AND Reviews.is_duplicate = False AND Reviews.review_date BETWEEN '{year}-{month}-{first_day}' AND '{year}-{month}-{last_day}') ORDER BY Reviews.duplicate_set asc, Reviews.title ASC, Reviews.review_date ASC, Reviews.title asc", engine)
    results.to_csv(os.path.join(DATASET_PATH, 'Reviews.csv'), index=False, sep=",")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--category', help='Category for dumping Reviews', type=str, default=None)
    parser.add_argument('-m', '--month', help='List the month', type=int, default=None)
    parser.add_argument('-y', '--year', help='List the year', type=int, default=None)

    parser.add_argument('--test', help='Testing', default=False, action='store_true')

    args = parser.parse_args()

    category = args.category
    month = args.month
    year = args.year
    test = args.test

    if test == True:
        try:
            DB_USER = config('DB_USER')
            DB_PASSWORD = config('DB_PASSWORD')
            DB_PORT = config('DB_PORT')
            DB_NAME = config('DB_NAME')
            DB_SERVER = config('DB_SERVER')
            DB_TYPE = config('DB_TYPE')
            engine = db_manager.Database(dbtype=DB_TYPE, username=DB_USER, password=DB_PASSWORD, port=DB_PORT, dbname=DB_NAME, server=DB_SERVER).db_engine
        except:
            DB_TYPE = 'sqlite'
            engine = db_manager.Database(dbtype=DB_TYPE).db_engine
        results = pd.read_sql_query(f"SELECT * FROM ProductListing", engine)
        results.to_csv(os.path.join(DATASET_PATH, 'test.csv'), index=False, sep=",")
        exit(0)

    if category is not None:
        if month is None or year is None:
            raise ValueError(f"Need to specify --month and --year")
        try:
            DB_USER = config('DB_USER')
            DB_PASSWORD = config('DB_PASSWORD')
            DB_PORT = config('DB_PORT')
            DB_NAME = config('DB_NAME')
            DB_SERVER = config('DB_SERVER')
            DB_TYPE = config('DB_TYPE')
            engine = db_manager.Database(dbtype=DB_TYPE, username=DB_USER, password=DB_PASSWORD, port=DB_PORT, dbname=DB_NAME, server=DB_SERVER).db_engine
        except:
            DB_TYPE = 'sqlite'
            engine = db_manager.Database(dbtype=DB_TYPE).db_engine
        # Fetch
        fetch_category_info(engine, category, month, year)

    # Pre-process
    clean_up_reviews()
    
    # Run the sentiment analysis
    sentiment_analysis()
    
    # Post-process
    review_df = pd.read_csv(os.path.join(DATASET_PATH, REVIEWS_FILE), sep=",", encoding="utf-8", usecols=["id", "product_id", "title", "body", "category"])
    indexed_sentiments = aggregate_sentiments_after_script()
    indexed_df = construct_indexed_df(review_df, indexed_sentiments)
    indexed_df.to_csv(os.path.join(DATASET_PATH, 'sentiment_analysis.csv'))
    counts = count_ranges(indexed_df, review_df)

    with open('sentiment_counts.pkl', 'wb') as f:
        pickle.dump(counts, f)

    df_count = pd.DataFrame(counts).T
    df_count.to_csv(os.path.join(DATASET_PATH, 'sentiment_counts.csv'))
