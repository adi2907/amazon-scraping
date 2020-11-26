#!/bin/bash

python3 scrapingtool/sentiment_analysis.py --category "all" --start_date "2020-08-01" --end_date "2020-11-01"
python3 scrapingtool/db_manager.py --insert_sentiment_breakdown --filename "data/sentiment_counts_all.pkl"
python3 scrapingtool/db_manager.py --insert_sentiment_reviews --filename "data/sentiment_db_all.csv"