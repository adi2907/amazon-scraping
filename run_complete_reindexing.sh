#!/bin/bash

python3 scrapingtool/db_manager.py --index_duplicate_sets
python3 scrapingtool/db_manager.py --index_qandas
python3 scrapingtool/db_manager.py --index_reviews
