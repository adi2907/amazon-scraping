#!/bin/bash

cp .env spider/.env
cp .env backend/amazonscraper/.env

cp categories.json backend/amazonscraper/categories.json
cp categories.json spider/categories.json

cp scrapingtool/subcategories.py backend/amazonscraper/subcategories.py
cp scrapingtool/subcategories.py spider/scrapingtool/subcategories.py

cp parameters.csv backend/parameters.csv
cp parameters.csv spider/parameters.csv
