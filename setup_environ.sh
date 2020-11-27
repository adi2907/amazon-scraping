#!/bin/bash

cp .env spider/.env
cp .env backend/amazonscraper/.env

cp categories.json backend/amazonscraper/categories.json
cp scrapingtool/subcategories.py backend/amazonscraper/subcategories.py
cp scrapingtool/subcategories.py spider/scrapingtool/subcategories.py
cp categories.json spider/categories.json