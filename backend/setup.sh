#!/bin/bash

cp ../.env amazonscraper/.env
cp ../scrapingtool/subcategories.py amazonscraper/subcategories.py

pip3 install -r requirements.txt
python3 manage.py makemigrations
python3 manage.py makemigrations accounts
python3 manage.py makemigrations dashboard
python3 manage.py migrate
python3 manage.py collectstatic