# python-scraping

This is the repo for scraping online websites using python -list, category pages

**********

## Requirements

To install the requirements for this project, run the following:

```bash
pip install -r requirements.txt
```

## Development Environment Setup

To setup the development environment on your local machine, proceed with the following steps:

* Create the environment file (`.env` file on your main project directory). A template environment is provided at `.env.example`. You can configure all your database, AWS credentials based on this template, and place it on your newly created `.env` file.

* After creating the environment, you need to install the below packages on your local machine:

```
1. Redis Server
2. Firefox
```

### Setup the local redis server

Firstly, you'll need to configure your local redis server and enter the credentials (`REDIS_SERVER_HOST=127.0.0.1`, etc) on `.env`. If you haven't setup a Redis Server password, you can remove the `REDIS_SERVER_PASSWORD` key on `.env`.

To test if your redis server is up and running, you can run the below commands from your Python interpreter session. Start this interpreter from the directory of this repository, and try the below sequence of python statements. You'll get an output similar to this:

```python
>>> from scrapingtool import cache
>>> cache = cache.Cache()
>>> cache.connect('master', use_redis=True)
2020-11-16 12:40:00 | INFO | Connected to Redis Cache!
>>> cache.set('foo', 'bar', timeout=30) # Will expire after 30 seconds
>>> cache.get('foo')
'bar'
```

### Library Overview and Scraping Commands

The entire library is divided into five parts:

1. Listing Scraping Module:

* For *creating* new items as more and more products get added to `ProductListing`. This module will be run on a daily basi
* Relevant files: `scrapingtool/browser.py`. The listing scraping is done using selenium.
* Command to run the category listing:

```bash
python3 scrapingtool/browser.py --category
```

2. Detail Scraping Module:

* For *updating* the `ProductDetails`, qanda and review information of existing products from the Listing table. This module is typically run on a weekly basis
* Relevant files: `scrapingtool/scraper.py`. The detail scraping is done using requests.
* Command to run the detail scraping:

```bash
python3 scrapingtool/scraper.py --categories "headphones" --override --listing --detail --no_listing --num_workers 5
```

This will scrape the details of the headphones category, and will spawn 5 worker threads (need to set `MULTITHREADING=True` in `.env` if using `--num_workers` option)

After the details scraping is done, you can run the below command to post-process everything and dump any pending data from the cache.

```bash
bash post_detail_scraping.sh
```

3. Archive Scraping Module:

* For *updating* the listing information of archived products. By definition, any product which does not show up in the listing page for that day will be classified as an archived product
* Relevant files: `spider/spiders/scraper.py` and `spider/pipelines.py`. The archive scraping is done using Scrapy.

To run scrapy, you must copy `.env` to `spider/.env` as well!

Change your directory to `spider` (where you recently copied your `.env` file) and run the below command to start the archive scraping:
Command to run the archive scraping:

```bash
scrapy crawl archive_details_spider -a category='all' -a instance_id=0 -a start_idx=0 -a end_idx=5 -o output.csv
```

4. Database Manager:

* This module is responsible for constructing the Database Schema and performing all database related operations for inserting and updating records for all the tables involved in the scraping.

There is no need to run this module separately, but it can still be used to fetch records in case a need arises to examine databases.

To export a database table into an external csv file, you can run the below command:

```bash
python3 scrapingtool/db_manager.py --export_to_csv --table "ProductListing" --csv "listing.csv"
```

This will export the `ProductListing` table into a csv file called `listing.csv`

Similarly, if you want to import a database table from an existing csv file, you can run the below command:

```bash
python3 scrapingtool/db_manager.py --import_from_csv --table "ProductListing" --csv "updated_listing.csv"
```

This will do the inverse operation of the export, where the database table is populated from an existing csv file.

5. Sentiment Analysis

* The sentiment analysis logic for classifying the sentiment of reviews is present in `scrapingtool/sentiment_analysis.py`

This needs to be done only after the details scraping is completed for a particular timeframe. It takes the records from `Reviews` table and analyzes this data (assumed to be bounded within a single month)

This can be done one a monthly basis, once all reviews for that month are retrieved for a category.

To run the sentiment analysis for a partcular month, run the below command:

```bash
python3 scrapingtool/sentiment_analysis.py --category "headphones" --month 10 --year 2020
```

The sentiment analysis will be done for headphones category for the month of October 2020.
The result of the analysis will be dumped into 2 files called `sentiment_analysis_headphones.csv` and `sentiment_counts_headphones.csv`.

The csv files can be exported into an external database, as per the need.


### Duplicate Sets

An important field in the `ProductListing` table is the `duplicate_set` Integer field. All the products are divided into disjoint sets, each indexed with a positive integer. This integer will represent the duplicate set index of that product, and a lot of the scraping logic + API is heavily dependent on the correctness of this field.

There needs to be an indexing done for the `ProductListing` table periodically, to ensure that the set indices are updated as per the live data.

When the ProductListing is done daily, the new products are initially assigned to an index of `NULL`. When it gets inserted, however, the program will try to look for the proper spot to insert that product. If the new product is found to match with an existing group, it is assigned to that same group (duplicate_set). Otherwise, it will be assigned a new index.

The re-indexing is done by comparing the `avg_rating` and `total_ratings` fields for all products across the same brand. The brand is assumed to be the first word of the product's title. If the ratings difference is less than 1% of the total ratings and the avg_rating difference is less than 0.1, both products are classified to be on the same duplicate set.

This is called as a *partial indexing*, and is implemented in the function `scrapingtool/db_manager` - update_duplicate_sets() function.

```bash
bash run_partial_indexing.sh
```

Periodically, however, a complete re-indexing may need to be done, to update any of the old products as well, since they can go to another duplicate set.

To run this full re-indexing, you can use the below command:

```bash
bash run_complete_reindexing.sh
```

This will completedly re-index all of the products across all the categories, and update the duplicate sets across relevant tables.

***********