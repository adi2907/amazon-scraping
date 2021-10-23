# scrapingtool

## Library Overview

*NOTE*: Here, we assume that the base directory is pointing towards the repository: `almetech/python-scraping`. Otherwise. the commands won't work.

The entire library is divided into five parts:

1. Listing Scraping Module:

* For *creating* new items as more and more products get added to `ProductListing`. This module will be run on a daily basi
* Relevant files: `scrapingtool/browser.py`. The listing scraping is done using selenium.
* Command to run the category listing:

```bash
python3 scrapingtool/browser.py category
```

2. Updating duplicates
* During list scraping, lot of SKUs get captured which are actually variants of the same SKU like color, size etc. They have the same number of ratings and price, hence are classified as duplicates
* Updating duplicates also updates the brand, title, model in both the ProductListing and ProductDetails table
* Update duplicates after list scraping so that only one value among duplicates is picked up during details scraping
* From amazon the duplicate set identifier is Parent Asin in case there are variants or child asin in case there are none

Update duplicates using the following command, set "True" to update all and "False" to update only those without a duplicate set
```bash
python scrapingtool/db_manager.py --update_duplicate_sets True
```

3. Detail Scraping Module:

* For *updating* the `ProductDetails`, qanda and review information of existing products from the Listing table. This module is typically run on a weekly basis
* Relevant files: `scrapingtool/scraper.py`. The detail scraping is done using requests.
* Command to run the detail scraping:

```bash
python3 scrapingtool/scraper.py --categories "smartphones"
```

This will scrape the details of the smartphones category. You can scrape multiple categories by passing them as comma separated arguments

```bash
python3 scrapingtool/scraper.py --categories "smartphones","headphones"
```


4. Database Manager:

* This module is responsible for constructing the Database Schema and performing all database related operations for inserting and updating records for all the tables involved in the scraping.

The main database tables are listed below:

* ProductListing: Listing table
* ProductDetails: Details table
* DailyProductListing: Listing table done on a daily basis
* QandA: QandAs
* Reviews: Reviews
* SentimentBreakdown: A summary of the positive and negative sentiments for each product
* SentimentAnalysis: The sentiment reviews table

There is no need to run this module separately, but it can still be used to fetch records in case a need arises to examine databases.


5. Subcategory Listing

* The subcategory listing file is located at `scrapingtool/subcategories.py`. This provides all the subcategories for all categories, along with the rules involved. Note that this file will consider fields ONLY from the `ProductDetails` table.

* The subcategories can be updated once a week / month, depending on the need. To start tue subcategory listing, you can run the below command:

```bash
bash run_subcategories.sh
```

This will take a couple of hours across all the categories.

After updating subcategories, you can start with the sentiment analysis module.


6. Sentiment Analysis

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

The complete sequence of commands for sentiment analysis is given below: (Here, the program will create a subdirectory called `data` for inserting all data specific to sentiment analysis)

This will analyze reviews for the period from August 1 to November 1 for all available categories, and insert the sentiment reviews + breakdown onto the database. The corresponding tables are at *SentimentAnalysis* and *SentimentBreakdown*

```bash
python3 scrapingtool/sentiment_analysis.py --category "all" --start_date "2020-08-01" --end_date "2020-11-01"
python3 scrapingtool/db_manager.py --insert_sentiment_breakdown --filename "data/sentiment_counts_all.pkl"
python3 scrapingtool/db_manager.py --insert_sentiment_reviews --filename "data/sentiment_db_all.csv"
```

If you want to run the sentiment analysis from the last review ID which was analyzed previously, from the `SentimentAnalysis` table, you can the this command:

```bash
python3 scrapingtool/sentiment_analysis.py --category "all" --start_date "2020-08-01" --end_date "2020-11-01" --last_review
```

For reference, a template script has been provided at `run_sentiments.sh`. Modify and run it according to the need (based on the dates / category).
The files will be of the format (where `CATEGORY` is the category provided - it can also be 'all'): `data/sentiment_counts_CATEGORY.pkl` and `data/sentiment_db_CATEGORY.csv`. One is a pickle file and another is a csv file.

**********************


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

*************