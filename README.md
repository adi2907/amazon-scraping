# python-scraping

This is the repo for scraping online websites using python -list, category pages

**********

## Requirements

To install the requirements for this project, run the following:

```bash
pip install -r requirements.txt
```

************

## Running the Script

After you've installed your dependencies, you can run the program in the following manner:

```bash
python script.py --categories "mobile, headphones" --listing --pages 1 --detail --number 2
```

Here, the `--categories` flag represents all the categories that you need to scrape. The program will scrape the take both the categories `["mobile", "headphone"]` and get the listing details for both these categories.

*NOTE*: By default, _all_ the reviews and QandAs for a product will be scraped. For more details, refer below.

If you only want to scrape the product listing, you can do it like this:

```bash
python script.py --categories "mobile, headphones" --listing --pages 2
```

This will scrape the first 2 pages for the product listing of mobiles and headphones.

The `--detail` flag is an optional flag if you want to fetch the product details.

To scrape only the product details, you can run it like this:

```bash
python script.py --categories "mobile, headphones" --details --number 2
```

This will fetch the first 2 product details for each category.

To scrape both the listing and details, you can run it like this:

```bash
python script.py --categories "mobile, headphones" --listing --pages 2 --detail --number 2
```

This will fetch the first 2 pages for product listing, and also scrape the first 2 products for each listing category.

## Scraping the QandA and Review Pages

The following options will restrict the scraper to scrape only until a certain number of pages, for the QandA, Reviews section.

```bash
python script.py --categories "headphones" --listing --pages 1 --detail --number 2 --review_pages 1 --qanda_pages 2
```

Here, the scraper will scrape 2 headphone products, and only fetch 1 review page. It will fetch the first 2 QandA pages.

***********