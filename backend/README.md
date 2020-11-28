# Backend for the Dashboard

This is the backend repository for the dashboard, where all the APIs are in-place.

The backend takes in the already scraped data from the main database, and aggregates the product and review summary into a secondary database backend. This backend can be set on `settings.py`, but for convenience, it is assumed to be a simple Sqlite backend.

# Environment Setup

The follow steps assume that you are located at the `almetech/python-scraping/backend` directory, where the backend lies.

To run the environment setup, simply run the below command:

```bash
bash setup.sh
```

The next step would be to test run the development server, to ensure that all dependencies are installed, and the server starts:

```bash
python3 manage.py runserver
```

If the test run is working, then the setup is complete.

## Run the aggregation

Typically, when updating the API database, an aggregation step is required. You can run the aggregation using the below command:

```bash
bash aggregate.sh
```

*****************************

## Start the server

1. Development Mode

```bash
python3 manage.py runserver
```

2. Production Mode

Collect Static Files

```bash
python3 manage.py collectstatic
```

Deploy

```bash
sudo bash deploy.sh
```

*****************************

## API Urls

Base Endpoint is assumed to be:

`SERVER_HOST/api/dashboard/`

1. Brand Listing API: GET `brandlist/<str:category>`
* GET: Lists all the brands for a category

2. Model Listing API: GET `modellist/<str:category>/<str:brand>`
* GET: Lists all the models per brand for a category

3. Brand and Model List API: GET `brand-model/<str:category>`
* GET: Lists brands + models for a category

4. Fetch Subcategories API: GET `fetchsubcategories/<str:category>`
* GET: Fetches all subcategories for a category
Query Params:
* `subcategory` (Optional): Subcategory Name (Can be "Price" / "Feature", etc)

5. Cumulative Model Marketshare API: GET `modelmarketshare/<str:category>/<int:period>/<int:max_products>`
* `period`: Month number
* `max_products`: Top k products

Query Params:
* `brand`: Brand
* `subcategory` (Optional): Subcategory

6. Subcategory Marketshare API: GET `subcategorymarketshare/<str:category>/<str:subcategory>/<int:period>/<int:max_products>`
* `subcategory`: Subcategory Name

7. Brand Marketshare API: GET `brandmarketshare/<str:category>/<int:period>/<int:max_products>`
Query Params:
* `subcategory` (Optional): Subcategory

8. Individual Marketshare API: POST `individualmarketshare`
Request Payload:
* `subcategory` (Optional)
* `model`
* `max_products` (Default = 10)
* `period` (Month)

9. Ratings over Time API: GET `rating/<str:category>`
Query Params:
* `brand`: Brand
* `subcategory` (Optional): Subcategory
* `weeks` (default = 2): Last N weeks from today 

10. Review Count API: GET `review-count/<str:category>`
Query Params:
* `brand`: Brand
* `subcategory` (Optional): Subcategory

11. Aspect Rating API: GET `aspect-rating/<str:category>`
Query Params:
* `brand`: Brand
* `subcategory` (Optional): Subcategory

12. Feature List API (Sentiments): GET `featurelist/<str:category>`
Query Params:
* `brand`: Brand
* `subcategory` (Optional): Subcategory

13. Review breakdown API (Sentiments): GET `review-breakdown/<str:category>`
Query Params:
* `brand`: Brand
* `subcategory` (Optional): Subcategory

14. Fetch Reviews API (Sentiments): GET `fetch-reviews/<str:category>`
Query Params:
* `model`: Model
* `product_id`: Product ID (Atleast one of model / product_id needs to be sent)
* `sentiment_type`: "pos" or "neg"
* `feature`: Feature

*****************************