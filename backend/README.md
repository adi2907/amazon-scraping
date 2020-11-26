# Backend for the Dashboard

This is the backend repository for the dashboard, where all the APIs are in-place.

The backend takes in the already scraped data from the main database, and aggregates the product and review summary into a secondary database backend. This backend can be set on `settings.py`, but for convenience, it is assumed to be a simple Sqlite backend.

# Environment Setup

The follow steps assume that you are located at the `almetech/python-scraping/backend` directory, where the backend lies.

* First, ensure that you copy the `.env` from the main folder to `amazonscraper/.env`.

```bash
cp ../.env amazonscraper/.env
```

* Then, install all django requirements using:

```bash
pip3 install -r requirements.txt
```

Collect all static files, using the help of whitenoise:

```bash
python3 manage.py collectstatic
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
sudo ./deploy.sh
```

*****************************