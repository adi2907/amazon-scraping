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


### Cronjob Commands

Periodic commands can be run as a cronjob in different instances, based on the need.

The list of crontab commands can be viewed using the command:

```bash
crontab -e
```

The below will list down the various cronjobs required by different instances. There is an assumption that dedicated instances are running for the following tasks:

* Instance 1: Product Listing Scraping

* Instance 2: Product Detail Scraping

* Instance 3: Archive Product Scraping

Crontab commands for Listing Instance:

```bash
# Kills any previous tmux sessions, so that we can start afresh
30 6 * * * tmux kill-session -t bro
30 7 * * * cd /home/ubuntu/python-scraping && tmux new-session -d -s bro \; send-keys "python ./scrapingtool/browser.py category" Enter
```

This will ensure that the listing scraping is done everyday at 7:30 local time on the listing instance.

Example crontab commands for Detail Instance(s):

```bash
# Kills any previous tmux sessions, so that we can start afresh
30 7 * * 1 tmux kill-session -t bro

0 8 * * 1 cd /home/ubuntu/updated/python-scraping && tmux new-session -d -s bro \; send-keys "python scrapingtool/scraper.py --tor --categories \"headphones\" --override --listing --detail --no_listing --num_workers 5 --worker_pages \"41, 42, 43, 44, 45\"" Enter
```

For this instance, this crontab command will do the detail scraping for headphones every week. You can extend this to multiple categories / instances as well.

Crontab commands for Archive Controller Instance:

```bash
# Kills any previous tmux sessions, so that we can start afresh
30 11 * * * tmux kill-session -t bro

00 12 * * * cd /home/ubuntu/python-scraping && tmux new-session -d -s bro \; send-keys "bash start_archive_instances.sh" Enter

# Terminate once a week and recreate new instances
0 0 * * 4 tmux kill-session -t bro && cd /home/ubuntu/python-scraping && tmux new-session -d -s bro \; send-keys "bash create_instances.sh" Enter

# Update Proxy Lists
30 0 * * 4 tmux kill-session -t bro && cd /home/ubuntu/python-scraping && tmux new-session -d -s bro \; send-keys "fab setup-proxy && fab setup-detail" Enter

# Terminate again
30 1 * * 4 tmux kill-session -t bro && cd /home/ubuntu/python-scraping && tmux new-session -d -s bro \; send-keys "fab terminate" Enter
```

### Automatic access over AWS

The archive product scraping needs multiple EC2 instances to run the scraping. To help with this, there is an automated mechanism to create, start and stop these instances using commands from the master EC2 server (Archive Controller instance).

The library `awstool` provides helper commands to directly access and use the AWS Api using Python.

List of commands for AWS:

1. View all AWS instances in a pretty printed format:

```bash
python3 awstool/api.py --pretty_print_instances
```

2. List all currently running instances, in a minimal fashion

```bash
python3 awstool/api.py --fetch_active_instances
```

3. Create `N` new instances (based on Ubuntu 20.04) and will add the new instance ids to `created_instance_ids.txt`

```bash
python3 awstool/api.py --create_instance --num_instances N
```

4. Populate the files and `active_instances.txt` with the Instance IP address. This *needs* to be run after creating the instances.

*NOTE*: However, you may need to wait for 1-2 mins for the instances to start up before running this command, since the IPs will be assigned only after it starts running:

```bash
python3 awstool/api.py --get_created_instance_details
```

5. Start instances

```bash
python3 awstool/api.py --start_instances --instance_ids "id1, id2"

python3 awstool/api.py --start_instances --filename "created_instance_ids.txt"
```

6. Stop instances

```bash
python3 awstool/api.py --stop_instances --instance_ids "id1, id2"

python3 awstool/api.py --stop_instances --filename "created_instance_ids.txt"
```

7. Terminate instances

```bash
python3 awstool/api.py --terminate_instances --instance_ids "id1, id2"

python3 awstool/api.py --terminate_instances --filename "created_instance_ids.txt"
```

### Automatic SSH access to the EC2 instances

To automate the ssh control of the ec2 instances for running specific commands, we use the *fabric* library, which is a SSH client in Python, that can be used to run commands across multiple instances.


#### Prerequisites
1. To connect to SSH, we need the AWS public keys for accessing all those instances, which belong to the same security group. You need to place that ssh key in `aws_private_key.pem`.
2. To allow the instances to access this Github repository, the Github public key also needs to be shared and placed in `aws_public_key.pem`.

This library uses a custom file called `fabfile.py`, which will contain all the tasks needed to run commands across the instances.

The general format for a fabric task is like this:

```python
# fabfile.py
@task
def my_custom_task(ctx):
    pass
```

We can run this fabric task using the below command:

```bash
fab my-custom-task
```

Notice that the underscore is replaced with a - in the task name.


Now, there are 5 main tasks for the instances:

1. The `setup` task (needs to be run to setup the system)
2. The `start-archive` task (starts archive detail scraping across all active instances)
3. The `terminate` task (Shuts down all the archive instances)

4. The `setup-proxy` task (Sets up the proxy service `tinyproxy` for relevant instances)
5. The `setup-detail` task (Used to copy the proxy IPs to the detail server)


The archive tasks will be generally run in this fashion:

```bash
fab setup
fab start-archive
```

***********