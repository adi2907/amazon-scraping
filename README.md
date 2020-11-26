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

### The scraper library

More information can be found [here](scrapingtool/README.md)


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

*****************

### Automatic access over AWS

This module is for automatic EC2 instance access to provide smooth instance management in an automated fashion.

AWS specfic API details [here](awstool/README.md)

However, you need to setup some other dependencies in order to automate the task management. Refer to the below section for more.

***********************

### Automatic SSH access to the EC2 instances

To automate the ssh control of the ec2 instances for running specific commands, we use the *fabric* library, which is a SSH client in Python, that can be used to run commands across multiple instances.

If you haven't done so already, you can install this library using `pip`

```bash
pip3 install fabric
```


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

## Dashboard

## Backend

Backend specific information is located [here](backend/README.md)