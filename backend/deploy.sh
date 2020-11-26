#!/bin/bash

set -e

LOGFILE=/home/ubuntu/python-scraping/backend/logs/gunicorn.log

LOGDIR=$(dirname $LOGFILE)
NUM_WORKERS=3

USER=root

GROUP=root

cd /home/ubuntu/python-scraping/backend

test -d $LOGDIR || mkdir -p $LOGDIR

# gunicorn3 -w $NUM_WORKERS --user=$USER --group=$GROUP --log-level=debug --log-file=$LOGFILE
gunicorn3 --bind 0.0.0.0:8000 amazonscraper.wsgi:application -w $NUM_WORKERS --log-level=debug --log-file=$LOGFILE

# https
#sudo gunicorn3 --certfile=server.crt --keyfile=server.key --bind 0.0.0.0:443 amazonscraper.wsgi:application -w 1 --log-level=debug
