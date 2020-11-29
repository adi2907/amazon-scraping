#!/bin/bash

gunicorn3 --bind=0.0.0.0:8001 core.wsgi:application