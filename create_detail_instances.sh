#!/bin/bash

python3 awstool/api.py --terminate_instances --filename "created_instance_ids.txt"
python3 awstool/api.py --reset_state
python3 awstool/api.py --create_instance --filename "num_categories.txt" --num_instances 5 # 5 is default, incase num_inactive.txt doesn't exist

#python3 awstool/api.py --create_instance --num_instances 8

sleep 300 # Wait for sometime for the instances to activate so that we can get the DNS

python3 awstool/api.py --get_created_instance_details

cp created_instance_ids.txt detail_instance_ids.txt

fab setup
fab setup-proxy
fab setup-detail