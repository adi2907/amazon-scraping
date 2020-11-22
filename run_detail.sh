#!/bin/bash

#python3 awstool/api.py --terminate_instances --filename "created_instance_ids.txt"
#python3 awstool/api.py --reset_state
python3 awstool/api.py --create_instance --num_instances 10

sleep 300 # Wait for sometime for the instances to activate so that we can get the DNS

python3 awstool/api.py --get_created_instance_details

fab setup
fab setup-proxy
fab setup-detail
fab start-detail