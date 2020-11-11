#!/bin/bash

python3 awstool/api.py --terminate_instances --filename "created_instance_ids.txt"
python3 awstool/api.py --reset_state
python3 awstool/api.py --create_instance --num_instances 8
python3 awstool/api.py --get_created_instance_details
fab setup