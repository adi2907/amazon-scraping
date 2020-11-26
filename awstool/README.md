### Automatic access over AWS

The archive product scraping needs multiple EC2 instances to run the scraping. To help with this, there is an automated mechanism to create, start and stop these instances using commands from the master EC2 server (Archive Controller instance).

The library `awstool` provides helper commands to directly access and use the AWS Api using Python.

*NOTE*: Here, we assume that the base directory is pointing towards the repository: `almetech/python-scraping`. Otherwise. the commands won't work.

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

8. Reset state (This will remove the files `active_instances.txt` and `created_instance_ids.txt`, so that you can start afresh. You should run this after terminating instances)

```bash
python3 awstool/api.py --reset_state
```

***********************
