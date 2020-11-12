import os

from decouple import config
from fabric import SerialGroup, task
from invoke import Responder


@task
def setup(ctx):
    if not os.path.exists('active_instances.txt'):
        raise ValueError(f"Please list the active instances on active_instances.txt. Run `python awstool/api.py --fetch_instances` to dump the currently active instances")
    
    if not os.path.exists('aws_private_key.pem'):
        raise ValueError(f"Please get the private key template at aws_private_key.pem")

    conn_params = []
    INSTANCE_USERNAME = 'ubuntu'
    
    with open('active_instances.txt', 'r') as f:
        for line in f:
            text = line.strip()
            if text not in ['', None]:
                conn_params.append(INSTANCE_USERNAME + '@' + text)
    
    num_instances = len(conn_params)
    
    #if conn_params == []:
    #    conn_params.append('ubuntu' + '@' + 'ec2-65-0-173-241.ap-south-1.compute.amazonaws.com')

    upgrade_response = Responder(
        pattern=r'What would you like to do about menu\.lst\?',
        response='2\n',
    )

    conns = SerialGroup(
        *(conn_params),
        connect_kwargs=
        {
            'key_filename': config('SSH_KEY_FILE'),
        },
        )
    ctx.CONNS = conns
    instance_number = 0

    ITEMS_PER_INSTANCE = 100 # 100 archived products per instance

    for _, conn in enumerate(ctx.CONNS):
        # Add the SSH key from `aws_key.pem` (the template permission file)
        with open('aws_private_key.pem', 'r') as f:
            template_key = f.read().strip()
        conn.run(f'echo "{template_key}" > ~/.ssh/id_rsa')
        
        with open('setup.sh', 'r') as f:
            for idx, line in enumerate(f):
                cmd = line.strip()
                if idx in [0, 1]:
                    result = conn.run(cmd, watchers=[upgrade_response])
                else:
                    if cmd.startswith('git'):
                        result = conn.run(cmd, watchers=[Responder(pattern=r'Are you sure you want to continue connecting \(yes/no\)\?', response='yes\n')])
                    else:
                        result = conn.run(cmd)
                print(result, result.exited)
        
        # Copy .env file
        with open('.env', 'r') as f:
            environment = f.read().strip()
        conn.run(f'echo "{environment}" > ~/python-scraping/.env')
        conn.run(f'echo "{environment}" > ~/python-scraping/spider/.env')

        # Now start
        conn.run("tmux new -d -s cron")
        command = f"scrapy crawl archive_details_spider -a category='headphones' -a start_idx={instance_number * ITEMS_PER_INSTANCE} -a end_idx={(instance_number + 1) * ITEMS_PER_INSTANCE} -o output.csv"
        #command = f'python3 scrapingtool/archive.py --process_archived_pids --categories "headphones" --instance_id {instance_number} --num_instances {num_instances} --num_threads 5'
        command = command.replace(' ', r'\ ')
        conn.run(r"tmux send -t cron.0 cd\ python-scraping ENTER")
        conn.run(r"tmux send -t cron.0 cd\ spider ENTER")
        conn.run(f"tmux send -t cron.0 {command} ENTER")

        conn.run("tmux new -d -s monitor")
        command = f'python3 scrapingtool/monitor.py'
        command = command.replace(' ', r'\ ')
        conn.run(r"tmux send -t monitor.0 cd\ python-scraping ENTER")
        conn.run(f"tmux send -t monitor.0 {command} ENTER")
        
        instance_number += 1
        # conn.run(f'cd python-scraping && python3 scrapingtool/archive.py --process_archived_pids --categories "headphones" --instance_id {idx} --num_instances {num_instances} --num_threads 5')

        #conn.run('touch test.txt')
        #conn.run('echo "HELLO WORLD" > test.txt')
        #result = conn.run('cat test.txt')
        #print(result, result.exited)
        #result = conn.sudo('service tor status')
        #print(result, result.exited)


@task
def terminate(ctx):
    if not os.path.exists('active_instances.txt'):
        raise ValueError(f"Please list the active instances on active_instances.txt. Run `python awstool/api.py --fetch_instances` to dump the currently active instances")
    
    if not os.path.exists('aws_private_key.pem'):
        raise ValueError(f"Please get the private key template at aws_private_key.pem")

    conn_params = []
    INSTANCE_USERNAME = 'ubuntu'
    
    with open('active_instances.txt', 'r') as f:
        for line in f:
            text = line.strip()
            if text not in ['', None]:
                conn_params.append(INSTANCE_USERNAME + '@' + text)
    
    num_instances = len(conn_params)

    upgrade_response = Responder(
        pattern=r'What would you like to do about menu\.lst\?',
        response='2\n',
    )

    conns = SerialGroup(
        *(conn_params),
        connect_kwargs=
        {
            'key_filename': config('SSH_KEY_FILE'),
        },
        )
    ctx.CONNS = conns
    instance_number = 0
    for _, conn in enumerate(ctx.CONNS):
        # Send it twice
        conn.run(r"tmux send -t cron.0 C-c")
        conn.run(r"tmux send -t cron.0 C-c")

        result = conn.sudo(r"shutdown")
        print(result, result.exited)
        
        instance_number += 1


@task
def retry(ctx):
    if not os.path.exists('active_instances.txt'):
        raise ValueError(f"Please list the active instances on active_instances.txt. Run `python awstool/api.py --fetch_instances` to dump the currently active instances")
    
    if not os.path.exists('aws_private_key.pem'):
        raise ValueError(f"Please get the private key template at aws_private_key.pem")

    conn_params = []
    INSTANCE_USERNAME = 'ubuntu'
    
    with open('active_instances.txt', 'r') as f:
        for line in f:
            text = line.strip()
            if text not in ['', None]:
                conn_params.append(INSTANCE_USERNAME + '@' + text)
    
    num_instances = len(conn_params)

    conns = SerialGroup(
        *(conn_params),
        connect_kwargs=
        {
            'key_filename': config('SSH_KEY_FILE'),
        },
        )
    ctx.CONNS = conns
    instance_number = 0
    for _, conn in enumerate(ctx.CONNS):
        # Send it twice
        conn.run(r"tmux send -t cron.0 C-c")
        conn.run(r"tmux send -t cron.0 C-c")

        command = f'python3 scrapingtool/archive.py --process_archived_pids --categories "headphones" --instance_id {instance_number} --num_instances {num_instances} --num_threads 5'
        command = command.replace(' ', r'\ ')
        conn.run(f"tmux send -t cron.0 {command} ENTER")
        
        instance_number += 1
