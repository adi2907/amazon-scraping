import os

from decouple import config
from fabric import SerialGroup, task
from invoke import Responder
from scrapingtool.utils import listing_categories


@task
def setup(ctx):
    if not os.path.exists('active_instances.txt'):
        raise ValueError(f"Please list the active instances on active_instances.txt. Run `python awstool/api.py --fetch_instances` to dump the currently active instances")
    
    if not os.path.exists('aws_private_key.pem'):
        raise ValueError(f"Please get the private key template at aws_private_key.pem")

    if not os.path.exists('allowed_hosts.txt'):
        raise ValueError(f"Please provide allowed_hosts.txt file")

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
        
        instance_number += 1


@task
def start_archive(ctx):
    if not os.path.exists('active_instances.txt'):
        raise ValueError(f"Please list the active instances on active_instances.txt. Run `python awstool/api.py --fetch_instances` to dump the currently active instances")
    
    if not os.path.exists('aws_private_key.pem'):
        raise ValueError(f"Please get the private key template at aws_private_key.pem")

    if not os.path.exists('allowed_hosts.txt'):
        raise ValueError(f"Please provide allowed_hosts.txt file")

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
        command = f"scrapy crawl archive_details_spider -a category='all' -a instance_id={instance_number} -a start_idx={instance_number * ITEMS_PER_INSTANCE} -a end_idx={(instance_number + 1) * ITEMS_PER_INSTANCE} -o output.csv"
        #command = f'python3 scrapingtool/archive.py --process_archived_pids --categories "headphones" --instance_id {instance_number} --num_instances {num_instances} --num_threads 5'
        command = command.replace(' ', r'\ ')
        conn.run(r"tmux send -t cron.0 cd\ python-scraping ENTER")
        conn.run(r"tmux send -t cron.0 cd\ spider ENTER")
        conn.run(f"tmux send -t cron.0 {command} ENTER")

        conn.run("tmux new -d -s controller")
        command = f'python3 scrapingtool/controller.py --instance_id {instance_number}'
        command = command.replace(' ', r'\ ')
        conn.run(r"tmux send -t controller.0 cd\ python-scraping ENTER")
        conn.run(f"tmux send -t controller.0 {command} ENTER")
        
        instance_number += 1


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
        try:
            conn.run(r"tmux send -t cron.0 C-c")
            conn.run(r"tmux send -t cron.0 C-c")
        except Exception as ex:
            print(ex)

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


@task
def setup_detail(ctx):
    if not os.path.exists('allowed_hosts.txt'):
        raise ValueError(f"Please list the allowed_hosts.txt")

    conn_params = []
    proxies = []
    
    INSTANCE_USERNAME = 'ubuntu'

    with open('allowed_hosts.txt', 'r') as f:
        for line in f:
            ip_address = line.strip()
            conn_params.append(INSTANCE_USERNAME + '@' + ip_address)
    
    with open('active_instances.txt', 'r') as f:
        for line in f:
            ip_address = line.strip().split('.')[0]
            if ip_address.startswith('ec2-'):
                ip_address = ip_address[4:].replace('-', '.')
                conn_params.append(INSTANCE_USERNAME + '@' + ip_address)
            else:
                ip_address = line.strip()
            proxies.append(ip_address + ':8888')
    
    proxy_list = ''

    for proxy in proxies:
        if proxy_list != '':
            proxy_list += '\n'
        proxy_list += proxy
    
    print(proxy_list)
    
    conns = SerialGroup(
        *(conn_params),
        connect_kwargs=
        {
            'key_filename': config('SSH_KEY_FILE'),
        },
        )
    ctx.CONNS = conns
    for _, conn in enumerate(ctx.CONNS):
        try:
            conn.run(f'echo "{proxy_list}" > ~/updated/python-scraping/proxy_list.txt')
        except:
            conn.run(f'echo "{proxy_list}" > ~/python-scraping/proxy_list.txt')


@task
def setup_proxy(ctx):
    if not os.path.exists('active_instances.txt'):
        raise ValueError(f"Please list the active instances on active_instances.txt. Run `python awstool/api.py --fetch_instances` to dump the currently active instances")
    
    if not os.path.exists('aws_private_key.pem'):
        raise ValueError(f"Please get the private key template at aws_private_key.pem")

    if not os.path.exists('allowed_hosts.txt'):
        raise ValueError(f"Please provide allowed_hosts.txt file")

    conn_params = []
    instance_ips = []
    INSTANCE_USERNAME = 'ubuntu'
    
    with open('active_instances.txt', 'r') as f:
        for line in f:
            text = line.strip()
            if text not in ['', None]:
                conn_params.append(INSTANCE_USERNAME + '@' + text)
            ip_address = line.strip().split('.')[0]
            if ip_address.startswith('ec2-'):
                ip_address = ip_address[4:].replace('-', '.')
            else:
                ip_address = line.strip()
            instance_ips.append(ip_address)
    
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

    upgrade_response = Responder(
        pattern=r'What would you like to do about menu\.lst\?',
        response='2\n',
    )

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

        with open('allowed_hosts.txt', 'r') as f:
            for line in f:
                ip_address = line.strip()
                result = conn.sudo(f'echo -e "\nAllow {ip_address}\n" | sudo tee -a /etc/tinyproxy/tinyproxy.conf')
                print(result)
            
            for ip in instance_ips:
                ip = ip.strip()
                result = conn.sudo(f'echo -e "\nAllow {ip}\n" | sudo tee -a /etc/tinyproxy/tinyproxy.conf')
                print(result)
        
        result = conn.sudo(f'sudo service tinyproxy restart')
        print(result)

@task
def start_detail(ctx):
    if not os.path.exists('active_instances.txt'):
        raise ValueError(f"Please list the active instances on active_instances.txt. Run `python awstool/api.py --fetch_instances` to dump the currently active instances")
    
    if not os.path.exists('aws_private_key.pem'):
        raise ValueError(f"Please get the private key template at aws_private_key.pem")

    if not os.path.exists('allowed_hosts.txt'):
        raise ValueError(f"Please provide allowed_hosts.txt file")

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

    categories = listing_categories
    
    INSTANCES_PER_CATEGORY = 2

    for idx, conn in enumerate(ctx.CONNS):
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


        category = categories[instance_number % len(categories)]
        
        conn.run(f'echo "{category}" > ~/python-scraping/categories.txt')

        # Now start
        try:
            conn.run('tmux kill-session -t cron')
        except:
            pass
        try:
            conn.run('tmux kill-session -t controller')
        except:
            pass
        conn.run("tmux new -d -s cron")
        command = f'python3 scrapingtool/scraper.py --categories_file categories.txt --override --listing --detail --no_listing --num_workers 5 --instance_id {instance_number}'
        #command = f'python3 scrapingtool/scraper.py --categories "{category}" --override --listing --detail --no_listing --num_workers 5 --worker_pages "61, 62, 63, 64, 65" --instance_id {instance_number}'
        command = command.replace(' ', r'\ ')
        conn.run(r"tmux send -t cron.0 cd\ python-scraping ENTER")
        conn.run(f"tmux send -t cron.0 {command} ENTER")

        conn.run("tmux new -d -s controller")
        command = f'python3 scrapingtool/controller.py --instance_id {instance_number}'
        command = command.replace(' ', r'\ ')
        conn.run(r"tmux send -t controller.0 cd\ python-scraping ENTER")
        conn.run(f"tmux send -t controller.0 {command} ENTER")
        
        instance_number += 1


@task
def post_detail(ctx):
    if not os.path.exists('active_instances.txt'):
        raise ValueError(f"Please list the active instances on active_instances.txt. Run `python awstool/api.py --fetch_instances` to dump the currently active instances")
    
    if not os.path.exists('aws_private_key.pem'):
        raise ValueError(f"Please get the private key template at aws_private_key.pem")

    if not os.path.exists('allowed_hosts.txt'):
        raise ValueError(f"Please provide allowed_hosts.txt file")

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

    categories = listing_categories
    
    INSTANCES_PER_CATEGORY = 2

    for idx, conn in enumerate(ctx.CONNS):
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


        category = categories[instance_number % len(categories)]
        
        conn.run(f'echo "{category}" > ~/python-scraping/categories.txt')

        # Now start
        try:
            conn.run('tmux kill-session -t cron')
        except:
            pass
        try:
            conn.run('tmux kill-session -t controller')
        except:
            pass
        conn.run("tmux new -d -s cron")
        command = f'bash post_detail_scraping.sh'
        command = command.replace(' ', r'\ ')
        conn.run(r"tmux send -t cron.0 cd\ python-scraping ENTER")
        conn.run(f"tmux send -t cron.0 {command} ENTER")
        
        instance_number += 1
