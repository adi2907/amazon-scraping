import os

from decouple import config
from fabric import SerialGroup, task


def setup_git_ssh(conn):
    CLONE_COMMAND = 'git clone git@github.com:almetech/python-scraping.git'
    commands = [
        'chmod 600 ~/.ssh/id_rsa',
        'ssh -o StrictHostKeyChecking=no git@github.com',
        CLONE_COMMAND,
    ]
    for command in commands:
        result = conn.run(command)
        print(result, result.exited)


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
            conn_params.append(INSTANCE_USERNAME + '@' + line.strip())
    
    #if conn_params == []:
    #    conn_params.append('ubuntu' + '@' + 'ec2-65-0-105-15.ap-south-1.compute.amazonaws.com')

    conns = SerialGroup(
        *(conn_params),
        connect_kwargs=
        {
            'key_filename': config('SSH_KEY_FILE'),
        },
        )
    ctx.CONNS = conns
    for conn in ctx.CONNS:
        # Add the SSH key from `aws_key.pem` (the template permission file)
        with open('aws_private_key.pem', 'r') as f:
            template_key = f.read().strip()
        conn.run(f'echo "{template_key}" > ~/.ssh/id_rsa')
        
        setup_git_ssh(conn)
        
        with open('setup.sh', 'r') as f:
            for line in f:
                cmd = line.strip()
                result = conn.run(cmd)
                print(result, result.exited)

        #conn.run('touch test.txt')
        #conn.run('echo "HELLO WORLD" > test.txt')
        #result = conn.run('cat test.txt')
        #print(result, result.exited)
        #result = conn.sudo('service tor status')
        #print(result, result.exited)
