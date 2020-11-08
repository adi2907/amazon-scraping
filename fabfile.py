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
            conn_params.append(INSTANCE_USERNAME + '@' + line.strip())
    
    #if conn_params == []:
    #    conn_params.append('ubuntu' + '@' + 'ec2-65-0-105-15.ap-south-1.compute.amazonaws.com')

    upgrade_response = Responder(
        pattern=r'What would you like to do about menu.lst\?',
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
    for conn in ctx.CONNS:
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
                        result = conn.run(cmd, watchers=[Responder(pattern='Are you sure you want to continue connecting \(yes/no\)\?', response='yes\n')])
                    else:
                        result = conn.run(cmd)
                print(result, result.exited)

        #conn.run('touch test.txt')
        #conn.run('echo "HELLO WORLD" > test.txt')
        #result = conn.run('cat test.txt')
        #print(result, result.exited)
        #result = conn.sudo('service tor status')
        #print(result, result.exited)
