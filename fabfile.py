from decouple import config
from fabric import SerialGroup, task
import os

@task
def test(ctx):
    if not os.path.exists('active_instances.txt'):
        raise ValueError(f"Please list the active instances on active_instances.txt. Run `python awstool/api.py --fetch_instances` to dump the currently active instances")
    
    conn_params = []
    INSTANCE_USERNAME = 'ubuntu'
    
    with open('active_instances.txt', 'r') as f:
        for line in f:
            conn_params.append(INSTANCE_USERNAME + '@' + line.strip())

    conns = SerialGroup(
        *(conn_params),
        connect_kwargs=
        {
            'key_filename': config('SSH_KEY_FILE'),
        })
    ctx.CONNS = conns
    for conn in ctx.CONNS:
        # Add the SSH key from `aws_key.pem` (the template permission file)
        with open('aws_[rivate_key.pem', 'r') as f:
            template_key = f.read().strip()
        conn.run(f'echo "{template_key}" >> ~/.ssh/id_rsa')
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
