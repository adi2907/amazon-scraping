import argparse

import boto3
from decouple import config
from termcolor import colored


def start_session(region="ap-south-1"):
    session = boto3.Session(region_name=region)
    ec2 = session.resource('ec2')
    return session, ec2


def fetch_instances(ec2, filters=[]):
    instances = ec2.instances.filter(Filters=filters)

    for instance in instances:
        print(instance.id, instance.instance_type)


def pretty_print_instances(ec2):
    for i in ec2.instances.all():

        print("Id: {0}\tState: {1}\tLaunched: {2}\tRoot Device Name: {3}".format(
            colored(i.id, 'cyan'),
            colored(i.state['Name'], 'green'),
            colored(i.launch_time, 'cyan'),
            colored(i.root_device_name, 'cyan')
        ))

        print("\tArch: {0}\tHypervisor: {1}".format(
            colored(i.architecture, 'cyan'),
            colored(i.hypervisor, 'cyan')
        ))

        print("\tPriv. IP: {0}\tPub. IP: {1}".format(
            colored(i.private_ip_address, 'red'),
            colored(i.public_ip_address, 'green')
        ))

        print("\tPriv. DNS: {0}\tPub. DNS: {1}".format(
            colored(i.private_dns_name, 'red'),
            colored(i.public_dns_name, 'green')
        ))

        print("\tSubnet: {0}\tSubnet Id: {1}".format(
            colored(i.subnet, 'cyan'),
            colored(i.subnet_id, 'cyan')
        ))

        print("\tKernel: {0}\tInstance Type: {1}".format(
            colored(i.kernel_id, 'cyan'),
            colored(i.instance_type, 'cyan')
        ))

        print("\tRAM Disk Id: {0}\tAMI Id: {1}\tPlatform: {2}\t EBS Optimized: {3}".format(
            colored(i.ramdisk_id, 'cyan'),
            colored(i.image_id, 'cyan'),
            colored(i.platform, 'cyan'),
            colored(i.ebs_optimized, 'cyan')
        ))

        print("\tBlock Device Mappings:")
        for idx, dev in enumerate(i.block_device_mappings, start=1):
            print("\t- [{0}] Device Name: {1}\tVol Id: {2}\tStatus: {3}\tDeleteOnTermination: {4}\tAttachTime: {5}".format(
                idx,
                colored(dev['DeviceName'], 'cyan'),
                colored(dev['Ebs']['VolumeId'], 'cyan'),
                colored(dev['Ebs']['Status'], 'green'),
                colored(dev['Ebs']['DeleteOnTermination'], 'cyan'),
                colored(dev['Ebs']['AttachTime'], 'cyan')
            ))

        try:
            print("\tTags:")
            for idx, tag in enumerate(i.tags, start=1):
                print("\t- [{0}] Key: {1}\tValue: {2}".format(
                    idx,
                    colored(tag['Key'], 'cyan'),
                    colored(tag['Value'], 'cyan')
                ))
        except:
            pass

        print("\tProduct codes:")
        for idx, details in enumerate(i.product_codes, start=1):
            print("\t- [{0}] Id: {1}\tType: {2}".format(
                idx,
                colored(details['ProductCodeId'], 'cyan'),
                colored(details['ProductCodeType'], 'cyan')
            ))

        print("Console Output:")
        # Commented out because this creates a lot of clutter..
        # print(i.console_output()['Output'])

        print("--------------------")


def create_instance(ec2, security_group_id, volume_size=64, image_id='ami-0cda377a1b884a1bc', instance_type='t2.medium', num_instances=1):
    '''
        volume_size (GB)
        instance_type
        num_instances
        security_group_id (Necessary)
    '''
    response = ec2.create_instances(ImageId=image_id, MinCount=1, MaxCount=num_instances, InstanceType=instance_type,
        BlockDeviceMappings=[
            {
                'DeviceName': '/dev/xvda',
                'Ebs': {
                    'DeleteOnTermination': True,
                    'VolumeSize': volume_size,
                    'VolumeType': 'gp2'
                },
            },
        ],
        Monitoring={
            'Enabled': False
        },
        SecurityGroupIds=[
            security_group_id,
        ],
    )
    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fetch_instances', help='Fetch all instances', default=False, action='store_true')
    parser.add_argument('--pretty_print_instances', help='Pretty Print all instances', default=False, action='store_true')
    parser.add_argument('--create_instance', help='Creates a new EC2 instance', default=False, action='store_true')

    args = parser.parse_args()

    _fetch_instances = args.fetch_instances
    _pretty_print_instances = args.pretty_print_instances
    _create_instance = args.create_instance

    if _fetch_instances == True:
        _, ec2 = start_session()
        fetch_instances(ec2)
    if _pretty_print_instances == True:
        _, ec2 = start_session()
        pretty_print_instances(ec2)
    if _create_instance == True:
        _, ec2 = start_session()
        response = create_instance(ec2, config('SECURITY_GROUP_ID'), volume_size=64, image_id=config('INSTANCE_AMI'), num_instances=1)
        print(f"{response}")