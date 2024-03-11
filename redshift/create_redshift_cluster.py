# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-redshift-home.html

import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")
vpc_id = os.getenv("AWS_VPC_ID")
redshift_port = int(os.getenv("REDSHIFT_PORT"))
sc_ip_range = os.getenv("SC_IP_RANGE")


security_group_name = "my-redshift-security-group"
security_group_desc = "Security group for redshift cluster access"
redshift_subnet_group_name = 'redshift-subnet-group'
redshift_cluster_name = "my-redshift-cluster"
redshift_role_name = "redshift-access-s3-role"

ec2_client = boto3.client(
    'ec2',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

redshift_client = boto3.client(
    'redshift', 
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

iam_client = boto3.client(
    'iam',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)


def create_redshift_security_group():
    try:
        response = ec2_client.create_security_group(
            Description=security_group_desc,
            GroupName=security_group_name,
            VpcId=vpc_id
        )
        security_group_id = response['GroupId']
        print("Created security group with ID: ", security_group_id)

        return security_group_id
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
            # The security group already exists
            response = ec2_client.describe_security_groups(
                Filters=[
                    {'Name': 'group-name', 'Values': [security_group_name]},
                    {'Name': 'vpc-id', 'Values': [vpc_id]}
                ]
            )
            security_group_id = response['SecurityGroups'][0]['GroupId']
            print('Security group already exists. Using existing security group with ID:', security_group_id)

            return security_group_id
        else:
            # Handle other exceptions
            print('Error creating security group:', e)

def add_inbound_rule(security_group_id):
    try:
        # Add the inbound rule to the security group
        response = ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': redshift_port,
                    'ToPort': redshift_port,
                    'IpRanges': [{'CidrIp': sc_ip_range}]
                }
            ]
        )
        print('Inbound rule added to the security group.')
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print('Inbound rule already exists for the specified port and IP range.')
        else:
            print('Error adding inbound rule:', e)
    except Exception as e:
        print("Exception occured while adding inbound rule!", e)

def add_self_reference_inbound_rule(security_group_id):
    try:
        response = ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 0,  # Set FromPort to 0 for all TCP traffic
                    'ToPort': 65535,  # Set ToPort to 65535 for all TCP traffic
                    'UserIdGroupPairs': [{'GroupId': security_group_id}]
                }
            ]
        )
        print('Inbound rule added to the security group.')
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print('Inbound rule already exists for the specified port and IP range.')
        else:
            print('Error adding inbound rule:', e)


def check_subnet_and_vpc():
    response = ec2_client.describe_subnets()
    subnet_ids = []

    for subnet in response['Subnets']:
        subnet_id = subnet['SubnetId']
        vpc_id = subnet['VpcId']
        cidr_block = subnet['CidrBlock']
        availability_zone = subnet['AvailabilityZone']

        subnet_ids.append(subnet_id)

        print(f"Subnet ID: {subnet_id}")
        print(f"VPC ID: {vpc_id}")
        print(f"CIDR Block: {cidr_block}")
        print(f"Availability Zone: {availability_zone}")
        print("------------------------")

    return subnet_ids


# The create_cluster_subnet_group operation in Amazon Redshift is used to create a subnet group that 
# represents a group of subnets. When creating a Redshift cluster, 
# you are required to associate the cluster with a subnet group.

# Deploying a cluster in multiple subnets allows you to distribute the cluster across 
# different availability zones (AZs). 

# If you have data sources or clients in different regions or AZs, placing the cluster in multiple subnets closer 
# to those data sources or clients can help reduce data transfer costs.

def create_redshift_subnet_group(subnet_ids):

    try:
        # Create the subnet group
        response = redshift_client.create_cluster_subnet_group(
            ClusterSubnetGroupName=redshift_subnet_group_name,
            Description='My subnet group for redshift cluster',
            SubnetIds=subnet_ids
        )

        print(f'Redshift Subnet group: {redshift_subnet_group_name} created successfully.')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterSubnetGroupAlreadyExists':
            print('Subnet group already exists. Skipping creation.')
        else:
            print('Error creating subnet group:', e)

def get_route_table_by_vpc():
    try:
        response = ec2_client.describe_route_tables(
            Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
        )
        main_route_table_id = response['RouteTables'][0]['RouteTableId']
        return main_route_table_id
    except Exception as e:
        print('Failed to get route table', e)

def create_s3_vpc_endpoint():
    try:
        route_table_id = get_route_table_by_vpc()
        print("======== ", route_table_id)
    
        ec2_client.create_vpc_endpoint(
            VpcEndpointType="Gateway",
            VpcId=vpc_id,
            ServiceName="com.amazonaws.us-east-1.s3",
            RouteTableIds=[route_table_id]
        )
    except Exception as e:
        print('Failed to create s3 vpc endpoint', e)

def create_cluster(security_group_id):
    # Define the cluster parameters used to create cluster in redshift
    cluster_parameters = {
        'ClusterIdentifier': redshift_cluster_name,
        'NodeType': 'dc2.large',
        'MasterUsername': os.getenv("REDSHIFT_USERNAME"),
        'MasterUserPassword': os.getenv("REDSHIFT_PASSWORD"),
        'DBName': 'mydatabase',
        'ClusterType': 'single-node',
        'NumberOfNodes': 1,
        'PubliclyAccessible': True,
        'VpcSecurityGroupIds': [security_group_id], 
        'AvailabilityZone': 'us-east-1a', # primarily created in the specified availability zone.
        'Port': redshift_port,
        'ClusterSubnetGroupName': redshift_subnet_group_name
    }

    # Create the cluster
    try:
        response = redshift_client.create_cluster(**cluster_parameters)
        print(f'Redshift cluster: {redshift_cluster_name} creation initiated.')
    except redshift_client.exceptions.ClusterAlreadyExistsFault:
        print(f'Redshift Cluster: {redshift_cluster_name} already exists. Skipping cluster creation.')

    #ClusterIdentifier parameter specifies the unique identifier for your Redshift cluster.
    #The redshift_client.get_waiter('cluster_available').wait() statement waits until the Redshift cluster becomes available. 
    #By default, it will continuously check the cluster status until it becomes available 
    # Wait for the cluster to be available

    redshift_client.get_waiter('cluster_available').wait(
        ClusterIdentifier=cluster_parameters['ClusterIdentifier']
    )

    print(f'Redshift cluster: {redshift_cluster_name} is now available.')


def update_cluster_role(role_name, cluster_identifier):
    try:
        roleArn = iam_client.get_role(RoleName=role_name)['Role']['Arn']
        print(roleArn)

        redshift_client.modify_cluster_iam_roles(
            ClusterIdentifier=cluster_identifier,
            AddIamRoles=[roleArn]
        )

        print("Updated Redshift Cluster role.")
    except Exception as e:
        print("Failed to update cluster role", e)
        

def main():
    security_group_id = create_redshift_security_group()
    if security_group_id is not None:
        add_inbound_rule(security_group_id)

    subnet_ids = check_subnet_and_vpc()

    create_redshift_subnet_group(subnet_ids=subnet_ids)
    create_cluster(security_group_id=security_group_id)
    update_cluster_role(role_name=redshift_role_name, cluster_identifier=redshift_cluster_name)

    create_s3_vpc_endpoint()

if __name__ == "__main__":
    main()
