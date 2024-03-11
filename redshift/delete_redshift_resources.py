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


def delete_redshift_security_group():
    try:
        response = ec2_client.delete_security_group(GroupName=security_group_name)
        print("Successfully deleted the security group: ", security_group_name)
    except Exception as e:
        print("Failed to delete security group: ", security_group_name)

def delete_redshift_subnet_group():
    try:
        response = redshift_client.delete_cluster_subnet_group(ClusterSubnetGroupName=redshift_subnet_group_name)
        print("Successfully deleted the redshift subnet group: ", redshift_subnet_group_name)
    except Exception as e:
        print("Failed to delete security group: ", security_group_name)

def delete_s3_vpc_endpoint():
    try:
        response = ec2_client.describe_vpc_endpoints(
            Filters=[
                {'Name': 'service-name', 'Values': ['com.amazonaws.us-east-1.s3']},
                {'Name': 'vpc-id', 'Values': [vpc_id]}
            ]
        )

        vpc_endpoint_id = response['VpcEndpoints'][0]['VpcEndpointId']
        
        response = ec2_client.delete_vpc_endpoints(VpcEndpointIds=[vpc_endpoint_id])
        print("Successfully deleted s3 vpc endpoint with id: ", vpc_endpoint_id)
    except Exception as e:
        print('Failed to delete s3 vpc endpoint', e)

def delete_redshift_cluster():
    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier=redshift_cluster_name,
            SkipFinalClusterSnapshot=True
        )
        print(f'Redshift cluster: {redshift_cluster_name} deletion initiated.')

        redshift_client.get_waiter('cluster_deleted').wait(
            ClusterIdentifier=redshift_cluster_name
        )
        print(f'Redshift cluster: {redshift_cluster_name} is now deleted.')
    except redshift_client.exceptions.ClusterNotFoundFault:
        print(f'Redshift Cluster: {redshift_cluster_name} does not exists. Skipping cluster deletion.')


def main():
    delete_redshift_cluster()
    delete_redshift_subnet_group()
    delete_s3_vpc_endpoint()
    delete_redshift_security_group()

if __name__ == "__main__":
    main()
