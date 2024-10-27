import boto3
import time
from botocore.exceptions import ClientError
import pprint

def delete_resources(suffix):
    boto3_session = boto3.session.Session()
    region_name = boto3_session.region_name
    sts_client = boto3.client('sts')
    account_id = sts_client.get_caller_identity()["Account"]
    s3_suffix = f"{region_name}-{account_id}"

    # Initialize clients
    bedrock_agent_client = boto3_session.client('bedrock-agent', region_name=region_name)
    aoss_client = boto3_session.client('opensearchserverless')
    s3_client = boto3.client('s3')
    iam_client = boto3.client('iam')
    pp = pprint.PrettyPrinter(indent=2)

    # Resource names
    vector_store_name = f'bedrock-sample-rag-{suffix}'
    kb_name = f"bedrock-sample-knowledge-base-{suffix}"
    bucket_name = f'bedrock-kb-{s3_suffix}'
    index_name = f"bedrock-sample-index-{suffix}"
    role_name = f"AmazonBedrockExecutionRoleForKnowledgeBase_{suffix}"

    try:
        # 1. Delete Knowledge Base and associated Data Source
        print("Deleting Knowledge Base and Data Source...")
        try:
            kbs = bedrock_agent_client.list_knowledge_bases()
            for kb in kbs.get('knowledgeBaseItems', []):
                if kb['name'] == kb_name:
                    # Delete all data sources first
                    data_sources = bedrock_agent_client.list_data_sources(
                        knowledgeBaseId=kb['knowledgeBaseId']
                    )
                    for ds in data_sources.get('dataSourceItems', []):
                        bedrock_agent_client.delete_data_source(
                            knowledgeBaseId=kb['knowledgeBaseId'],
                            dataSourceId=ds['dataSourceId']
                        )
                    # Then delete the knowledge base
                    bedrock_agent_client.delete_knowledge_base(
                        knowledgeBaseId=kb['knowledgeBaseId']
                    )
        except ClientError as e:
            print(f"Error deleting Knowledge Base: {e}")

        # 2. Delete OpenSearch Serverless Collection
        print("Deleting OpenSearch Serverless Collection...")
        try:
            aoss_client.delete_collection(name=vector_store_name)
            # Wait for collection deletion
            while True:
                try:
                    aoss_client.batch_get_collection(names=[vector_store_name])
                    print("Waiting for collection deletion...")
                    time.sleep(30)
                except ClientError:
                    print("Collection deleted")
                    break
        except ClientError as e:
            print(f"Error deleting collection: {e}")

        # 3. Delete OpenSearch Security Policies
        print("Deleting OpenSearch Security Policies...")
        try:
            aoss_client.delete_access_policy(name=f"bedrock-sample-policy-{vector_store_name}")
            aoss_client.delete_network_policy(name=f"bedrock-sample-policy-{vector_store_name}")
            aoss_client.delete_security_policy(name=f"bedrock-sample-policy-{vector_store_name}")
        except ClientError as e:
            print(f"Error deleting security policies: {e}")

        # 4. Delete IAM Role and Policies
        print("Deleting IAM Role and Policies...")
        try:
            # Detach and delete policies
            for policy in iam_client.list_attached_role_policies(RoleName=role_name)['AttachedPolicies']:
                iam_client.detach_role_policy(RoleName=role_name, PolicyArn=policy['PolicyArn'])

            for policy in iam_client.list_role_policies(RoleName=role_name)['PolicyNames']:
                iam_client.delete_role_policy(RoleName=role_name, PolicyName=policy)

            # Delete the role
            iam_client.delete_role(RoleName=role_name)
        except ClientError as e:
            print(f"Error deleting IAM role: {e}")

        # 5. Empty and Delete S3 Bucket
        print("Deleting S3 Bucket...")
        try:
            bucket = boto3.resource('s3').Bucket(bucket_name)
            bucket.objects.all().delete()  # Delete all objects in the bucket
            s3_client.delete_bucket(Bucket=bucket_name)  # Delete the bucket
        except ClientError as e:
            print(f"Error deleting S3 bucket: {e}")

        print("Resource cleanup completed")

    except Exception as e:
        print(f"An error occurred during cleanup: {e}")

# Usage
if __name__ == "__main__":
    # Use the same suffix as used in creation
    suffix = 123  # Replace with the suffix used during resource creation
    delete_resources(suffix)

