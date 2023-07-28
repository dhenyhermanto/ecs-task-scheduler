import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_ecs_client():
    return boto3.client('ecs')

def get_dynamodb_resource():
    return boto3.resource('dynamodb')

def get_service_arns(ecs_client, cluster_name):
    response = ecs_client.list_services(cluster=cluster_name, maxResults=10)
    serviceArns = response['serviceArns']
    while 'nextToken' in response:
        response = ecs_client.list_services(
            cluster=cluster_name,
            maxResults=10,
            nextToken=response['nextToken']
        )
        serviceArns.extend(response['serviceArns'])
    return serviceArns

def get_services(ecs_client, cluster_name, serviceArns):
    services = []
    for serviceArn in serviceArns:
        response = ecs_client.describe_services(
            cluster=cluster_name,
            services=[serviceArn]
        )
        service = response['services'][0]
        services.append(service)
    return services

def store_services_in_dynamodb(cluster_name, table_name, partition_key_attribute, sort_key_attribute):
    logger.info('Storing services in DynamoDB')

    ecs_client = get_ecs_client()
    serviceArns = get_service_arns(ecs_client, cluster_name)
    services = get_services(ecs_client, cluster_name, serviceArns)

    dynamodb_services = []
    for service in services:
        dynamodb_services.append({
            partition_key_attribute: service['serviceName'],
            sort_key_attribute: str(service['desiredCount'])
        })

    dynamodb_resource = get_dynamodb_resource()
    table = dynamodb_resource.Table(table_name)
    with table.batch_writer() as batch:
        for service in dynamodb_services:
            logger.info(f"Storing service {service[partition_key_attribute]} with desired count {service[sort_key_attribute]} in DynamoDB")
            batch.put_item(Item=service)

def get_services_from_dynamodb(table_name, sort_key_attribute):
    logger.info('Retrieving services from DynamoDB')

    dynamodb_resource = get_dynamodb_resource()
    table = dynamodb_resource.Table(table_name)
    response = table.scan()
    services = response['Items']

    # Filter services with desired count greater than 0
    services = [service for service in services if int(service[sort_key_attribute]) > 0]

    return services

def update_desired_count_to_zero(cluster_name, table_name, partition_key_attribute, sort_key_attribute):
    logger.info('Updating desired count of services to 0')

    ecs_client = get_ecs_client()

    # Retrieve the list of services from the DynamoDB table
    services = get_services_from_dynamodb(table_name, sort_key_attribute)

    # Update the desired count of each service to 0
    for service in services:
        if 'sandbox' not in service[partition_key_attribute] and 'periscope' not in service[partition_key_attribute]:
            logger.info(f"Updating desired count of service {service[partition_key_attribute]} to 0")
            ecs_client.update_service(
                cluster=cluster_name,
                service=service[partition_key_attribute],
                desiredCount=0
            )

def update_desired_count_from_dynamodb(cluster_name, table_name, partition_key_attribute, sort_key_attribute):
    logger.info('Updating desired count of services from DynamoDB')

    ecs_client = get_ecs_client()

    # Retrieve the list of services from the DynamoDB table
    dynamodb_resource = get_dynamodb_resource()
    table = dynamodb_resource.Table(table_name)
    response = table.scan()
    services = response['Items']

    # Update the desired count of each service
    for service in services:
        if 'sandbox' not in service[partition_key_attribute] and 'periscope' not in service[partition_key_attribute]:
            logger.info(f"Updating desired count of service {service[partition_key_attribute]} to {service[sort_key_attribute]}")
            ecs_client.update_service(
                cluster=cluster_name,
                service=service[partition_key_attribute],
                desiredCount=int(service[sort_key_attribute])
            )

    # Delete the items from the DynamoDB table
    for service in services:
        logger.info(f"Deleting service {service[partition_key_attribute]} from DynamoDB")
        table.delete_item(
            Key={
                partition_key_attribute: service[partition_key_attribute],
                sort_key_attribute: service[sort_key_attribute]
            }
        )

def lambda_handler(event, context):
     cluster_name = event['CLUSTER_NAME']
     table_name = event['TABLE_NAME']
     partition_key_attribute = event['PARTITION_KEY_ATTRIBUTE']
     sort_key_attribute = event['SORT_KEY_ATTRIBUTE']

     if event['ACTION'] == 'stop':
         store_services_in_dynamodb(cluster_name, table_name, partition_key_attribute, sort_key_attribute)
         get_services_from_dynamodb(table_name, sort_key_attribute)
         update_desired_count_to_zero(cluster_name, table_name, partition_key_attribute, sort_key_attribute)
     elif event['ACTION'] == 'start':
         update_desired_count_from_dynamodb(cluster_name, table_name, partition_key_attribute, sort_key_attribute)
