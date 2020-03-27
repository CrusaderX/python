import boto3
import argparse
import datetime
import pytz
import logging
import uuid

from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description='Recover DocumentDB from snapshot to point in time')
parser.add_argument('--cluster', help='AWS cluster identifier which should be restored. Ex: my-documentdb',
                    required=True)
parser.add_argument('--profile', help='AWS profile',
                    required=True)
parser.add_argument('--region', help='AWS region',
                    required=True)
parser.add_argument('--time', help='Point in time to restore. Format is: %Y %m %d %H %M',
                    required=True)
parser.add_argument('--attempts', help='Max attempts for instance waiter',
                    default=50, required=True)
parser.add_argument('--delay', help='Max delay time for one attempt for instance waiter',
                    default=40, required=True)
args = parser.parse_args()


def describeDbClusters(client, dbClusterIdentifier: str) -> dict:
  """
    Return DocumentDB cluster information
  """
  try:
    return client.describe_db_clusters(
      DBClusterIdentifier = dbClusterIdentifier
    )['DBClusters'][0]
  except ClientError as error:
    return logging.warning('{}'.format(error.response['Error']['Message']))


def describeClusterInstances(client, clusterArn: str) -> dict:
  """
    Return DocumentDB instances from given cluster
  """
  try:
    return client.describe_db_instances(
        Filters = [
          {
            'Name': 'db-cluster-id',
            'Values': [
              clusterArn
            ]
          }
        ]
      )
  except ClientError as error:
    return logging.warning('{}'.format(error.response['Error']['Message']))


def validateRestoreDateTime(earliestRestorableTime, time: str, latestRestorableTime) -> bool:
  """
    Validate if date between two date ranges or not
  """
  if earliestRestorableTime < time < latestRestorableTime:
    return True
  return False


def restoreDbClusterToPointInTime(
    client, sourceDbClusterIdentifier: str, restoreDbClusterIdentifier: str, 
    dbSubnetGroupName: str, vpcSecurityGroupIds: list, 
    year: int, month: int, day: int, hour: int, minute: int
  ) -> dict:
  """
    Return DocumentDB object restored from snapshot in point with given time
  """
  try:
    return client.restore_db_cluster_to_point_in_time(
      DBClusterIdentifier = restoreDbClusterIdentifier,
      SourceDBClusterIdentifier = sourceDbClusterIdentifier,
      RestoreToTime = datetime.datetime(year, month, day, hour, minute).isoformat() + 'Z',
      DBSubnetGroupName = dbSubnetGroupName,
      VpcSecurityGroupIds = vpcSecurityGroupIds,
    )['DBCluster']
  except ClientError as error:
    return logging.warning('{}'.format(error.response['Error']['Message']))


def createClusterInstances(
    client, dbInstanceIdentifier: str, dbInstanceClass: str, 
    availabilityZone: str,  dbClusterIdentifier: str
  ) -> dict:
  """
    Create DocumentDB instance inside given Cluster
  """
  try:
    return client.create_db_instance(
        DBInstanceIdentifier = dbInstanceIdentifier,
        DBInstanceClass = dbInstanceClass,
        Engine = 'docdb',
        AvailabilityZone = availabilityZone,
        DBClusterIdentifier = dbClusterIdentifier
      )['DBInstance']['DBInstanceIdentifier']
  except ClientError as error:
    return logging.warning('{}'.format(error.response['Error']['Message']))


def main():
  session = boto3.Session(profile_name=args.profile, region_name=args.region)
  docdb = session.client('docdb')
  utc = pytz.UTC
  dbClusterIdentifier = args.cluster
  randomUUID = str(uuid.uuid1())
  restoreClusterIdentifier = dbClusterIdentifier + '-' + randomUUID
  createInstancesResponses = []
  year, month, day, hour, minute = map(int, args.time.split(' '))
  
  cluster = describeDbClusters(docdb, dbClusterIdentifier)
  if cluster:
    earliestRestorableTime = cluster['EarliestRestorableTime']
    latestRestorableTime = cluster['LatestRestorableTime']
    inputTime = utc.localize(datetime.datetime.strptime(args.time, "%Y %m %d %H %M"))
    
    timeValidToRestore = validateRestoreDateTime(
      earliestRestorableTime, inputTime, latestRestorableTime
    )

    if timeValidToRestore:
      dbClusterArn = cluster['DBClusterArn']
      dbSubnetGroupName = cluster['DBSubnetGroup']

      instances = describeClusterInstances(docdb, dbClusterArn)
      instancesDbInstanceClass = instances['DBInstances'][0]['DBInstanceClass']
      instancesVpcSecurityGroups = instances['DBInstances'][0]['VpcSecurityGroups']
      instancesVpcSecurityGroupsIds = [vpc['VpcSecurityGroupId'] for vpc in instancesVpcSecurityGroups]

      if instances:
        restoreCluster = restoreDbClusterToPointInTime(
          docdb, dbClusterIdentifier, restoreClusterIdentifier, dbSubnetGroupName, 
          instancesVpcSecurityGroupsIds, year, month, day, hour, minute
        )
        if restoreCluster['Status'] == 'creating':
          clusterEndpoint = restoreCluster['Endpoint']

          for i in range(len(instances['DBInstances'])):
            createInstancesResponses.append(createClusterInstances(
              docdb, dbClusterIdentifier + str(i + 1) + '-' + randomUUID, instancesDbInstanceClass,
              instances['DBInstances'][i]['AvailabilityZone'], restoreClusterIdentifier
            ))

          for dbInstanceIdentifier in createInstancesResponses:
            responseWaiter = docdb.get_waiter('db_instance_available')
            responseWaiter.wait(
              DBInstanceIdentifier = dbInstanceIdentifier,
              WaiterConfig = {
                'Delay': args.delay,
                'MaxAttempts': args.attempts
              }
            )
          logging.info('Cluster {} created successfully. Endpoint is: {}'.format(
            restoreClusterIdentifier, clusterEndpoint
          ))


main()