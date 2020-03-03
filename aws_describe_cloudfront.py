from datetime import datetime

import asyncio
import argparse
import aiobotocore


parser = argparse.ArgumentParser(description='Invalidate cache for cloudfront distribution')
parser.add_argument('--tag', help='CloudFront tag',
                    required=True)
parser.add_argument('--path', help='Distribution path to invalidate. Default is: /*',
                    required=True)
args = parser.parse_args()

async def describeDistributions() -> list:
  """
    Get all CloudFront distributions and return they ARNs.
  """
  distributions = await client.list_distributions()
  arns = []
  for distribution in distributions['DistributionList']['Items']:
    arns.append({'distributionId': distribution['Id'], 'distributionArn': distribution['ARN']})
  return arns


async def describeDistributionsTags(distributionId: str, distributionArn: str) -> dict:
  """
    Return distribution tags.
  """
  return { 
    'distributionId': distributionId,
    'distributionTags': (await client.list_tags_for_resource(Resource=distributionArn))['Tags']['Items']
  }

async def createInvalidation(distributionId: str, path: str = '/*') -> dict:
  """
    Invalidate distribution cache for given path.
  """
  return await client.create_invalidation(
    DistributionId = distributionId,
    InvalidationBatch = {
      'Paths': {
        'Quantity': 1,
        'Items': [
          path
        ],
      },
      'CallerReference': datetime.now(tz=None).strftime("%H:%M:%S.%f:%b:%d:%Y")
    }
  )


async def main():
  global client
  
  session = aiobotocore.get_session()
  client = session.create_client('cloudfront')
    
  tags  = []
  tasks = []
  
  distributions = await describeDistributions()
  
  for distribution in distributions:
    tasks.append(describeDistributionsTags(distribution['distributionId'], distribution['distributionArn']))
    
  for task in await asyncio.gather(*tasks):
    tags.append({ 'distributionId': task['distributionId'], 'distributionTags': task['distributionTags'] })

  for distribution in tags:
    for tag in distribution['distributionTags']:  
      if tag['Key'] == 'Name' and tag['Value'] == args.tag:
        distributionId = distribution['distributionId']

  if distributionId:
    print(await createInvalidation(distributionId))


asyncio.run(main())
