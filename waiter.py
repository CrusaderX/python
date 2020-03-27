import asyncio
import http3
import time
import argparse

client = http3.AsyncClient()

parser = argparse.ArgumentParser(description='Wait until all backends are up and healthy')
parser.add_argument('--delay', help='Max delay time for one attempt to wait for',
                    default=2, required=False)
args = parser.parse_args()

urls = [
    'http://backend1/api/v1/healthz',
    'http://backend2/api/v1/healthz',
    'http://backend3/api/v1/healthz',
    'http://backend4/api/v1/healthz',
    'http://backend5/api/v1/healthz'
]

async def req(url) -> dict:
  """
    Return current url and status code for the request. If
    request fail against of network connection or anything
    else we return None as not successfull response code.
    
    todo: add exceptions handling
  """
  try:
    return { "url": url, "status_code": (await client.get(url)).status_code }
  except:
    return { "url": url, "status_code": None }

async def main() -> None:
  print('Waiting services to be healthy')
  responses = []
  tasks = [ req(url) for url in urls ]
  responses.append(await asyncio.gather(*tasks))
  
  while True:
    tasks = []
    for response in responses:
      for status in response:
        if status['status_code'] != 200:
          tasks.append(req(status['url']))

    if not tasks:
      break

    responses = []
    time.sleep(args.delay)
    responses.append(await asyncio.gather(*tasks))

# in compability to run via Python 3.6
loop = asyncio.get_event_loop()
result = loop.run_until_complete(main())

# or uncomment this line if you use Python 3.7+
# asyncio.run(main())