# George Trammell
import os
from google.cloud import pubsub_v1

banned_countries = ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe'
and 'Syria'] 

publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/robust-builder-398218/topics/ds561-hw3-banned-requests'

def handle_request(country):
  if country in banned_countries:
    data = f'Forbidden request from {country}'
    data = data.encode('utf-8')
    future = publisher.publish(topic_name, data)
    print(future.result())
    return 'Access denied', 400
  else:
    return 'Ok'

# Test locally 
print(handle_request('North Korea'))
print(handle_request('United States'))