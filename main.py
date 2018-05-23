from __future__ import print_function

import os
import time
from bluelens_log import Logging

import bottlenose
from bs4 import BeautifulSoup

import redis
import pickle

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-amazon-best-seeker')
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

REDIS_AMZ_BEST_ASIN_QUEUE = "bl:amz:best:asin:queue"
REDIS_AMZ_BEST_ASIN_QUEUE_TEST = "bl:amz:best:asin:queue:test"

amazon = bottlenose.Amazon(AWSAccessKeyId=os.environ['AWS_ACCESS_KEY_ID'],
                           AWSSecretAccessKey=os.environ['AWS_SECRET_ACCESS_KEY'],
                           AssociateTag=os.environ['AWS_ASSOCIATE_TAG'],
                           Parser=lambda text: BeautifulSoup(text, 'xml'))

node_ids = [
  '2368343011',
  '2368365011',
  '5418124011',
  '1044544',
  '1044548',
  '2368344011',
  '5418125011',
  '5418126011'
]

def call_item_search_api(node_id, index):
  try:
    print(node_id + ' / ' + str(index))
    product_list = []

    item_search_res = amazon.ItemSearch(SearchIndex='Fashion', BrowseNode=node_id, ItemPage=index,
                                        Sort='popularity-rank')

    items = item_search_res.find_all('Item')
    for item in items:
      if item:
        product = {}
        product['node_id'] = node_id
        product['asin'] = item.ASIN.text

        product_list.append(product)

    if len(product_list) > 0:
      rconn.lpush(REDIS_AMZ_BEST_ASIN_QUEUE, pickle.dumps(product_list))
      # rconn.lpush(REDIS_AMZ_BEST_ASIN_QUEUE_TEST, pickle.dumps(product_list))

      if index < 10:
        index += 1
        call_item_search_api(node_id, index)

    time.sleep(0.5)

  except Exception as e:
    log.error(str(e))


def seek_amazon_bests():
  for node_id in node_ids:
    call_item_search_api(node_id, 1)

def start(rconn):
  seek_amazon_bests()

if __name__ == '__main__':
  try:
    # log.info('Start bl-amazon-best-seeker')
    start(rconn)
  except Exception as e:
    log.error(str(e))
