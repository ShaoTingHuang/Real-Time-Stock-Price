# 1. Talk to any topic, configure
# 2. Fetch stock price every second

from googlefinance import getQuotes
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from apscheduler.schedulers.background import BackgroundScheduler

import atexit
import logging   # write into log
import time
import json
import argparse


# scheduler, automatically do something
schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()


# logger configuration
logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG) # used during development
# logger.setLevel(logging.ERROR) # used after development

symbol = 'AAPL'
topic_name = 'stock'
kafka_broker = '127.0.0.1:9092'

"""
release resources properly
    1. thread pool
    2. database connections
    3. network connections
"""
def shutdown_hook(producer):
    try:
        producer.flush(10)
        schedule.shutdown()
        logger.info('shutdown resources')
    except KafkaError as ke:
        logger.warn('failed to flush kafka')
    finally:
        producer.close(10)



# fetch stock price function
def fetch_price(producer, symbol):
    """
    :param producer
    :param symbol - string 
    
    """
    try:
        price = json.dumps(getQuotes(symbol))
        logger.debug('received stock price %s ' % price)
        producer.send(topic=topic_name, value=price, timestamp_ms=time.time())
    except KafkaTimeoutError as timeout_error:
        logger.warn('failed to send stock price for %s to kafka' % symbol)
        # logger.warning()
    except Exception:
        logger.warn('failed to send stock price')


if __name__ == '__main__':
    # get the parse from command line
    # - argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='the symbol of the stock')
    parser.add_argument('topic_name', help='the kafka topic to push to')
    parser.add_argument('kafka_broker', help='the location of kafka broker')

    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # create kafka producer instance
    producer = KafkaProducer(bootstrap_servers=kafka_broker)


    schedule = BackgroundScheduler()
    schedule.add_executor('threadpool')
    schedule.add_job(fetch_price, 'interval', [producer, symbol], seconds=1)

    schedule.start()

    #atexit.register(shutdown_hook, producer)

    while True:
        pass





