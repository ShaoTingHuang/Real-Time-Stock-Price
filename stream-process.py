# - read from any kafka broker and topic
# - perform average every 5s
# - write data back to kafka

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import json
import logging
import time
import atexit

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-process')
logger.setLevel(logging.INFO)


topic = 'stock-analyzer'
broker = 'localhost'
target_topic = 'average-stock-price'
kafka_producer = None

def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :return: None
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)

'''
def process(time, rdd):
    # - calculate average
    num_of_record = rdd.count()
    if num_of_record == 0:
        return

    price_sum = rdd.map(lambda record: float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePrice'))).reduce(lambda a, b : a + b)
    average = price_sum / num_of_record
    logger.info('received %d records from kafka, average price is %f' % (num_of_record, average))
    data = json.dumps({'average': average})
    kafka_producer.send(target_topic, value=data)




if __name__ == '__main__':
    # -kafka broker, kafka original topic, kafka target topic
    if len(sys.argv) != 4:
        print("usage: stream-process.py kafka-broker, kafka-original-topic, kafka-new-topic")
        exit(1)

    sc = SparkContext("local[2]", "AverageStockPrice")
    sc.setLogLevel('INFO')
    ssc = StreamingContext(sc, 5)


    broker, topic, target_topic = sys.argv[1:]

    # create data stream, each information only import one time
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': broker})

    #process_stream(directKafkaStream)
    directKafkaStream.foreachRDD(process)

    kafka_producer = KafkaProducer(
        bootstrap_servers=broker
    )

    # - setup proper shutdown hook
    atexit.register(shutdown_hook, kafka_producer)

    ssc.start()
    ssc.awaitTermination()
'''

def process_stream(stream):

    def send_to_kafka(rdd):
        results = rdd.collect()
        for r in results:
            data = json.dumps(
                {
                    'symbol': r[0],
                    'timestamp': time.time(),
                    'average': r[1]
                }
            )
            try:
                logger.info('Sending average price %s to kafka' % data)
                kafka_producer.send(target_topic, value=data)
            except KafkaError as error:
                logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)

    def pair(data):
        record = json.loads(data[1].decode('utf-8'))[0]
        return record.get('StockSymbol'), (float(record.get('LastTradePrice')), 1)

    stream.map(pair).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda (k, v): (k, v[0]/v[1])).foreachRDD(send_to_kafka)

if __name__ == '__main__':
    # -kafka broker, kafka original topic, kafka target topic
    if len(sys.argv) != 4:
        print("usage: stream-process.py kafka-broker, kafka-original-topic, kafka-new-topic")
        exit(1)

    # - create SparkContext and StreamingContext
    sc = SparkContext("local[2]", "StockAveragePrice")
    sc.setLogLevel('INFO')
    ssc = StreamingContext(sc, 5)

    brokers, topic, target_topic = sys.argv[1:]

    # - instantiate a kafka stream for processing
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': brokers})
    process_stream(directKafkaStream)

    # - instantiate a simple kafka producer
    kafka_producer = KafkaProducer(
        bootstrap_servers=brokers
    )

    # - setup proper shutdown hook
    atexit.register(shutdown_hook, kafka_producer)

    ssc.start()
    ssc.awaitTermination()
