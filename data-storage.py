# - point to any kafka cluster and topic
# - point to any cassandra cluster + table

import argparse
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
import json
import atexit

topic_name = 'stock-analyzer'
kafka_cluster = 'localhost:9092'
cassandra_cluster = 'localhost'
keyspace = 'bittiger'
table = 'stock'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)

def shutdown_hook(consumer, session):
    """
        a shutdown hook to be called before the shutdown
        :param consumer: instance of a kafka consumer
        :param session: instance of a cassandra session
        :return: None
    """
    try:
        logger.info('Start to close Kafka Consumer')
        consumer.close()
        logger.info('Kafka Consumer closed')
        logger.info('Closing Cassandra Session')
        session.shutdown()
        logger.info('Cassandra Session closed')
    except KafkaError as kafka_error:
        logger.warn('Failed to close Kafka Consumer, caused by: %s', kafka_error.message)
    finally:
        logger.info('Existing program')


def persist_data(message, session):
    """
        persist stock data into cassandra
        :param stock_data:
            the stock data looks like this:
            [{
                "Index": "NASDAQ",
                "LastTradeWithCurrency": "109.36",
                "LastTradeDateTime": "2016-08-19T16:00:02Z",
                "LastTradePrice": "109.36",
                "LastTradeTime": "4:00PM EDT",
                "LastTradeDateTimeLong": "Aug 19, 4:00PM EDT",
                "StockSymbol": "AAPL",
                "ID": "22144"
            }]
        :param cassandra_session:

        :return: None
    """
    try:
        logger.debug('start to save message %s' % message)
        parsed = json.loads(message)[0]
        symbol = parsed.get('StockSymbol')
        price = float(parsed.get('LastTradePrice'))
        trade_time = parsed.get('LastTradeDateTime')

        statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) values " \
                    "('%s', '%s', %f)" % (table, symbol, trade_time, price)

        session.execute(statement)
        logger.info(
            'Persistend data to cassandra for symbol: %s, '
            'price: %f, tradetime: %s' % (symbol, price, trade_time))


    except Exception:
        logger.error('Failed to persist data to cassandra %s', message)


if __name__ == '__main__':
    # get the parse from command line
    # - argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='target kafka topic')
    parser.add_argument('kafka_cluster', help='target kafka cluster')
    parser.add_argument('cassandra_cluster', help='target cassandra cluster')
    parser.add_argument('keyspace', help='target keyspace')
    parser.add_argument('table', help='target table')

    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_cluster = args.kafka_cluster
    cassandra_cluster = args.cassandra_cluster
    keyspace = args.keyspace
    table = args.table

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_cluster
    )

    cassandra_cluster_obj = Cluster(
        contact_points=cassandra_cluster.split(',')
    )

    session = cassandra_cluster_obj.connect()

    session.execute("create keyspace if not exists %s WITH replication = "
                    "{'class': 'SimpleStrategy', 'replication_factor' : '3'}"
                    "AND durable_writes = 'true'" % keyspace)

    session.set_keyspace(keyspace)

    session.execute("create table if not exists %s "
                    "(stock_symbol text, trade_time timestamp, trade_price float, "
                    "primary key (stock_symbol, trade_time))" % table)

    atexit.register(shutdown_hook, consumer, session)

    for msg in consumer:
        persist_data(msg.value, session)
