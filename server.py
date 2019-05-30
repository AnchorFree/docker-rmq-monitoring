"""RabbitMQ Monitoring exporter"""

import datetime
import json
import logging
import multiprocessing as mp
import sys
import time
import pika
import requests
from prometheus_client import start_http_server, Histogram, Counter, Gauge
from smart_getenv import getenv


def init_logger():
    """ Creates a logging instance"""
    log = logging.getLogger("rmq-monitoring")
    out_hdlr = logging.StreamHandler(sys.stdout)
    out_hdlr.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    out_hdlr.setLevel(logging.INFO)
    log.addHandler(out_hdlr)
    log.setLevel(logging.INFO)
    return log


def publish_rmq_message(config, log, d_stats, d_p_msgs):
    """Starts a publisher"""
    while True:
        try:
            connection_publisher = pika.BlockingConnection(pika.ConnectionParameters(
                host=config["rmq_server"],
                port=config["rmq_port"],
                virtual_host=config["rmq_vhost"],
                credentials=config["multipassport"],
                ssl=config["rmq_ssl"],
                blocked_connection_timeout=120))

            init_time = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
            channel = connection_publisher.channel()
            channel_time = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')

            if config['exchange_publisher_create']:
                channel.exchange_declare(exchange=config['exchange_publisher'],
                                         exchange_type='direct', durable=True)

            channel.queue_declare(config["routing_key"],
                                  arguments={'x-expires': 10000, 'x-max-length': 1000,
                                             'x-message-ttl': config["expire_timeout_ms"]})
            declare_time = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
            msg = {"init_time": init_time}
            msg_json = json.dumps(msg)
            channel.basic_publish(exchange=config["exchange_publisher"],
                                  routing_key=config["routing_key"],
                                  body=msg_json)
            send_time = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
            connection_publisher.close()
            close_time = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
            d_p_msgs[init_time] = {"channel_time": channel_time,
                                   "declare_time": declare_time,
                                   "send_time": send_time,
                                   "close_time": close_time}

        except Exception as exception:  # pylint: disable=W0703
            log.error("Publisher failed: %s", exception)
            d_stats["rmq_monitoring_publish_fails"] += 1

        time.sleep(config["publisher_interval"])


def consumer_callback(channel, method, body, d_c_msgs):
    """Sends ack to RabbitMQ and counts delivery_time"""
    channel.basic_ack(delivery_tag=method.delivery_tag)
    msg = json.loads(body.decode('utf-8'))
    delivery_time = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
    d_c_msgs[msg["init_time"]] = {'delivery_time': delivery_time}


def consume_rmq_message(config, log, d_stats, d_c_msgs):
    """Starts a consumer"""
    while True:
        try:
            connection_consumer = pika.BlockingConnection(pika.ConnectionParameters(
                host=config["rmq_server"],
                port=config["rmq_port"],
                virtual_host=config["rmq_vhost"],
                credentials=config["multipassport"],
                ssl=config["rmq_ssl"],
                blocked_connection_timeout=120))

            channel = connection_consumer.channel()
            channel.queue_declare(config["routing_key"],
                                  arguments={'x-expires': 10000, 'x-max-length': 1000,
                                             'x-message-ttl': config["expire_timeout_ms"]})
            channel.queue_bind(exchange=config["exchange_consumer"],
                               queue=config["routing_key"],
                               routing_key=config["routing_key"])
            channel.basic_consume(lambda ch, method, properties,
                                         body: consumer_callback(ch, method, body, d_c_msgs),
                                  queue=config["routing_key"],
                                  no_ack=False)
            channel.start_consuming()
            channel.close()
        except Exception as exception:  # pylint: disable=W0703
            log.error("Consumer failed: %s", exception)
            d_stats["rmq_monitoring_consume_fails"] += 1

        time.sleep(1)


def get_delta_ms(delta):
    """
    Returns delta in ms
    Arguments:
        delta - datetime.timedelta object

    """
    return (delta.days * 86400000) + (delta.seconds * 1000) + (delta.microseconds / 1000)


def check_exchanges(config, log, d_exchanges):
    """Checks exchanges for existence"""
    while True:
        if not config['exchanges_to_check_list']:
            log.info("No exchanges to check provided")
            return 0
        if config['rmq_api_ssl']:
            proto = 'https://'
        else:
            proto = 'http://'

        for exchange in config['exchanges_to_check_list']:
            url = proto + config['rmq_api_server'] + ':' + str(config['rmq_api_port'])\
                  + '/api/exchanges/' + exchange
            try:
                req = requests.get(url, auth=requests.auth.HTTPBasicAuth(config['rmq_user'],
                                                                         config['rmq_password']))
                if 'name' in req.json() and 'error' not in req.json():
                    d_exchanges[exchange] = datetime.datetime.utcnow().strftime('%s')

            except Exception as exception:  # pylint: disable=W0703
                log.error("Error accessing API: %s, url: %s", exception, url)

            if exchange not in d_exchanges:
                d_exchanges[exchange] = 0

        time.sleep(config['exchanges_check_interval'])


def main():     # pylint: disable=R0914, R0915
    """Manages consumer, publisher and accounting"""
    config = {
        "rmq_server": getenv('RMQ_SERVER', default='localhost', type=str),
        "rmq_user": getenv('RMQ_USER', default='', type=str),
        "rmq_password": getenv('RMQ_PASSWORD', default='', type=str),
        "rmq_vhost": getenv('RMQ_VHOST', default='', type=str),
        "rmq_port": getenv('RMQ_PORT', default=5671, type=int),
        "rmq_ssl": getenv('RMQ_SSL', default=True, type=bool),
        "exchange_publisher": getenv('RMQ_EXCHANGE_PUBLISHER', default='', type=str),
        "exchange_publisher_create": getenv('RMQ_EXCHANGE_PUBLISHER_CREATE', default=False, type=bool),
        "exchange_consumer": getenv('RMQ_EXCHANGE_CONSUMER', default='', type=str),
        "routing_key": getenv('RMQ_ROUTING_KEY', default='', type=str),
        "expire_timeout_ms": getenv('RMQ_EXPIRE_TIMEOUT', default=5000, type=int),
        "publisher_interval": getenv('RMQ_PUBLISHER_INTERVAL', default=0.5, type=float),
        "exporter_port": getenv('EXPORTER_PORT', default=9100, type=int),
        "add_bucket_values": getenv('ADD_BUCKET_VALUES', default=[], type=list),
        "exchanges_to_check_list": getenv('RMQ_EXCHANGES_TO_CHECK_LIST', default=[], type=list),
        "exchanges_check_interval": getenv('RMQ_EXCHANGES_CHECK_INTERVAL', default=30, type=float),
        "rmq_api_server": getenv('RMQ_API_SERVER', default='localhost', type=str),
        "rmq_api_port": getenv('RMQ_API_PORT', default=15671, type=int),
        "rmq_api_ssl": getenv('RMQ_API_SSL', default=True, type=bool)
    }

    config["multipassport"] = pika.PlainCredentials(config["rmq_user"], config["rmq_password"])

    log = init_logger()

    manager = mp.Manager()

    d_p_msgs = manager.dict()
    d_c_msgs = manager.dict()
    d_stats = manager.dict({"rmq_monitoring_publish_fails": 0, "rmq_monitoring_consume_fails": 0})
    d_exchanges = manager.dict()

    rmq_monitoring_expired_msgs = Counter('rmq_monitoring_expired_msgs', 'Expired messages')
    rmq_monitoring_publish_fails = Counter('rmq_monitoring_publish_fails', 'Publisher fails')
    rmq_monitoring_consume_fails = Counter('rmq_monitoring_consume_fails', 'Consumer fails')
    rmq_monitoring_exchange_last_seen_alive_timestamp =\
        Gauge('rmq_monitoring_exchange_last_alive_timestamp',
              'Exchange last seen alive', ['exchange'])

    p_consumer = mp.Process(target=consume_rmq_message, args=(config, log, d_stats, d_c_msgs))
    p_publisher = mp.Process(target=publish_rmq_message, args=(config, log, d_stats, d_p_msgs))
    p_exchange_check = mp.Process(target=check_exchanges, args=(config, log, d_exchanges))
    p_consumer.daemon = True
    p_publisher.daemon = True
    p_exchange_check.daemon = True
    p_consumer.start()
    p_publisher.start()
    p_exchange_check.start()

    rmq_monitoring_event_time_ms = Histogram('rmq_monitoring_event_time_ms', '', ['action'],
                                             buckets=[le**2 for le in range(1, 18)]
                                             + config["add_bucket_values"])

    start_http_server(config["exporter_port"])

    while True:
        tmp = d_stats["rmq_monitoring_publish_fails"]
        rmq_monitoring_publish_fails.inc(tmp)
        d_stats["rmq_monitoring_publish_fails"] = d_stats["rmq_monitoring_publish_fails"] - tmp

        tmp = d_stats["rmq_monitoring_consume_fails"]
        rmq_monitoring_consume_fails.inc(tmp)
        d_stats["rmq_monitoring_consume_fails"] = d_stats["rmq_monitoring_consume_fails"] - tmp

        for k, _ in d_c_msgs.items():
            if k not in d_p_msgs:
                continue
            init_time = datetime.datetime.strptime(k, '%Y%m%d%H%M%S%f')
            delivery_time = datetime.datetime.strptime(d_c_msgs[k]['delivery_time'],
                                                       '%Y%m%d%H%M%S%f')
            send_time = datetime.datetime.strptime(d_p_msgs[k]['send_time'],
                                                   '%Y%m%d%H%M%S%f')
            channel_time = datetime.datetime.strptime(d_p_msgs[k]['channel_time'],
                                                      '%Y%m%d%H%M%S%f')
            declare_time = datetime.datetime.strptime(d_p_msgs[k]['declare_time'],
                                                      '%Y%m%d%H%M%S%f')
            close_time = datetime.datetime.strptime(d_p_msgs[k]['close_time'],
                                                    '%Y%m%d%H%M%S%f')

            rmq_monitoring_event_time_ms.labels(action="channel_create")\
                .observe(get_delta_ms(channel_time - init_time))
            rmq_monitoring_event_time_ms.labels(action="queue_declare")\
                .observe(get_delta_ms(declare_time - channel_time))
            rmq_monitoring_event_time_ms.labels(action="msg_publish")\
                .observe(get_delta_ms(send_time - declare_time))
            rmq_monitoring_event_time_ms.labels(action="channel_close")\
                .observe(get_delta_ms(close_time - send_time))
            rmq_monitoring_event_time_ms.labels(action="msg_travel")\
                .observe(get_delta_ms(delivery_time - send_time))
            rmq_monitoring_event_time_ms.labels(action="total_time")\
                .observe(get_delta_ms(delivery_time - init_time))
            rmq_monitoring_event_time_ms.labels(action="total_publish_time")\
                .observe(get_delta_ms(send_time - init_time))

            d_p_msgs.pop(k)
            d_c_msgs.pop(k)

        current_time = datetime.datetime.utcnow()
        for k, _ in d_p_msgs.items():
            init_time = datetime.datetime.strptime(k, '%Y%m%d%H%M%S%f')
            if get_delta_ms(current_time - init_time) > config["expire_timeout_ms"] and \
                            k not in d_c_msgs:
                log.debug('message expired')
                rmq_monitoring_expired_msgs.inc(1)
                d_p_msgs.pop(k)

        for key, val in d_exchanges.items():
            rmq_monitoring_exchange_last_seen_alive_timestamp.labels(exchange=key).set(val)

        time.sleep(0.1)

    p_consumer.join()
    p_publisher.join()
    p_exchange_check.join()


if __name__ == '__main__':
    main()
