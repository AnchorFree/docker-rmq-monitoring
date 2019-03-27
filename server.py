"""RabbitMQ Monitoring exporter"""

import datetime
import json
import logging
import multiprocessing as mp
import sys
import time
import pika
from prometheus_client import start_http_server, Histogram, Counter
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
                ssl=config["rmq_ssl"]))

            init_time = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
            channel = connection_publisher.channel()
            channel_time = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
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
                ssl=config["rmq_ssl"]))
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


def main():     # pylint: disable=R0914
    """Manages consumer, publisher and accounting"""
    config = {
        "rmq_server": getenv('RMQ_SERVER', default='localhost', type=str),
        "rmq_user": getenv('RMQ_USER', default='', type=str),
        "rmq_password": getenv('RMQ_PASSWORD', default='', type=str),
        "rmq_vhost": getenv('RMQ_VHOST', default='', type=str),
        "rmq_port": getenv('RMQ_PORT', default=5671, type=int),
        "rmq_ssl": getenv('RMQ_SSL', default=True, type=bool),
        "exchange_publisher": getenv('RMQ_EXCHANGE_PUBLISHER', default='', type=str),
        "exchange_consumer": getenv('RMQ_EXCHANGE_CONSUMER', default='', type=str),
        "routing_key": getenv('RMQ_ROUTING_KEY', default='', type=str),
        "expire_timeout_ms": getenv('RMQ_EXPIRE_TIMEOUT', default=5000, type=int),
        "publisher_interval": getenv('RMQ_PUBLISHER_INTERVAL', default=0.5, type=float),
        "exporter_port": getenv('EXPORTER_PORT', default=9100, type=int)
    }

    config["multipassport"] = pika.PlainCredentials(config["rmq_user"], config["rmq_password"])

    log = init_logger()

    manager = mp.Manager()

    d_p_msgs = manager.dict()
    d_c_msgs = manager.dict()
    d_stats = manager.dict({"rmq_monitoring_publish_fails": 0, "rmq_monitoring_consume_fails": 0})

    rmq_monitoring_expired_msgs = Counter('rmq_monitoring_expired_msgs', 'Expired messages')
    rmq_monitoring_publish_fails = Counter('rmq_monitoring_publish_fails', 'Publisher fails')
    rmq_monitoring_consume_fails = Counter('rmq_monitoring_consume_fails', 'Consumer fails')

    p_consumer = mp.Process(target=consume_rmq_message, args=(config, log, d_stats, d_c_msgs))
    p_publisher = mp.Process(target=publish_rmq_message, args=(config, log, d_stats, d_p_msgs))
    p_consumer.daemon = True
    p_publisher.daemon = True
    p_consumer.start()
    p_publisher.start()

    rmq_monitoring_event_time_ms = Histogram('rmq_monitoring_event_time_ms', '', ['action'],
                                             buckets=[le**2 for le in range(1, 18)])

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

        time.sleep(0.1)

    p_consumer.join()
    p_publisher.join()


if __name__ == '__main__':
    main()
