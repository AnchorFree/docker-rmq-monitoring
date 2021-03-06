# docker-rmq-monitoring

The exporter sends a message to RabbitMQ server with RMQ_PUBLISHER_INTERVAL (sec) interval and measures operations delay. 
Message is considered expired upon reaching RMQ_EXPIRE_TIMEOUT (ms). RMQ_USER should have a monitoring tag to
check exchanges.

## Avalaible metrics

```
rmq_monitoring_consume_fails_created
rmq_monitoring_consume_fails_total

rmq_monitoring_publish_fails_created
rmq_monitoring_publish_fails_total

rmq_monitoring_event_time_ms_bucket
rmq_monitoring_event_time_ms_created
rmq_monitoring_event_time_ms_sum
rmq_monitoring_event_time_ms_count

rmq_monitoring_expired_msgs_created
rmq_monitoring_expired_msgs_total

rmq_monitoring_exchange_last_seen_alive_timestamp
```

## Docker compose sample
```yaml
version: '2'
services:
  rabbitmq-monitoring-exporter:
    container_name: rabbitmq-monitoring-exporter
    image: anchorfree/rmq-monitoring:INFRA-7537
    restart: always
    hostname: "${HOSTNAME}"
    network_mode: "host"
    environment:
      - TZ=US/Pacific
      - RMQ_SERVER=localhost
      - RMQ_PORT=5671
      - RMQ_SSL=True
      - RMQ_USER=user
      - RMQ_PASSWORD=password
      - RMQ_VHOST=/
      - RMQ_EXCHANGE_PUBLISHER=actions_fanout
      - RMQ_EXCHANGE_PUBLISHER_CREATE=False
      - RMQ_EXCHANGE_CONSUMER=test_exchange
      - RMQ_ROUTING_KEY=monitoring-${HOSTNAME}
      - RMQ_EXPIRE_TIMEOUT=5000
      - RMQ_PUBLISHER_INTERVAL=0.5
      - EXPORTER_PORT=9101
      - CONSUL_EXPORT_RMQ-MONITORING-EXPORTER=9101
      - ADD_BUCKET_VALUES=1000,1500,3000
      - RMQ_EXCHANGES_TO_CHECK_LIST=%2F/test_exchange,%2F/actions_fanout
      - RMQ_EXCHANGES_CHECK_INTERVAL=30
      - RMQ_API_SERVER=localhost
      - RMQ_API_PORT=15671
      - RMQ_API_SSL=True
```
