from confluent_kafka import Consumer, KafkaError

# Kafka集群地址
from confluent_kafka.cimpl import KafkaException

bootstrap_servers = 'localhost:9093'

# 主题名称
topic = 'test_python'

# 消费者配置
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'test_python1',
    'auto.offset.reset': 'earliest'  # 从最早的可用消息开始消费
}

# 创建消费者
consumer = Consumer(consumer_config)

# 订阅主题
consumer.subscribe([topic])

try:
    while True:
        # 从主题中拉取消息
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Reached end of partition
                print('%% %s [%d] reached end at offset %d\n' %
                      (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # 处理收到的消息
            print('Received message: {}'.format(msg.value().decode('utf-8')))

except KeyboardInterrupt:
    pass

finally:
    # 关闭消费者
    consumer.close()
