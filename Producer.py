from confluent_kafka import Producer

# Kafka集群地址
bootstrap_servers = 'localhost:9093'

# 主题名称
topic = 'test_python'

# 生产者配置
producer_config = {
    'bootstrap.servers': bootstrap_servers,
}

# 创建生产者
producer = Producer(producer_config)

# 发送消息
try:
    # 发送消息到指定主题
    for i in range(10):
        message = 'Message {}'.format(i)
        producer.produce(topic, message.encode('utf-8'))
        print('Sent: {}'.format(message))

except Exception as e:
    print('Exception occurred: {}'.format(str(e)))

finally:
    # 刷新缓冲区，确保所有消息都已发送
    producer.flush()
    # 关闭生产者
    producer.close()