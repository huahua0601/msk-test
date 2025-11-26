#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MSK Kafka 消费者示例
从指定的topic消费消息
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import signal
import sys

# MSK Broker地址
BOOTSTRAP_SERVERS = [
    'boot-y8y.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092',
    'boot-dw1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092',
    'boot-nm1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092'
]

# Topic名称
TOPIC_NAME = 'test-topic'

# Consumer Group ID
GROUP_ID = 'test-consumer-group'


# 优雅退出处理
def signal_handler(sig, frame):
    print('\n\n⚠️ 收到退出信号，正在关闭消费者...')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def create_consumer(from_beginning=True):
    """创建Kafka消费者实例"""
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            # Consumer Group ID
            group_id=GROUP_ID,
            # 值反序列化
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # 键反序列化
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            # 从最早的消息开始消费（如果没有已提交的offset）
            auto_offset_reset='earliest' if from_beginning else 'latest',
            # 自动提交offset
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            # Session超时
            session_timeout_ms=30000,
            # API版本自动检测
            api_version_auto_timeout_ms=10000
        )
        print("✅ Kafka消费者创建成功！")
        return consumer
    except KafkaError as e:
        print(f"❌ 创建Kafka消费者失败: {e}")
        return None


def consume_messages(consumer, max_messages=None):
    """消费消息"""
    print("\n开始消费消息...(按 Ctrl+C 退出)\n")
    print("=" * 80)
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            print(f"\n📨 收到消息 #{message_count}:")
            print(f"   - Topic: {message.topic}")
            print(f"   - Partition: {message.partition}")
            print(f"   - Offset: {message.offset}")
            print(f"   - Key: {message.key}")
            print(f"   - Timestamp: {message.timestamp}")
            print(f"   - Value: {json.dumps(message.value, ensure_ascii=False, indent=6)}")
            print("-" * 80)
            
            # 如果设置了最大消息数，达到后退出
            if max_messages and message_count >= max_messages:
                print(f"\n✅ 已消费 {message_count} 条消息，达到最大限制")
                break
                
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断操作")
    except Exception as e:
        print(f"\n❌ 消费消息时发生错误: {e}")
    finally:
        print(f"\n📊 总共消费了 {message_count} 条消息")


def main():
    """主函数"""
    print("=" * 80)
    print("MSK Kafka 消费者测试")
    print("=" * 80)
    print(f"Broker地址: {', '.join(BOOTSTRAP_SERVERS)}")
    print(f"Topic名称: {TOPIC_NAME}")
    print(f"Consumer Group: {GROUP_ID}")
    print("=" * 80)
    
    # 创建消费者（从头开始消费）
    consumer = create_consumer(from_beginning=True)
    if not consumer:
        return
    
    try:
        # 开始消费消息
        consume_messages(consumer)
        
    finally:
        # 关闭消费者
        consumer.close()
        print("\n✅ 消费者已关闭")
        print("=" * 80)


if __name__ == "__main__":
    main()

