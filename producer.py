#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MSK Kafka 生产者示例
向指定的topic发送消息
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
from datetime import datetime

# MSK Broker地址
BOOTSTRAP_SERVERS = [
    'boot-y8y.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092',
    'boot-dw1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092',
    'boot-nm1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092'
]

# Topic名称
TOPIC_NAME = 'test-topic'


def create_producer():
    """创建Kafka生产者实例"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            # 值序列化为JSON
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # 键序列化
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            # ACK设置：all表示等待所有副本确认
            acks='all',
            # 重试次数
            retries=3,
            # 请求超时
            request_timeout_ms=30000,
            # API版本自动检测
            api_version_auto_timeout_ms=10000
        )
        print("✅ Kafka生产者创建成功！")
        return producer
    except KafkaError as e:
        print(f"❌ 创建Kafka生产者失败: {e}")
        return None


def send_message(producer, key, value):
    """发送单条消息到Kafka"""
    try:
        future = producer.send(TOPIC_NAME, key=key, value=value)
        # 阻塞等待结果
        record_metadata = future.get(timeout=10)
        print(f"✅ 消息发送成功:")
        print(f"   - Topic: {record_metadata.topic}")
        print(f"   - Partition: {record_metadata.partition}")
        print(f"   - Offset: {record_metadata.offset}")
        print(f"   - Key: {key}")
        print(f"   - Value: {value}")
        return True
    except KafkaError as e:
        print(f"❌ 消息发送失败: {e}")
        return False


def send_batch_messages(producer, count=10):
    """批量发送消息"""
    print(f"\n开始批量发送 {count} 条消息...\n")
    success_count = 0
    
    for i in range(count):
        key = f"key-{i}"
        value = {
            "message_id": i,
            "content": f"这是第 {i} 条测试消息",
            "timestamp": datetime.now().isoformat(),
            "source": "msk-producer-test"
        }
        
        if send_message(producer, key, value):
            success_count += 1
        
        # 每条消息间隔0.5秒
        time.sleep(0.5)
    
    # 确保所有消息都已发送
    producer.flush()
    
    print(f"\n📊 批量发送完成: {success_count}/{count} 条消息成功")
    return success_count


def main():
    """主函数"""
    print("=" * 60)
    print("MSK Kafka 生产者测试")
    print("=" * 60)
    print(f"Broker地址: {', '.join(BOOTSTRAP_SERVERS)}")
    print(f"Topic名称: {TOPIC_NAME}")
    print("=" * 60)
    
    # 创建生产者
    producer = create_producer()
    if not producer:
        return
    
    try:
        # 发送单条测试消息
        print("\n【测试1】发送单条消息:")
        send_message(producer, "test-key", {
            "content": "这是一条测试消息",
            "timestamp": datetime.now().isoformat()
        })
        
        time.sleep(2)
        
        # 批量发送消息
        print("\n" + "=" * 60)
        print("【测试2】批量发送消息:")
        send_batch_messages(producer, count=10)
        
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断操作")
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
    finally:
        # 关闭生产者
        producer.close()
        print("\n✅ 生产者已关闭")
        print("=" * 60)


if __name__ == "__main__":
    main()

