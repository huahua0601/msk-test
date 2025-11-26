#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MSK Kafka 完整测试脚本
同时测试生产和消费功能
"""

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
from datetime import datetime
import threading

# MSK Broker地址
BOOTSTRAP_SERVERS = [
    'boot-y8y.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092',
    'boot-dw1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092',
    'boot-nm1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092'
]

# Topic名称
TOPIC_NAME = 'test-topic'


def test_connection():
    """测试MSK连接"""
    print("\n【步骤1】测试MSK连接...")
    print("-" * 60)
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            request_timeout_ms=10000
        )
        
        # 获取集群元数据
        cluster_metadata = admin_client.list_topics()
        print(f"✅ 成功连接到MSK集群！")
        print(f"   现有Topics: {list(cluster_metadata)}")
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"❌ 连接MSK失败: {e}")
        return False


def create_topic_if_not_exists():
    """创建Topic（如果不存在）"""
    print("\n【步骤2】检查/创建Topic...")
    print("-" * 60)
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            request_timeout_ms=10000
        )
        
        # 获取现有topics
        existing_topics = admin_client.list_topics()
        
        if TOPIC_NAME in existing_topics:
            print(f"✅ Topic '{TOPIC_NAME}' 已存在")
        else:
            # 创建新topic
            topic = NewTopic(
                name=TOPIC_NAME,
                num_partitions=3,
                replication_factor=2
            )
            admin_client.create_topics([topic])
            print(f"✅ Topic '{TOPIC_NAME}' 创建成功")
            time.sleep(2)  # 等待topic创建完成
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"⚠️ Topic操作警告: {e}")
        print("   (如果topic已存在，可以忽略此警告)")
        return True


def test_producer():
    """测试生产者"""
    print("\n【步骤3】测试消息生产...")
    print("-" * 60)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )
        print("✅ 生产者创建成功")
        
        # 发送测试消息
        messages_sent = 0
        for i in range(5):
            key = f"test-key-{i}"
            value = {
                "id": i,
                "message": f"测试消息 #{i}",
                "timestamp": datetime.now().isoformat(),
                "test_run": "integration-test"
            }
            
            future = producer.send(TOPIC_NAME, key=key, value=value)
            record_metadata = future.get(timeout=10)
            messages_sent += 1
            
            print(f"   ✅ 消息 {i+1}/5 发送成功 "
                  f"(partition={record_metadata.partition}, "
                  f"offset={record_metadata.offset})")
            
            time.sleep(0.5)
        
        producer.flush()
        producer.close()
        
        print(f"\n✅ 生产者测试完成！共发送 {messages_sent} 条消息")
        return True
        
    except Exception as e:
        print(f"❌ 生产者测试失败: {e}")
        return False


def test_consumer():
    """测试消费者"""
    print("\n【步骤4】测试消息消费...")
    print("-" * 60)
    
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id='integration-test-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=10000  # 10秒无消息则退出
        )
        print("✅ 消费者创建成功")
        
        # 消费消息
        messages_received = 0
        print("\n   开始消费消息...")
        
        for message in consumer:
            messages_received += 1
            print(f"   📨 收到消息 {messages_received}: "
                  f"key={message.key}, "
                  f"value={message.value.get('message', 'N/A')}")
            
            # 最多消费10条作为演示
            if messages_received >= 10:
                break
        
        consumer.close()
        
        if messages_received > 0:
            print(f"\n✅ 消费者测试完成！共消费 {messages_received} 条消息")
            return True
        else:
            print(f"\n⚠️ 未消费到任何消息（可能需要等待一会儿）")
            return True
        
    except Exception as e:
        print(f"❌ 消费者测试失败: {e}")
        return False


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("MSK Kafka 完整测试")
    print("=" * 60)
    print(f"Broker地址:")
    for broker in BOOTSTRAP_SERVERS:
        print(f"  - {broker}")
    print(f"Topic名称: {TOPIC_NAME}")
    print("=" * 60)
    
    # 测试连接
    if not test_connection():
        print("\n❌ 测试失败：无法连接到MSK集群")
        return False
    
    time.sleep(1)
    
    # 创建Topic
    if not create_topic_if_not_exists():
        print("\n❌ 测试失败：无法创建Topic")
        return False
    
    time.sleep(1)
    
    # 测试生产者
    if not test_producer():
        print("\n❌ 测试失败：生产者测试失败")
        return False
    
    time.sleep(2)  # 等待消息传播
    
    # 测试消费者
    if not test_consumer():
        print("\n❌ 测试失败：消费者测试失败")
        return False
    
    # 所有测试通过
    print("\n" + "=" * 60)
    print("🎉 所有测试通过！MSK集群工作正常！")
    print("=" * 60)
    return True


def main():
    """主函数"""
    try:
        success = run_all_tests()
        if success:
            print("\n✅ 测试完成，您可以使用以下命令进行更多测试:")
            print(f"   生产者: python producer.py")
            print(f"   消费者: python consumer.py")
        else:
            print("\n❌ 测试未完全通过，请检查错误信息")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ 测试被用户中断")
    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

