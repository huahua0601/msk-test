# MSK Kafka 测试工具

用于测试Amazon MSK（Managed Streaming for Kafka）的Python工具集。

## 环境配置

### MSK集群信息

- **Broker地址:**
  - boot-y8y.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092
  - boot-dw1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092
  - boot-nm1.democluster.zc4yi8.c5.kafka.us-east-1.amazonaws.com:9092

- **默认Topic:** test-topic

## 快速开始

### 1. 创建Conda环境

```bash
# 使用environment.yml创建环境
conda env create -f environment.yml

# 激活环境
conda activate msk-test
```

### 2. 手动安装依赖（可选）

如果不使用environment.yml，可以手动创建环境：

```bash
# 创建新环境
conda create -n msk-test python=3.9 -y

# 激活环境
conda activate msk-test

# 安装依赖
pip install kafka-python==2.0.2
```

### 3. 运行测试

#### 方式1：完整测试（推荐）

运行完整的集成测试，自动测试连接、生产和消费：

```bash
python test_msk.py
```

这个脚本会：
- ✅ 测试MSK集群连接
- ✅ 创建Topic（如果不存在）
- ✅ 发送测试消息
- ✅ 消费测试消息

#### 方式2：单独测试生产者

```bash
python producer.py
```

功能：
- 发送单条测试消息
- 批量发送10条消息
- 显示详细的发送结果

#### 方式3：单独测试消费者

```bash
python consumer.py
```

功能：
- 持续监听并消费消息
- 显示详细的消息内容
- 按Ctrl+C退出

## 文件说明

- **environment.yml** - Conda环境配置文件
- **test_msk.py** - 完整测试脚本（推荐先运行这个）
- **producer.py** - 独立的生产者脚本
- **consumer.py** - 独立的消费者脚本
- **README.md** - 本文档

## 使用场景

### 场景1：验证MSK集群是否正常工作

```bash
python test_msk.py
```

### 场景2：持续发送测试数据

```bash
python producer.py
```

### 场景3：监控消息消费

在一个终端运行生产者：
```bash
python producer.py
```

在另一个终端运行消费者：
```bash
python consumer.py
```

## 配置修改

如果需要修改配置，请编辑对应的Python文件：

- **修改Broker地址:** 编辑 `BOOTSTRAP_SERVERS` 变量
- **修改Topic名称:** 编辑 `TOPIC_NAME` 变量
- **修改Consumer Group:** 编辑 `GROUP_ID` 变量（仅消费者）

## 故障排查

### 问题1：无法连接到MSK

```
❌ 连接MSK失败: ...
```

**解决方案:**
1. 检查网络连接
2. 确认安全组允许访问端口9092
3. 确认MSK集群处于活动状态
4. 检查broker地址是否正确

### 问题2：Topic创建失败

```
❌ Topic操作警告: ...
```

**解决方案:**
- 如果topic已存在，可以忽略此警告
- 确认有创建topic的权限
- 可以手动创建topic后再运行测试

### 问题3：消费不到消息

```
⚠️ 未消费到任何消息
```

**解决方案:**
1. 确认生产者已成功发送消息
2. 检查消费者的offset设置（auto_offset_reset）
3. 确认topic名称一致
4. 等待几秒后重试

## 依赖包说明

- **kafka-python (2.0.2)** - Python的Kafka客户端库
  - 支持生产者和消费者
  - 支持Kafka Admin操作
  - 兼容Kafka 0.8+

## 安全注意事项

当前配置使用PLAINTEXT连接。如果MSK集群启用了安全认证（如TLS或SASL），需要添加相应的安全配置。

### 示例：添加TLS支持

```python
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SSL',
    ssl_cafile='/path/to/ca-cert',
    # ... 其他配置
)
```

## 性能调优建议

### 生产者优化

- `batch_size`: 控制批量发送的大小
- `linger_ms`: 消息发送前的等待时间
- `compression_type`: 启用压缩（gzip, snappy, lz4）

### 消费者优化

- `fetch_min_bytes`: 最小抓取字节数
- `fetch_max_wait_ms`: 最大等待时间
- `max_partition_fetch_bytes`: 单次抓取最大字节数

## 许可证

MIT License

## 联系方式

如有问题或建议，请提交Issue。

