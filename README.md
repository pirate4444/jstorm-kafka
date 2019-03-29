# Kafka增加SASL认证，jstorm的KafkaSpout怎么写

Kafka增加了SASL认证，消费端也要做相应的调整。阿里官方jstorm中的jstorm-kafka，使用SimpleConsumer去消费数据，我研究了好久，不知道怎么增加认证。
于是只好新写了个SaslPartionConsumer，封装了org.apache.kafka.clients.consumer.KafkaConsumer去消费带有SASL认证的Kafka。

## 使用方式

参考代码中的KafkaSpoutTest.java类

大家一起交流、学习。