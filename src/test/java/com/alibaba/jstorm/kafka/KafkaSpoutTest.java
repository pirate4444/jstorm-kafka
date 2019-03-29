package com.alibaba.jstorm.kafka;

import java.util.Properties;

import org.junit.Test;

import backtype.storm.topology.TopologyBuilder;

public class KafkaSpoutTest {

	@Test
	public void testOriginKafkaSpout() {
		//不带sasl认证（官方版本）的KafkaSpout
		Properties properties=new Properties();
		properties.put("kafka.zookeeper.hosts", "127.0.0.1:2181");//zookeeper地址
		properties.put("kafka.broker.hosts", "127.0.0.1:9092");//kafka地址
		properties.put("kafka.broker.partitions", "10");//这个topic有多少个partition
		properties.put("kafka.topic", "TEST_TOPIC");
		properties.put("kafka.client.id", "YAYI");
		
        KafkaSpoutConfig kafkaSpoutConfig = new KafkaSpoutConfig(properties);
        
        //创建一个KafkaSpout，从kafka中读取数据，开始数据流转
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",kafkaSpout,1);
	}
	
	@Test
	public void testSaslKafkaSpout() {
		//sasl认证的KafkaSpout
		Properties properties=new Properties();
		properties.put("kafka.zookeeper.hosts", "127.0.0.1:2181");//zookeeper地址
		properties.put("kafka.broker.hosts", "127.0.0.1:9092");//kafka地址
		properties.put("kafka.broker.partitions", "10");//这个topic有多少个partition
		properties.put("kafka.topic", "TEST_TOPIC");
		properties.put("kafka.client.id", "YAYI");
		
		//与原版不同的是，这里加了sasl认证。
		//下面的各个参数，包括username和password，填实际值
		properties.put("security.protocol", "SASL_PLAINTEXT");
		properties.put("sasl.mechanism", "PLAIN");
		properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"username\" password=\"password\";");
		
		//主要的不同是这个类com.alibaba.jstorm.kafka.PartitionCoordinator.createPartitionConsumers(Map, TopologyContext)
		//根据是否配置上面三个参数，创建不同的 IPartitionConsumer 接口。
        KafkaSpoutConfig kafkaSpoutConfig = new KafkaSpoutConfig(properties);
        
        //创建一个KafkaSpout，从kafka中读取数据，开始数据流转
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",kafkaSpout,1);
	}
	
}
