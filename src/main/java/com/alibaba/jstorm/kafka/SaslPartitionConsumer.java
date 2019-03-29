/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;

/**
 * 
 * @author liangyi
 *
 */
public class SaslPartitionConsumer implements IPartitionConsumer{
	private static Logger LOG = LoggerFactory.getLogger(SaslPartitionConsumer.class);

	private int partition;
	private KafkaConsumer<String, ByteBuffer> consumer;

	private PartitionCoordinator coordinator;

	private KafkaSpoutConfig config;
	private LinkedList<ByteBufferAndOffset> emittingMessages = new LinkedList<ByteBufferAndOffset>();
	private SortedSet<Long> pendingOffsets = new TreeSet<Long>();
	private SortedSet<Long> failedOffsets = new TreeSet<Long>();
	private long emittingOffset;
	private long lastCommittedOffset;
	private ZkState zkState;
	private Map stormConf;

	public SaslPartitionConsumer(Map conf, KafkaSpoutConfig config, int partition, ZkState offsetState) {
		this.stormConf = conf;
		this.config = config;
		this.partition = partition;
		
		Properties p=new Properties();
		p.put("bootstrap.servers", config.brokerHosts);
		
		p.put("group.id", config.clientId);//
		p.put("client.id", config.clientId);
		p.put("enable.auto.commit", "False");
		p.put("auto.commit.interval.ms", "1000");
		p.put("session.timeout.ms", config.socketTimeoutMs);
		
		if (config.fromBeginning) {
			p.put("auto.offset.reset", "earliest");//earliest,latest
		}
		
		p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteBufferDeserializer");
		
		p.put("security.protocol", config.securityProtocol);
		p.put("sasl.mechanism", config.saslMechanism);
		p.put("sasl.jaas.config",config.saslJaasConfig);
		KafkaConsumer<String, ByteBuffer> consumer=new KafkaConsumer<String, ByteBuffer>(p);

		this.consumer = consumer;
		this.zkState = offsetState;
		
		Long jsonOffset = null;
		try {
			Map<Object, Object> json = offsetState.readJSON(zkPath());
			if (json != null) {
				// jsonTopologyId = (String)((Map<Object,Object>)json.get("topology"));
				jsonOffset = (Long) json.get("offset");
			}
		} catch (Throwable e) {
			LOG.warn("Error reading and/or parsing at ZkNode: " + zkPath(), e);
		}

		try {
			if (config.fromBeginning) {
				emittingOffset = getBeginOffset(config.topic, partition);
			} else {
				if (jsonOffset == null) {
					lastCommittedOffset = getLatestOffset(config.topic, partition);
				} else {
					lastCommittedOffset = jsonOffset;
				}
				emittingOffset = lastCommittedOffset;
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	private long getBeginOffset(String topic,int partition) {
		TopicPartition topicAndPartition = new TopicPartition(topic,partition);
		Collection<TopicPartition> collection = new ArrayList<TopicPartition>(Arrays.asList(topicAndPartition));
		Map<TopicPartition, Long> beginningOffsets =consumer.beginningOffsets(collection);
		long offset = beginningOffsets.get(topicAndPartition);
		return offset;
	}
	private long getLatestOffset(String topic,int partition) {
		TopicPartition topicAndPartition = new TopicPartition(topic,partition);
		Collection<TopicPartition> collection = new ArrayList<TopicPartition>(Arrays.asList(topicAndPartition));
		Map<TopicPartition, Long> lastOffsets = consumer.endOffsets(collection);
		long offset = lastOffsets.get(topicAndPartition);
		return offset;
	}

	public EmitState emit(SpoutOutputCollector collector) {
		if (emittingMessages.isEmpty()) {
			fillMessages();
		}

		int count = 0;
		while (true) {
			ByteBufferAndOffset toEmitMsg = emittingMessages.pollFirst();
			if (toEmitMsg == null) {
				return EmitState.EMIT_END;
			}
			count++;
			Iterable<List<Object>> tups = generateTuples(toEmitMsg.byteBuffer());

			if (tups != null) {
				for (List<Object> tuple : tups) {
					// LOG.debug("emit message {}", new
					// String(Utils.toByteArray(toEmitMsg.message().payload())));
					collector.emit(tuple, new KafkaMessageId(partition, toEmitMsg.offset()));
				}
				if (count >= config.batchSendCount) {
					break;
				}
			} else {
				ack(toEmitMsg.offset());
			}
		}

		if (emittingMessages.isEmpty()) {
			return EmitState.EMIT_END;
		} else {
			return EmitState.EMIT_MORE;
		}
	}
	
	private ConsumerRecords<String, ByteBuffer> poll(long timeout,long offset){
		TopicPartition t=new TopicPartition(config.topic, partition);
		consumer.assign(Arrays.asList(t));
		consumer.seek(t, offset);
		ConsumerRecords<String, ByteBuffer> msgs=consumer.poll(timeout);
		return msgs;
	}
	
	private void fillMessages() {

		ConsumerRecords<String, ByteBuffer> msgs=null;
		try {
			msgs = poll(0, emittingOffset + 1);
			
			if (msgs == null||msgs.isEmpty()) {
				if (LOG.isWarnEnabled()) {
					LOG.warn("fetch null message from offset {}", emittingOffset);
				}

				if (emittingOffset==0) {
					//等于0说明partition里没有数据，不处理了
					return;
				}
				// 如果取到空，则改为从最后的Offset取数 
				long latestOffset = getLatestOffset(config.topic, partition);
				if (latestOffset==this.emittingOffset) {
					return;
				}
				this.emittingOffset = latestOffset;
				msgs = poll(100, emittingOffset + 1);
				if (msgs == null) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("also fetch null message from offset {}", emittingOffset);
					}
					return;
				}

			}

			for (ConsumerRecord<String, ByteBuffer> msg : msgs) {
				ByteBufferAndOffset byteBufferAndOffset=new ByteBufferAndOffset(msg.value(), msg.offset());
				emittingMessages.add(byteBufferAndOffset);
				emittingOffset = msg.offset();
				pendingOffsets.add(emittingOffset);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage(), e);
		}
	}

	public void commitState() {
		try {
			long lastOffset = 0;
			if (pendingOffsets.isEmpty() || pendingOffsets.size() <= 0) {
				lastOffset = emittingOffset;
			} else {
				lastOffset = pendingOffsets.first();
			}
			if (lastOffset != lastCommittedOffset) {
				Map<Object, Object> data = new HashMap<Object, Object>();
				data.put("topology", stormConf.get(Config.TOPOLOGY_NAME));
				data.put("offset", lastOffset);
				data.put("partition", partition);
				data.put("broker", config.brokerHosts);
				data.put("topic", config.topic);
				zkState.writeJSON(zkPath(), data);
				lastCommittedOffset = lastOffset;
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}

	}

	public void ack(long offset) {
		try {
			pendingOffsets.remove(offset);
		} catch (Exception e) {
			LOG.error("offset ack error " + offset);
		}
	}

	public void fail(long offset) {
		failedOffsets.remove(offset);
	}

	public void close() {
		coordinator.removeConsumer(partition);
		consumer.close();
	}

	@SuppressWarnings("unchecked")
	public Iterable<List<Object>> generateTuples(ByteBuffer payload) {
		Iterable<List<Object>> tups = null;
		if (payload == null) {
			return null;
		}
		tups = Arrays.asList(Utils.tuple(Utils.toByteArray(payload)));
		return tups;
	}

	private String zkPath=null;
	private String zkPath() {
		if (zkPath==null) {
			zkPath = config.zkRoot + "/kafka/offset/topic/" + config.topic + "/" + config.clientId + "/" + partition;
		}
		return zkPath;
	}

	public PartitionCoordinator getCoordinator() {
		return coordinator;
	}

	public void setCoordinator(PartitionCoordinator coordinator) {
		this.coordinator = coordinator;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

}
