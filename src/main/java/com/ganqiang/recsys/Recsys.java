package com.ganqiang.recsys;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.ganqiang.recsys.bolt.HBaseStoreBolt;
import com.ganqiang.recsys.spout.MessageScheme;
import com.ganqiang.recsys.util.Constants;
import com.ganqiang.recsys.util.Initializer;

public class Recsys {

	private static final Logger logger = Logger.getLogger(Recsys.class);
	private static final boolean islocal = true;

	static {
		Initializer.setup();
	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		logger.info("begin to running recsys.");
		BrokerHosts brokerHosts = new ZkHosts(Constants.kafka_zk_address);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, Constants.kafka_topic, 	Constants.kafka_zk_root, Constants.kafka_id);

		Config conf = new Config();
		Map<String, String> map = new HashMap<String, String>();
		map.put("metadata.broker.list", Constants.kakfa_broker_list);
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", map);
//		conf.put("topic", "topic2");

		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig));
		builder.setBolt("bolt", new HBaseStoreBolt()).shuffleGrouping("spout");
//		builder.setBolt("kafkabolt", new KafkaBolt<String, Integer>()).shuffleGrouping("bolt");

		if (!islocal) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(Constants.storm_topology_name, conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(Constants.storm_topology_name, conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology(Constants.storm_topology_name);
			cluster.shutdown();
		}
		logger.info("run recsys finish.");
	}

	public static void clientproduce(){
		Properties props = new Properties();
		props.put("zk.connect", "localhost:2183");
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		String msg = Constants.test_log;
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		KeyedMessage<String, String> data = new KeyedMessage<String, String>("topic1",  msg);
		producer.send(data);
		producer.close();
	}

}
